"""Unified event matching: one scorer for save-time matching, postponement routing,
cancellation and the leak monitor. Design: https://claude.ai/artifact/Gev7r6tWdojSbV2fSzYDj2

Currently runs in shadow mode only: decisions are logged to match_log next to what the
legacy save_to_database path did, and nothing here writes to events.
"""
import json
import re
from datetime import datetime, timedelta

from rapidfuzz import fuzz, utils

UPDATE_AT = 75
REVIEW_AT = 55
SIBLING_CAP = 74
TIE_MARGIN = 10
POSTPONE_TITLE_SIM = 85
POSTPONE_LOOKBACK_DAYS = 7

LINK_FIELDS = ('ticket_url', 'event_url')
MATCHING_LEVELS = ('key', 'evidence')
PLACEHOLDER = re.compile(r'^(tba|tbd|to be announced|special event|private event|event)$', re.I)


# ---------- venue identity profile ----------

def venue_profile(venue_config):
    """Per-venue identity rules, defined once. link_trust maps each link field to
    key | evidence | ignore | drop. Falls back to the legacy flags for venues without one."""
    cfg = venue_config or {}
    trust = {f: 'evidence' for f in LINK_FIELDS}
    unreliable = cfg.get('unreliable_urls')
    if unreliable is True:
        trust = {f: 'ignore' for f in LINK_FIELDS}
    elif unreliable in LINK_FIELDS:
        trust[unreliable] = 'ignore'
    trust.update(cfg.get('link_trust', {}))
    return {
        'link_trust': trust,
        'ignore_patterns': [re.compile(p) for p in cfg.get('link_ignore_patterns', [])],
        'multi_show': bool(cfg.get('dupe_check_start_time')),
        'auto_approve': bool(cfg.get('auto_approve')),
    }


def usable_links(record, profile, levels=MATCHING_LEVELS, batch_ignored=()):
    """Link values from this record that matching may use, at the given trust levels."""
    out = set()
    for field in LINK_FIELDS:
        value = record.get(field)
        if not value or profile['link_trust'].get(field) not in levels:
            continue
        if value in batch_ignored or any(p.search(value) for p in profile['ignore_patterns']):
            continue
        out.add(value.rstrip('/'))
    return out


# ---------- normalization helpers ----------

def _strip_openers(name):
    return re.sub(r'\s+(with|featuring|feat\.?|presents)\s+.*$', '', name or '', flags=re.I).strip()


def title_similarity(a, b):
    """0-100. Min of full-title and opener-stripped similarity, so a shared opener phrase
    ('with special guest') can't make two different headliners look alike."""
    if not a or not b:
        return None
    full = fuzz.token_set_ratio(a, b, processor=utils.default_process)
    stripped = fuzz.token_set_ratio(_strip_openers(a), _strip_openers(b), processor=utils.default_process)
    return min(full, stripped)


def _is_placeholder(name):
    return not name or bool(PLACEHOLDER.match(utils.default_process(name)))


def _minutes(t):
    if not t:
        return None
    try:
        h, m = str(t)[:5].split(':')
        return int(h) * 60 + int(m)
    except ValueError:
        return None


def _show_minutes(record):
    m = _minutes(record.get('start_time'))
    return m if m is not None else _minutes(record.get('doors_time'))


def _date(d):
    try:
        return datetime.strptime(str(d)[:10], '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return None


def _after_midnight(minutes):
    # Only a 12am-3am start has an ambiguous date; an 11pm show's date is unambiguous.
    return minutes is not None and minutes <= 3 * 60


# ---------- features ----------

def venue_score(listing, cand, ctx):
    return 30  # blocking only admits same-venue candidates


def date_score(listing, cand, ctx):
    a, b = _date(listing.get('start_date')), _date(cand.get('start_date'))
    if a is None or b is None:
        return 0
    return 25 if a == b else 5


def time_score(listing, cand, ctx):
    a, b = _show_minutes(listing), _show_minutes(cand)
    if a is None or b is None:
        return 0
    # A day-apart candidate is only admitted near midnight, where the two dates are usually
    # the same show under different date conventions, so compare clock times.
    diff = min(abs(a - b), 1440 - abs(a - b))
    if diff <= 15:
        return 25
    if diff <= 75:
        return 10
    return -40 if ctx['profile']['multi_show'] else 0


def sole_event_score(listing, cand, ctx):
    if _show_minutes(listing) is not None and _show_minutes(cand) is not None:
        return 0
    same_day = [c for c in ctx['pool'] if c.get('start_date') == cand.get('start_date')]
    return 10 if len(same_day) == 1 else 0


def url_score(listing, cand, ctx):
    mine = usable_links(listing, ctx['profile'], batch_ignored=ctx['batch_ignored'])
    theirs = usable_links(cand, ctx['profile'])
    return 20 if mine & theirs else 0


def title_score(listing, cand, ctx):
    if _is_placeholder(listing.get('name')) or _is_placeholder(cand.get('name')):
        return 0
    if utils.default_process(listing['name']) == utils.default_process(cand['name']):
        return 20
    sim = title_similarity(listing['name'], cand['name'])
    if sim >= 85:
        return 15
    if sim >= 60:
        return 5
    return -15


def room_score(listing, cand, ctx):
    a, b = listing.get('location'), cand.get('location')
    if not a or not b:
        return 0
    return 10 if utils.default_process(a) == utils.default_process(b) else -30


FEATURES = [venue_score, date_score, time_score, sole_event_score, url_score, title_score, room_score]


def sibling_cap_applies(listing, cand, ctx):
    if _show_minutes(listing) is not None and _show_minutes(cand) is not None:
        return False
    similar = [c for c in ctx['pool']
               if c.get('start_date') == cand.get('start_date')
               and (title_similarity(listing.get('name'), c.get('name')) or 0) >= 60]
    return len(similar) >= 2


def score(listing, cand, ctx):
    parts = {f.__name__.replace('_score', ''): f(listing, cand, ctx) for f in FEATURES}
    total = sum(parts.values())
    if sibling_cap_applies(listing, cand, ctx):
        total = min(total, SIBLING_CAP)
        parts['sibling_cap'] = SIBLING_CAP
    return total, parts


# ---------- blocking ----------

def in_block(listing, cand):
    a, b = _date(listing.get('start_date')), _date(cand.get('start_date'))
    if a is None or b is None or cand.get('review_status') == 'rejected':
        return False
    gap = abs((a - b).days)
    if gap == 0:
        return True
    if gap > 1 or cand.get('recurring') in ('yes', 'possible'):
        return False
    la, lb = _show_minutes(listing), _show_minutes(cand)
    return la is None or lb is None or _after_midnight(la) or _after_midnight(lb)


# ---------- batch decisions ----------

def decide_batch(events, rows_by_venue, venue_configs, today=None):
    """Shadow decisions for one save batch. rows_by_venue: {venue name: [row dicts]} as the
    DB stood before the batch. Returns one decision dict per event, in order."""
    today = today or datetime.now().date()
    decisions = []
    claimed = {}           # row id -> index of the listing that claimed it
    best_against = {}      # row id -> best score any listing reached against it
    virtual = {}           # venue -> virtual rows for listings this batch would insert

    for idx, ev in enumerate(events):
        venue = ev.get('venue')
        profile = venue_profile(venue_configs.get(ev.get('venue_key')))
        rows = rows_by_venue.get(venue, []) + virtual.get(venue, [])
        ctx = {'profile': profile, 'batch_ignored': set(ev.get('batch_ignored_urls', [])), 'pool': []}
        d = {'index': idx, 'decision': None, 'target_id': None, 'score': None, 'parts': None,
             'runner_up_id': None, 'runner_up_score': None, 'reason': None}

        # Source-key shortcut
        keys = usable_links(ev, profile, levels=('key',), batch_ignored=ctx['batch_ignored'])
        key_hit = next((r for r in rows if r.get('review_status') != 'rejected'
                        and keys & usable_links(r, profile, levels=('key',))), None) if keys else None
        if key_hit:
            same_date = key_hit.get('start_date') == ev.get('start_date')
            d.update(decision='update' if same_date else 'postpone_hold', target_id=key_hit['id'],
                     reason='source_key' if same_date else 'source_key_date_change')
            if same_date:
                claimed.setdefault(key_hit['id'], idx)
            decisions.append(d)
            continue

        block = [r for r in rows if in_block(ev, r)]
        ctx['pool'] = block
        scored = sorted(((score(ev, r, ctx), r) for r in block), key=lambda x: -x[0][0])
        for (total, _), r in scored:
            best_against[r['id']] = max(best_against.get(r['id'], -999), total)

        # One-to-one: a row already claimed by an earlier listing is unavailable, unless this
        # listing is a second copy of that earlier listing (an intra-batch duplicate).
        available = []
        for (total, parts), r in scored:
            if r['id'] in claimed and total >= UPDATE_AT:
                first = events[claimed[r['id']]]
                if (title_similarity(first.get('name'), ev.get('name')) or 0) >= 85 and \
                        first.get('start_date') == ev.get('start_date') and \
                        first.get('start_time') == ev.get('start_time'):
                    d.update(decision='batch_duplicate', target_id=r['id'], score=total, parts=parts)
                    break
                continue
            available.append(((total, parts), r))
        if d['decision']:
            decisions.append(d)
            continue

        if available:
            (top, parts), best = available[0]
            d.update(score=top, parts=parts, target_id=best['id'])
            if len(available) > 1:
                (second, _), runner = available[1]
                d.update(runner_up_id=runner['id'], runner_up_score=second)
            if top >= REVIEW_AT and len(available) > 1 and top - available[1][0][0] < TIE_MARGIN \
                    and available[1][0][0] >= REVIEW_AT:
                d.update(decision='review', reason='tie')
            elif top >= UPDATE_AT:
                d['decision'] = 'update'
            elif top >= REVIEW_AT:
                d['decision'] = 'review'
        if d['decision'] == 'update' and isinstance(d['target_id'], str):
            d['decision'] = 'batch_duplicate'  # matched an earlier listing in this same batch
        if d['decision'] == 'update':
            claimed.setdefault(d['target_id'], idx)
        if d['decision'] is None:
            d.update(decision='postpone_hold', target_id=None, reason='under_review_floor')
        if d['decision'] in ('review', 'postpone_hold') or d['decision'] is None:
            v = dict(ev, id=f'new:{idx}', review_status='pending')
            virtual.setdefault(venue, []).append(v)
            if d['decision'] == 'review':
                claimed.setdefault(v['id'], idx)
        decisions.append(d)

    # Postponement routing runs once the whole batch is matched.
    for d in decisions:
        if d['decision'] != 'postpone_hold':
            continue
        ev = events[d['index']]
        if d['reason'] == 'source_key_date_change':
            d.update(decision='reschedule')
            continue
        venue = ev.get('venue')
        lookback = today - timedelta(days=POSTPONE_LOOKBACK_DAYS)
        evd = _date(ev.get('start_date'))
        candidates = []
        for r in rows_by_venue.get(venue, []):
            rd = _date(r.get('start_date'))
            if rd is None or evd is None or abs((rd - evd).days) <= 1 or rd < lookback:
                continue
            if r.get('review_status') == 'rejected' or r.get('event_status') == 'canceled':
                continue
            if r.get('recurring') in ('yes', 'possible'):
                continue
            sim = title_similarity(ev.get('name'), r.get('name')) or 0
            if sim >= POSTPONE_TITLE_SIM:
                candidates.append((sim, r))
        if not candidates:
            d.update(decision='insert', target_id=None, reason='no_match')
            continue
        sim, old = max(candidates, key=lambda x: x[0])
        old_missing = old['id'] not in claimed and best_against.get(old['id'], -999) < REVIEW_AT
        if old_missing:
            d.update(decision='review', target_id=old['id'], reason='possible_postponement')
        else:
            d.update(decision='insert', target_id=None, reason='second_booking')

    return decisions, claimed, best_against


def missing_rows(active_rows, claimed, best_against, postponement_targets):
    """Shadow cancellation: active future rows no batch event matched or scored 55+ against."""
    return [r['id'] for r in active_rows
            if r['id'] not in claimed
            and best_against.get(r['id'], -999) < REVIEW_AT
            and r['id'] not in postponement_targets]


# ---------- legacy comparison ----------

def agrees(shadow, legacy):
    """legacy: {'outcome': ..., 'target_id': ..., 'new_id': ...} from save_to_database."""
    lo, lt = legacy.get('outcome'), legacy.get('target_id')
    sd, st = shadow['decision'], shadow['target_id']
    if sd == 'update':
        return lo in ('exact_skip', 'url_update', 'near_update') and lt == st
    if sd == 'reschedule':
        return lo == 'url_update' and lt == st
    if sd == 'review':
        return lo == 'possible_duplicate' and (lt == st or isinstance(st, str))
    if sd == 'insert':
        return lo == 'insert'
    if sd == 'batch_duplicate':
        return lo in ('exact_skip', 'url_update', 'near_update', 'possible_duplicate')
    return False
