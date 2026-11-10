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
# Days of past rows loaded as candidates (±1-day matches, recurrence history).
HISTORY_DAYS = 7

LINK_FIELDS = ('ticket_url', 'event_url')
MATCHING_LEVELS = ('key', 'evidence')
PLACEHOLDER = re.compile(r'^(tba|tbd|to be announced|special event|private event|event)$', re.I)


# ---------- venue identity profile ----------

def is_multi_venue_feed(venue_config):
    """True for city-calendar style ICS feeds whose events belong to many venues."""
    cfg = venue_config or {}
    return bool(cfg.get('ical_url')) and not cfg.get('ics_single_venue')


def venue_profile(venue_config):
    """Per-venue identity rules, read only from the unified-matching settings:
    link_trust (key | evidence | ignore | drop per link field, default evidence),
    multi_show and link_ignore_patterns. Links to the venue's own pages never count."""
    cfg = venue_config or {}
    trust = {f: 'evidence' for f in LINK_FIELDS}
    trust.update(cfg.get('link_trust', {}))
    site = re.match(r'(https?://[^/]+)', cfg.get('url') or '')
    generic = {u.rstrip('/') for u in (cfg.get('url'), cfg.get('venue_url'), site.group(1) if site else None) if u}
    return {
        'link_trust': trust,
        'ignore_patterns': [re.compile(p) for p in cfg.get('link_ignore_patterns', [])],
        'generic_links': generic,
        'multi_show': bool(cfg.get('multi_show')),
        'excluded': is_multi_venue_feed(cfg),
    }


def _link(record, field):
    # Scraped events carry the links as extracted, before any legacy nulling.
    extracted = record.get('extracted_links')
    return extracted.get(field) if extracted is not None else record.get(field)


def usable_links(record, profile, levels=MATCHING_LEVELS, batch_ignored=()):
    """Link values from this record that matching may use, at the given trust levels."""
    out = set()
    for field in LINK_FIELDS:
        value = _link(record, field)
        if not value or profile['link_trust'].get(field) not in levels:
            continue
        value = value.rstrip('/')
        if value in batch_ignored or value in profile['generic_links'] \
                or any(p.search(value) for p in profile['ignore_patterns']):
            continue
        out.add(value)
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
    if mine & theirs:
        return 20
    # Both linked but no link in common: a tie-breaker that tips an exact 75 into review
    # (a different show in the same slot and room). Approved 2026-11-09.
    return -1 if mine and theirs else 0


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
    if a is None or b is None:
        return False
    gap = abs((a - b).days)
    if gap == 0:
        return True
    if gap > 1 or cand.get('recurring') in ('yes', 'possible'):
        return False
    la, lb = _show_minutes(listing), _show_minutes(cand)
    return la is None or lb is None or _after_midnight(la) or _after_midnight(lb)


# ---------- batch decisions ----------

def looks_recurring(dates):
    """Same weekday 3+ times, or a constant gap of 2+ days (mirrors _looks_recurring).
    Back-to-back days are a multi-night run, never recurrence (2026-11-10)."""
    d = sorted({_date(x) for x in dates if _date(x)})
    if len(d) < 2:
        return False
    gaps = {(d[i + 1] - d[i]).days for i in range(len(d) - 1)}
    if len(d) == 2:
        return gaps == {7}
    return (len(gaps) == 1 and min(gaps) > 1) or len({x.weekday() for x in d}) == 1


def _batch_ignored_links(events, profile):
    """Links shared by differently named events in one scrape identify a page, not an event."""
    names = {}
    for ev in events:
        for link in usable_links(ev, profile):
            names.setdefault(link, set()).add(utils.default_process(ev.get('name') or ''))
    return {link for link, n in names.items() if len(n) > 1}


def _anomalies(events, rows):
    """Logged only, never decisive: batch shapes that have signalled extraction failures."""
    found = []
    per_date = {}
    for ev in events:
        per_date[ev.get('start_date')] = per_date.get(ev.get('start_date'), 0) + 1
    if per_date:
        usual = sorted(per_date.values())[len(per_date) // 2]
        for d, n in per_date.items():
            if n >= 10 and n >= 3 * max(usual, 1):
                found.append(f'date_pileup:{d}:{n}')
    by_link = {}
    for ev in events:
        for f in LINK_FIELDS:
            if _link(ev, f):
                by_link.setdefault(_link(ev, f).rstrip('/'), set()).add(ev.get('start_date'))
    recurring_links = {(_link(r, f) or '').rstrip('/') for r in rows for f in LINK_FIELDS
                       if r.get('recurring') in ('yes', 'possible')}
    for link, dates in by_link.items():
        if len(dates) >= 3 and link not in recurring_links:
            found.append(f'link_many_dates:{len(dates)}')
    return found


def _rejected(row):
    return row.get('review_status') == 'rejected'


def _open_review_item(row):
    """A pending possible-duplicate row: a review item matching created and nobody resolved."""
    return row.get('review_status') == 'pending' and row.get('duplicate_of_id') is not None


def _decide_venue(idxs, events, rows, profile, today):
    """Decisions for one venue's slice of a batch. Rows are assigned to events by score,
    highest first, so page order never decides which event gets a row."""
    evs = {i: events[i] for i in idxs}
    batch_ignored = _batch_ignored_links(list(evs.values()), profile)
    out = {i: {'index': i, 'decision': None, 'target_id': None, 'score': None, 'parts': None,
               'runner_up_id': None, 'runner_up_score': None, 'reason': None, 'recurring': None}
           for i in idxs}
    claimed = {}        # row id -> event index
    best_against = {}   # row id -> best score any event reached against it

    # 1. Source key: a verified per-occurrence link is identity.
    for i, ev in evs.items():
        keys = usable_links(ev, profile, levels=('key',), batch_ignored=batch_ignored)
        if not keys:
            continue
        hits = [r for r in rows if keys & usable_links(r, profile, levels=('key',))]
        hit = next((r for r in hits if r.get('review_status') != 'rejected'), hits[0] if hits else None)
        if not hit:
            continue
        best_against[hit['id']] = 999
        if hit.get('review_status') == 'rejected':
            # Only a rejected copy exists: keep the rejection rather than resurfacing the show.
            out[i].update(decision='skip_rejected', target_id=hit['id'], reason='source_key')
        elif hit['id'] in claimed:
            out[i].update(decision='batch_duplicate', target_id=hit['id'], reason='source_key')
        elif hit.get('start_date') == ev.get('start_date'):
            claimed[hit['id']] = i
            out[i].update(decision='update', target_id=hit['id'], reason='source_key')
        else:
            claimed[hit['id']] = i
            out[i].update(decision='reschedule', target_id=hit['id'], reason='source_key_date_change')

    # Key-matched events are still scored against their block, decision unchanged, so other
    # stored copies of the same show don't look missing to the cancellation check.
    for i, ev in evs.items():
        if out[i]['reason'] != 'source_key':
            continue
        block = [r for r in rows if in_block(ev, r)]
        ctx = {'profile': profile, 'batch_ignored': batch_ignored, 'pool': [r for r in block if not _rejected(r)]}
        for r in block:
            best_against[r['id']] = max(best_against.get(r['id'], -999), score(ev, r, ctx)[0])

    # 2. Score every remaining event against its candidate rows. Rows already claimed by a
    #    source-key match stay candidates so a second copy of that listing is recognized.
    open_rows = rows
    pairs = []
    by_event = {}
    for i, ev in evs.items():
        if out[i]['decision']:
            continue
        block = [r for r in open_rows if in_block(ev, r)]
        # Rejected rows are candidates (so a rejection is remembered) but never count as
        # other events at the venue for the sole-event bonus or the sibling cap.
        ctx = {'profile': profile, 'batch_ignored': batch_ignored, 'pool': [r for r in block if not _rejected(r)]}
        scored = []
        for r in block:
            total, parts = score(ev, r, ctx)
            scored.append((total, parts, r))
            best_against[r['id']] = max(best_against.get(r['id'], -999), total)
            pairs.append((total, i, r['id'], _rejected(r)))
        by_event[i] = sorted(scored, key=lambda x: -x[0])

    # 3. Assign rows highest score first; each event and each row at most once. Live rows
    #    are assigned before rejected ones, so a live copy always wins when both exist.
    #    A listing whose best row (75+) was already taken by a higher-scoring listing is a
    #    second copy of it: skipped, never passed down to its next-best row.
    assigned, copies, seen = {}, {}, set()
    for total, i, rid, rej in sorted(pairs, key=lambda x: (x[3], -x[0], x[1])):
        if i in assigned or i in copies:
            continue
        best_pair = i not in seen
        seen.add(i)
        if rid in claimed:
            if best_pair and total >= UPDATE_AT:
                copies[i] = (rid, total)
            continue
        if total < REVIEW_AT:
            continue
        assigned[i] = rid
        claimed[rid] = i
    for i, (rid, total) in copies.items():
        parts = next(x[1] for x in by_event[i] if x[2]['id'] == rid)
        out[i].update(decision='batch_duplicate', target_id=rid, score=total, parts=parts, reason='copy')
    for i, rid in assigned.items():
        total, parts, target = next(x for x in by_event[i] if x[2]['id'] == rid)
        others = [x for x in by_event[i] if x[2]['id'] != rid and not _rejected(x[2])]
        d = out[i]
        d.update(score=total, parts=parts, target_id=rid)
        if others:
            d.update(runner_up_id=others[0][2]['id'], runner_up_score=others[0][0])
        tied = [x[2] for x in others if x[0] >= REVIEW_AT and abs(total - x[0]) < TIE_MARGIN]
        open_item = next((r for r in [target] + tied if _open_review_item(r)), None)
        if _rejected(target):
            if total >= UPDATE_AT:
                d.update(decision='skip_rejected')
            else:
                d.update(decision='review', reason='rejected_match')
        elif open_item is not None and (open_item is target or total < UPDATE_AT or tied):
            # An unresolved review row already covers this show: keep it current instead of
            # inserting another one each scrape.
            d.update(decision='update', target_id=open_item['id'], reason='open_review_item')
        elif tied:
            d.update(decision='review', reason='tie')
        else:
            d['decision'] = 'update' if total >= UPDATE_AT else 'review'

    # 4. An unassigned event that scores as an update against another event in this
    #    scrape is a second copy of it.
    primaries = [i for i in idxs if out[i]['decision'] is not None]
    for i in idxs:
        d = out[i]
        if d['decision'] is not None:
            continue
        ev = evs[i]
        best = None
        for j in primaries:
            other = dict(evs[j], id=f'new:{j}', review_status='pending')
            if not in_block(ev, other):
                continue
            total, parts = score(ev, other, {'profile': profile, 'batch_ignored': batch_ignored, 'pool': [other]})
            if best is None or total > best[0]:
                best = (total, parts, j)
        if best and best[0] >= UPDATE_AT:
            j = best[2]
            target = out[j]['target_id'] if out[j]['decision'] in ('update', 'review') and out[j]['target_id'] else f'new:{j}'
            d.update(decision='batch_duplicate', target_id=target, score=best[0], parts=best[1])
        else:
            if by_event.get(i):
                top = by_event[i][0]
                d.update(score=top[0], parts=top[1])
            primaries.append(i)

    # 5. Postponement routing, once the whole batch is matched.
    for i in idxs:
        d = out[i]
        if d['decision'] is not None:
            continue
        ev = evs[i]
        evd = _date(ev.get('start_date'))
        candidates = []
        for r in rows:
            rd = _date(r.get('start_date'))
            # Only a show still ahead (or today) can have been postponed; a past date dropping
            # off the page is expected (2026-11-09).
            if rd is None or evd is None or abs((rd - evd).days) <= 1 or rd < today:
                continue
            if r.get('review_status') == 'rejected' or r.get('event_status') == 'canceled':
                continue
            if r.get('recurring') in ('yes', 'possible'):
                continue
            sim = title_similarity(ev.get('name'), r.get('name')) or 0
            if sim >= POSTPONE_TITLE_SIM:
                candidates.append((sim, r))
        if not candidates:
            d.update(decision='insert', reason='no_match')
        else:
            old = max(candidates, key=lambda x: x[0])[1]
            if old['id'] not in claimed and best_against.get(old['id'], -999) < REVIEW_AT:
                d.update(decision='review', target_id=old['id'], reason='possible_postponement')
            else:
                d.update(decision='insert', reason='second_booking')

    # 6. Recurrence at insert: inherit a confirmed series, or detect a new pattern.
    for i in idxs:
        d = out[i]
        if d['decision'] != 'insert':
            continue
        ev = evs[i]
        links = usable_links(ev, profile, batch_ignored=batch_ignored)
        title = utils.default_process(ev.get('name') or '')
        series = [r for r in rows if r.get('review_status') != 'rejected'
                  and ((links and links & usable_links(r, profile))
                       or utils.default_process(r.get('name') or '') == title)]
        if any(r.get('recurring') == 'yes' for r in series):
            d['recurring'] = 'yes'
        elif series and looks_recurring([r['start_date'] for r in series] + [ev.get('start_date')]):
            d['recurring'] = 'possible'

    return out, claimed, best_against


def decide_batch(events, rows_by_venue, venue_configs, today=None):
    """Shadow decisions for one save batch against the DB as it stood before the batch.
    rows_by_venue: {venue name: [row dicts]}. Returns one decision per event, in order,
    plus the claimed rows, best score per row, and logged batch anomalies per venue."""
    today = today or datetime.now().date()
    groups = {}
    for i, ev in enumerate(events):
        groups.setdefault((ev.get('venue_key'), ev.get('venue')), []).append(i)
    decisions = [None] * len(events)
    claimed, best_against, anomalies = {}, {}, {}
    for (key, venue), idxs in groups.items():
        profile = venue_profile(venue_configs.get(key))
        if profile['excluded']:
            for i in idxs:
                decisions[i] = {'index': i, 'decision': 'excluded', 'target_id': None, 'score': None,
                                'parts': None, 'runner_up_id': None, 'runner_up_score': None,
                                'reason': 'city_calendar', 'recurring': None}
            continue
        rows = rows_by_venue.get(venue, [])
        out, c, b = _decide_venue(idxs, events, rows, profile, today)
        for i in idxs:
            decisions[i] = out[i]
        claimed.update(c)
        best_against.update(b)
        found = _anomalies([events[i] for i in idxs], rows)
        if found:
            anomalies[venue] = found
    return decisions, claimed, best_against, anomalies


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
    if sd == 'excluded':
        return None
    if sd == 'skip_rejected':
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
