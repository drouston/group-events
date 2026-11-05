"""Golden set for event_matching. Run on every weight or threshold change:
    python -m pytest tests/  (or: python tests/test_golden_matches.py)
Seeded with the proposal's worked examples; add resolved review cases one at a time."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import event_matching as em

SINGLE = {}                                   # default profile: one show a night
MULTI = {'multi_show': True}                 # venue with early/late shows
KEYED = {'link_trust': {'ticket_url': 'key'}}

V = 'Test Venue'


def row(id, name, date, time=None, **kw):
    return dict(id=id, name=name, start_date=date, start_time=time, venue=V,
                review_status=kw.pop('review_status', 'approved'), event_status='scheduled', **kw)


def listing(name, date, time=None, **kw):
    return dict(name=name, start_date=date, start_time=time, venue=V, venue_key='v', **kw)


def decide(ev, rows, cfg=SINGLE, today=None):
    from datetime import date
    decisions, _, _, _ = em.decide_batch([ev], {V: rows}, {'v': cfg}, today=today or date(2026, 11, 1))
    return decisions[0]


CASES = [
    # (label, listing, rows, venue config, expected decision, expected target id)
    ('URL randomized, nothing else changed',
     listing('Jane Doe', '2026-12-01', '20:00', ticket_url='https://t/new'),
     [row(1, 'Jane Doe', '2026-12-01', '20:00', ticket_url='https://t/old')], SINGLE, 'update', 1),
    ('TBA becomes an artist name',
     listing('Jane Doe', '2026-12-01', '20:00'),
     [row(1, 'TBA', '2026-12-01', '20:00')], SINGLE, 'update', 1),
    ('Title drift with tour name',
     listing('Jane Doe: Fall Tour', '2026-12-01', '20:00'),
     [row(1, 'Jane Doe Live', '2026-12-01', '20:00')], SINGLE, 'update', 1),
    ('Doors listed vs show listed',
     listing('Jane Doe', '2026-12-01', None, doors_time='19:00'),
     [row(1, 'Jane Doe', '2026-12-01', '20:00')], SINGLE, 'update', 1),
    ('No time, only show that night',
     listing('Jane Doe', '2026-12-01'),
     [row(1, 'Jane Doe', '2026-12-01', '20:00')], SINGLE, 'update', 1),
    ('No time, early and late shows exist',
     listing('Jane Doe', '2026-12-01'),
     [row(1, 'Jane Doe', '2026-12-01', '19:00'), row(2, 'Jane Doe', '2026-12-01', '21:30')], MULTI, 'review', None),
    ('Early show vs late show, same comedian',
     listing('Jane Doe', '2026-12-01', '21:30'),
     [row(1, 'Jane Doe', '2026-12-01', '19:00')], MULTI, 'insert', None),
    ('Same slot, different act (takeover)',
     listing('John Roe', '2026-12-01', '20:00'),
     [row(1, 'Jane Doe', '2026-12-01', '20:00')], SINGLE, 'review', 1),
    ('Same title, same time, different room',
     listing('Jane Doe', '2026-12-01', '20:00', location='Upstairs'),
     [row(1, 'Jane Doe', '2026-12-01', '20:00', location='Downstairs')], SINGLE, 'review', 1),
    ('12:30am show dated a day apart',
     listing('Jane Doe', '2026-12-02', '00:30'),
     [row(1, 'Jane Doe', '2026-12-01', '00:30')], SINGLE, 'update', 1),
    # Decisions made in the 2026-11-02 design review
    ('Time change at a single-show venue updates in place',
     listing('Jane Doe', '2026-12-01', '21:00'),
     [row(1, 'Jane Doe', '2026-12-01', '19:00')], SINGLE, 'update', 1),
    ('Second night of a run is a new event, not a merge',
     listing('Jane Doe', '2026-12-02', '20:00', ticket_url='https://t/run'),
     [row(1, 'Jane Doe', '2026-12-01', '20:00', ticket_url='https://t/run')], SINGLE, 'insert', None),
    ('Source key with a new date reschedules',
     listing('Jane Doe', '2026-12-09', '20:00', ticket_url='https://t/123'),
     [row(1, 'Jane Doe', '2026-12-01', '20:00', ticket_url='https://t/123')], KEYED, 'reschedule', 1),
    ('A link to the venue\'s own site is never evidence',
     listing('John Roe', '2026-12-01', '20:00', event_url='https://venue.test/shows'),
     [row(1, 'Jane Doe', '2026-12-01', '20:00', event_url='https://venue.test/shows')],
     {'url': 'https://venue.test/shows'}, 'review', 1),
    ('Old unreliable_urls flag no longer affects matching',
     listing('Jane Doe', '2026-12-01', '20:00', event_url='https://t/e1'),
     [row(1, 'Jane Doe', '2026-12-01', '20:00', event_url='https://t/e1')],
     {'unreliable_urls': 'event_url'}, 'update', 1),
    ('Shared opener phrase does not match different headliners',
     listing('Deer Tick with special guest Presley Haile', '2026-12-01', '20:00'),
     [row(1, 'Cowboy Mouth with special guest Lvvrs', '2026-12-01', '20:00')], SINGLE, 'review', 1),
    ('Postponement: old date gone from the scrape, new date appears',
     listing('Jane Doe', '2026-12-15', '20:00'),
     [row(1, 'Jane Doe', '2026-12-01', '20:00')], SINGLE, 'review', 1),
    ('Recurring series row is never a postponement target',
     listing('Sunday Showcase', '2026-12-08', '20:00'),
     [row(1, 'Sunday Showcase', '2026-12-01', '20:00', recurring='yes')], SINGLE, 'insert', None),
    ('Friday and Saturday 11pm late shows stay separate (Riot, 2026-11 replay)',
     listing('Saturday Night Standup Comedy Late Show!', '2026-11-14', '23:00'),
     [row(1, 'Saturday Night Standup Comedy Late Show!', '2026-11-13', '23:00')], SINGLE, 'insert', None),
    ('Canceled row matched again is updated (reactivated)',
     listing('Jane Doe', '2026-12-01', '20:00'),
     [dict(row(1, 'Jane Doe', '2026-12-01', '20:00'), event_status='canceled')], SINGLE, 'update', 1),
]


def test_golden_cases():
    failures = []
    for label, ev, rows, cfg, want, want_target in CASES:
        d = decide(ev, rows, cfg)
        ok = d['decision'] == want and (want_target is None or d['target_id'] == want_target)
        if not ok:
            failures.append(f"{label}: got {d['decision']} → {d['target_id']} "
                            f"(score {d['score']}, {d['parts']}), want {want} → {want_target}")
    assert not failures, '\n'.join(failures)


def test_batch_duplicate_within_one_scrape():
    from datetime import date
    evs = [listing('Jane Doe', '2026-12-01', '20:00'), listing('Jane Doe', '2026-12-01', '20:00')]
    decisions, _, _, _ = em.decide_batch(evs, {V: []}, {'v': SINGLE}, today=date(2026, 11, 1))
    assert [d['decision'] for d in decisions] == ['insert', 'batch_duplicate'], decisions


def test_two_shows_cannot_land_on_one_row():
    from datetime import date
    evs = [listing('Jane Doe', '2026-12-01', '19:00'), listing('Jane Doe', '2026-12-01', '21:30')]
    decisions, _, _, _ = em.decide_batch(evs, {V: [row(1, 'Jane Doe', '2026-12-01', '19:00')]}, {'v': MULTI},
                                      today=date(2026, 11, 1))
    assert decisions[0]['decision'] == 'update' and decisions[1]['decision'] == 'insert', decisions


def test_best_score_wins_regardless_of_page_order():
    from datetime import date
    # The weaker match comes first on the page; the row still goes to the stronger one.
    evs = [listing('Jane Doe Trio', '2026-12-01', '21:00'), listing('Jane Doe', '2026-12-01', '20:00')]
    decisions, _, _, _ = em.decide_batch(evs, {V: [row(1, 'Jane Doe', '2026-12-01', '20:00')]}, {'v': SINGLE},
                                         today=date(2026, 11, 1))
    assert decisions[1]['decision'] == 'update' and decisions[1]['target_id'] == 1, decisions
    assert decisions[0]['decision'] != 'update', decisions


def test_city_calendar_feeds_are_excluded():
    from datetime import date
    decisions, _, _, _ = em.decide_batch([listing('Jane Doe', '2026-12-01')], {V: []},
                                         {'v': {'ical_url': 'https://example.test/feed.ics'}}, today=date(2026, 11, 1))
    assert decisions[0]['decision'] == 'excluded', decisions


def test_recurring_series_inherited_at_insert():
    from datetime import date
    rows = [row(1, 'Sunday Showcase', '2026-12-06', '20:00', recurring='yes')]
    decisions, _, _, _ = em.decide_batch([listing('Sunday Showcase', '2026-12-13', '20:00')], {V: rows},
                                         {'v': SINGLE}, today=date(2026, 11, 1))
    assert decisions[0]['decision'] == 'insert' and decisions[0]['recurring'] == 'yes', decisions


def test_key_match_does_not_leave_stored_copies_missing():
    from datetime import date
    rows = [row(1, 'Jane Doe', '2026-12-01', '20:00', ticket_url='https://t/123'),
            row(2, 'Jane Doe', '2026-12-01', '20:00')]          # an older stored copy
    decisions, claimed, best, _ = em.decide_batch(
        [listing('Jane Doe', '2026-12-01', '20:00', ticket_url='https://t/123')], {V: rows}, {'v': KEYED},
        today=date(2026, 11, 1))
    assert decisions[0]['target_id'] == 1
    assert em.missing_rows(rows, claimed, best, set()) == [], (claimed, best)


if __name__ == '__main__':
    for t in (test_golden_cases, test_batch_duplicate_within_one_scrape, test_two_shows_cannot_land_on_one_row,
              test_best_score_wins_regardless_of_page_order, test_city_calendar_feeds_are_excluded,
              test_recurring_series_inherited_at_insert, test_key_match_does_not_leave_stored_copies_missing):
        try:
            t()
            print('PASS', t.__name__)
        except AssertionError as e:
            print('FAIL', t.__name__, '\n', e)
