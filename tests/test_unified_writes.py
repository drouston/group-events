"""Unified write path against an in-memory SQLite events table (no production access).
Run: python tests/test_unified_writes.py"""
import os
import sqlite3
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('SKIP_DELAY', '1')
os.environ['MATCH_ENGINE'] = 'unified'
import universal_scraper as us  # noqa: E402
import event_matching as em  # noqa: E402

KEY = 'bad_astronaut'
VENUE = us.VENUES[KEY]['name']
D1 = (date.today() + timedelta(days=30)).isoformat()
D2 = (date.today() + timedelta(days=37)).isoformat()


def db():
    conn = sqlite3.connect(':memory:')
    c = conn.cursor()
    c.execute('''CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, start_date TEXT, end_date TEXT,
                 doors_time TEXT, start_time TEXT, end_time TEXT, multi_day BOOLEAN, venue TEXT, location TEXT,
                 city TEXT, state TEXT, price TEXT, ticket_url TEXT, event_url TEXT, venue_url TEXT, description TEXT,
                 genre TEXT, confidence TEXT, status TEXT, review_status TEXT, event_status TEXT, created_at TEXT,
                 event_type TEXT, visible BOOLEAN, sold_out BOOLEAN DEFAULT 0, date_changed BOOLEAN DEFAULT 0,
                 openers TEXT, duplicate_of_id INTEGER, flags TEXT, source TEXT, recurring TEXT,
                 cancel_flagged_at TEXT)''')
    return conn, c


def add(c, name, d, t, review='approved', event='scheduled', link=None, flagged=None):
    c.execute('''INSERT INTO events (name, start_date, start_time, venue, review_status, event_status, status,
                 ticket_url, cancel_flagged_at) VALUES (?,?,?,?,?,?,?,?,?)''',
              (name, d, t, VENUE, review, event, us.legacy_status(review, event, None), link, flagged))
    return c.lastrowid


def rows(c):
    c.execute(f"SELECT {', '.join(us.MATCH_ROW_COLS)} FROM events")
    out = [dict(zip(us.MATCH_ROW_COLS, r)) for r in c.fetchall()]
    return {VENUE: out}


def listing(name, d, t, link=None):
    return {'name': name, 'start_date': d, 'start_time': t, 'venue': VENUE, 'venue_key': KEY,
            'ticket_url': link, 'event_type': 'music'}


def run(c, events):
    shadow = {}
    decisions, claimed, best, _ = em.decide_batch(events, rows(c), us.VENUES)
    shadow.update(decisions=decisions, claimed=claimed, best_against=best, rows_by_venue=rows(c))
    stats = dict.fromkeys(('inserted', 'updated', 'date_corrections', 'content_changes', 'link_changes',
                           'skipped', 'flagged'), 0)
    us.apply_unified(c, '?', events, shadow, False, stats)
    canceled = us.cancel_unified(c, '?', events, shadow)
    return decisions, stats, canceled


def get(c, rid):
    c.execute("SELECT name, start_date, start_time, review_status, event_status, cancel_flagged_at, duplicate_of_id "
              "FROM events WHERE id = ?", (rid,))
    return c.fetchone()


def test_update_reschedule_insert():
    conn, c = db()
    a = add(c, 'Jane Doe', D1, '20:00', link='https://t/1')
    b = add(c, 'Old Act', D1, '19:00', link='https://t/2')
    evs = [listing('Jane Doe Live', D1, '20:00', 'https://t/1'),     # title drift: update
           listing('Old Act', D2, '19:00', 'https://t/2'),           # source key, new date: reschedule
           listing('New Show', D2, '21:00', 'https://t/3')]          # new
    decisions, stats, _ = run(c, evs)
    assert [d['decision'] for d in decisions] == ['update', 'reschedule', 'insert'], decisions
    assert get(c, a)[0] == 'Jane Doe Live'
    assert get(c, b)[1] == D2 and get(c, b)[3] == 'approved', get(c, b)   # reschedule keeps status
    assert stats['inserted'] == 1 and stats['updated'] == 2 and stats['date_corrections'] == 1, stats


def test_review_links_and_cancellation_two_misses():
    conn, c = db()
    keep = add(c, 'Jane Doe', D1, '20:00', link='https://t/1')
    gone = add(c, 'Gone Act', D2, '20:00', link='https://t/9')
    filler = [add(c, f'Filler Band {chr(65+i)}', D2, f'1{i}:00', link=f'https://t/f{i}') for i in range(8)]
    # Jane Doe is no longer listed; a different act took her slot (a takeover): review, linked.
    evs = [listing(f'Filler Band {chr(65+i)}', D2, f'1{i}:00', f'https://t/f{i}') for i in range(8)] + \
          [listing('John Roe', D1, '20:00', 'https://t/x')]
    decisions, stats, canceled = run(c, evs)
    assert decisions[-1]['decision'] == 'review' and stats['flagged'] == 1, decisions[-1]
    c.execute("SELECT duplicate_of_id, review_status FROM events WHERE name = 'John Roe'")
    assert c.fetchone() == (keep, 'pending')
    assert get(c, gone)[5] is not None and get(c, gone)[4] == 'scheduled'   # first miss: flagged only
    decisions, stats, canceled = run(c, evs[:-1])
    assert canceled == 1 and get(c, gone)[4] == 'canceled', get(c, gone)    # second miss: canceled


def test_canceled_row_reactivates():
    conn, c = db()
    rid = add(c, 'Jane Doe', D1, '20:00', event='canceled', link='https://t/1')
    filler = [add(c, f'Filler Band {chr(65+i)}', D2, f'1{i}:00') for i in range(4)]
    run(c, [listing('Jane Doe', D1, '20:00', 'https://t/1')] + [listing(f'Filler Band {chr(65+i)}', D2, f'1{i}:00') for i in range(4)])
    assert get(c, rid)[4] == 'scheduled' and get(c, rid)[3] == 'pending', get(c, rid)


if __name__ == '__main__':
    for t in (test_update_reschedule_insert, test_review_links_and_cancellation_two_misses, test_canceled_row_reactivates):
        try:
            t()
            print('PASS', t.__name__)
        except AssertionError as e:
            print('FAIL', t.__name__, '\n', e)
