"""Compare LLM extraction on production page text vs page_trim/window variants. Writes nothing to the DB.

  python compare_trim.py --measure [venue ...]   # page sizes only, no LLM (all LLM venues if none given)
  python compare_trim.py venue [venue ...]       # extract both versions --runs times each and compare

Per venue: page sizes, events found, events consistently found by only one version, and how many
of the venue's active future DB rows each version's runs cover (a missed row is a false cancellation
risk). Report saved to debug_exports/trim_compare_<timestamp>.json.
"""
import argparse
import json
import os
import re
from datetime import datetime
from difflib import SequenceMatcher
from urllib.parse import urljoin

from bs4 import BeautifulSoup

os.environ.setdefault('SKIP_DELAY', '1')
import universal_scraper as us  # noqa: E402

NON_LLM = ('timely_calendar_id', 'seetickets', 'ajax_url', 'ical_url')
PARSER_VENUES = {'white_oak', 'mucky_duck'}
CHAR_LIMIT = 100000


def llm_venues():
    return [k for k, v in us.VENUES.items() if not any(v.get(f) for f in NON_LLM) and k not in PARSER_VENUES]


def fetch(venue, trim):
    """Production fetch sequence (single page or paginated). Returns (text, [(url, html)])."""
    kw = dict(wait_time=venue.get('wait_time', 3), scroll_count=venue.get('scroll_count', 1),
              load_more_id=venue.get('load_more_id'), trim_hidden_text=trim)
    if not venue.get('paginated'):
        text, html = us.scrape_page(venue['url'], pagination_sentinel=venue.get('pagination_sentinel'), **kw)
        return text, [(venue['url'], html)]
    texts, pages = [], []
    for start in venue.get('urls') or [venue['url']]:
        url, n = start, 1
        while url and n <= venue.get('max_pages', 10):
            text, html = us.scrape_page(url, **kw)
            texts.append(text)
            pages.append((url, html))
            nxt = BeautifulSoup(html, 'html.parser').select_one(venue.get('next_page_selector', '#moreshowsbtn'))
            url = urljoin(url, nxt['href']) if nxt and nxt.get('href') else None
            n += 1
    return '\n' + '\n'.join(texts), pages


_MONTH = r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?'
_DATE = re.compile(rf'\b{_MONTH}\s+(\d{{1,2}})\b|\b(\d{{1,2}})\s+{_MONTH}\b|\b(\d{{1,2}})/(\d{{1,2}})\b', re.I)


def page_dates(text):
    """Distinct month-day mentions: a rough, LLM-free check that trimming kept the listing."""
    out = set()
    for m in _DATE.finditer(text):
        g = m.groups()
        out.add((g[0][:3].lower(), g[1]) if g[0] else (g[3][:3].lower(), g[2]) if g[3] else (g[4], g[5]))
    return out


def measure(key):
    venue = us.VENUES[key]
    _, pages = fetch(venue, trim=True)
    texts = {'full': '', 'no_hidden': '', 'trimmed': ''}
    for url, html in pages:
        texts['full'] += us.html_to_text(html, url)
        texts['no_hidden'] += us.html_to_text(html, url, drop_hidden=True)
        texts['trimmed'] += us.html_to_text(html, url, drop_hidden=True, dedupe=True)
    res = {k: len(v) for k, v in texts.items()}
    full_dates, trim_dates = page_dates(texts['full']), page_dates(texts['trimmed'])
    res['dates'] = [len(full_dates), len(trim_dates)]
    res['dates_lost'] = sorted(map(str, full_dates - trim_dates))[:20]
    return res


def norm(name):
    return re.sub(r'[^a-z0-9]', '', (name or '').lower())


def same(a, b):
    if a.get('start_date') != b.get('start_date'):
        return False
    links_a = {a.get('ticket_url'), a.get('event_url')} - {None, ''}
    links_b = {b.get('ticket_url'), b.get('event_url')} - {None, ''}
    if links_a & links_b:
        return True
    return SequenceMatcher(None, norm(a.get('name')), norm(b.get('name'))).ratio() >= 0.6


def extract(venue, text, runs):
    out = []
    for _ in range(runs):
        splits_before = us.SPLIT_COUNTS.get(venue['name'], 0)
        evs = us.extract_events_with_llm_raw(text[:CHAR_LIMIT], venue_name=venue['name'],
                                             venue_url=venue.get('venue_url') or venue.get('url', ''),
                                             venue_instruction=venue.get('venue_instruction', ''))
        today = datetime.now().date().isoformat()
        evs = [e for e in evs.get('events', []) if (e.get('start_date') or '') >= today]
        out.append({'events': evs, 'splits': us.SPLIT_COUNTS.get(venue['name'], 0) - splits_before})
    return out


def consistent_only(runs_a, runs_b):
    """Events in every run of A that no run of B found."""
    first, rest = runs_a[0]['events'], runs_a[1:]
    found = [e for e in first if all(any(same(e, x) for x in r['events']) for r in rest)]
    return [e for e in found if not any(same(e, x) for r in runs_b for x in r['events'])]


def db_rows(key):
    conn = us.get_db_connection()
    c = conn.cursor()
    c.execute('''SELECT id, name, start_date, ticket_url, event_url FROM events
                 WHERE (venue = %s OR source = %s) AND start_date >= %s
                 AND review_status IN ('pending', 'approved') AND event_status != 'canceled' ''',
              (us.VENUES[key]['name'], key, datetime.now().date().isoformat()))
    rows = [dict(zip(('id', 'name', 'start_date', 'ticket_url', 'event_url'), r)) for r in c.fetchall()]
    conn.close()
    return rows


def missed_rows(rows, runs):
    """Rows not covered by at least one run (what a cancellation check would see as missing)."""
    return [r for r in rows if any(not any(same(r, e) for e in run['events']) for run in runs)]


def short(e):
    return f"{e.get('start_date')} {e.get('start_time') or '--:--'} {(e.get('name') or '')[:50]}"


# name: (desktop window + hidden marks, drop hidden, remove repeats). 'base' is production (756px,
# duplicate text trimmed); 'raw' is untrimmed.
VARIANTS = {'base': (False, False, True), 'raw': (False, False, False), 'trim': (True, True, True),
            'wide': (True, False, True)}


def compare(key, runs, variants):
    venue = us.VENUES[key]
    pages = {wide: fetch(venue, trim=wide)[1] for wide in {VARIANTS[v][0] for v in ['base'] + variants}}
    texts = {v: '\n'.join(us.html_to_text(h, u, drop_hidden=VARIANTS[v][1], dedupe=VARIANTS[v][2])
                          for u, h in pages[VARIANTS[v][0]])
             for v in ['base'] + variants}
    out = {v: extract(venue, texts[v], runs) for v in texts}
    rows = db_rows(key)
    res = {'db_rows': len(rows)}
    for v, r in out.items():
        res[v] = {'chars': len(texts[v]), 'events': [len(x['events']) for x in r],
                  'splits': sum(x['splits'] for x in r),
                  'db_missed': [short(x) for x in missed_rows(rows, r)]}
        if v != 'base':
            res[v]['lost'] = [short(e) for e in consistent_only(out['base'], r)]
            res[v]['gained'] = [short(e) for e in consistent_only(r, out['base'])]
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('venues', nargs='*')
    ap.add_argument('--measure', action='store_true')
    ap.add_argument('--runs', type=int, default=2)
    ap.add_argument('--variants', default='trim', help=f"comma list of {', '.join(v for v in VARIANTS if v != 'base')}")
    args = ap.parse_args()
    keys = args.venues or (llm_venues() if args.measure else [])
    if not keys:
        ap.error('name venues to compare, or use --measure')

    results = []
    for key in keys:
        try:
            results.append({'venue': key, **(measure(key) if args.measure else compare(key, args.runs, args.variants.split(',')))})
        except Exception as e:
            results.append({'venue': key, 'error': str(e)})
        print(json.dumps(results[-1], indent=1))

    os.makedirs('debug_exports', exist_ok=True)
    path = f"debug_exports/trim_{'measure' if args.measure else 'compare'}_{datetime.now():%Y%m%d_%H%M}.json"
    with open(path, 'w') as f:
        json.dump(results, f, indent=1)

    print('\n' + '=' * 70)
    for r in results:
        if 'error' in r:
            print(f"{r['venue']:24} ERROR {r['error'][:60]}")
        elif args.measure:
            print(f"{r['venue']:24} full {r['full']:>7}  no_hidden {r['no_hidden']:>7}  trimmed {r['trimmed']:>7}"
                  f"  ({100 - 100 * r['trimmed'] // max(r['full'], 1)}% smaller)  dates {r['dates'][0]}->{r['dates'][1]}")
        else:
            for v in [k for k in r if k in VARIANTS]:
                x = r[v]
                diff = f"lost {len(x['lost']):>2} gained {len(x['gained']):>2}" if v != 'base' else ' ' * 18
                print(f"{r['venue'] if v == 'base' else '':22} {v:7} chars {x['chars']:>7}  events {str(x['events']):9} "
                      f"splits {x['splits']}  {diff}  DB missed {len(x['db_missed'])} of {r['db_rows']}")
    print(f"\nSaved {path}")


if __name__ == '__main__':
    main()
