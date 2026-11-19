"""Try extraction prompts on cached page text. Writes nothing to the DB.

  python prompt_lab.py arena_theatre --prompts minimal,prod --runs 3 [--refetch]

Page text is fetched once with production settings and cached in debug_exports/prompt_lab/.
Arena is scored against the dates in its own /event/MMDDYY-slug links.
"""
import argparse
import json
import os
import re
from datetime import datetime

from rapidfuzz import fuzz, utils

os.environ.setdefault('SKIP_DELAY', '1')
import universal_scraper as us  # noqa: E402

CACHE = 'debug_exports/prompt_lab'
JSON_LINE = 'Return ONLY valid JSON with an "events" array containing ALL events found.'


BASE_FIELDS = ['name', 'start_date', 'event_url']
EXTRA = {'src': 'date_source: the exact text you read start_date from', 'time': 'start_time: HH:MM format (24-hour) or null', 'type': 'event_type', 'genre': 'genre'}
TZ_LINE = " Don't apply any timezone conversion."


def build_prompt(venue_name, venue_url, extras=(), tz=False):
    fields = '\n'.join(f'- {f}' for f in BASE_FIELDS + [EXTRA[x] for x in extras])
    return f"""You are an expert at extracting structured event data from venue websites. The name of this venue is "{venue_name}", and it's website is "{venue_url}". Today's date is {datetime.now().strftime('%Y-%m-%d')}.{TZ_LINE if tz else ''}

Fields to extract:
{fields}

{JSON_LINE}"""


def variant(name):
    """'min', 'min+time', 'min+time+type+tz', ... ; 'prod' is production."""
    parts = name.split('+')[1:]
    return lambda vn, vu: build_prompt(vn, vu, [x for x in parts if x != 'tz'], 'tz' in parts)


def page_text(key, refetch=False):
    os.makedirs(CACHE, exist_ok=True)
    path = f'{CACHE}/{key}.txt'
    if refetch or not os.path.exists(path):
        v = us.VENUES[key]
        text, _ = us.scrape_page(v['url'], wait_time=v.get('wait_time', 3), scroll_count=v.get('scroll_count', 1),
                                 load_more_id=v.get('load_more_id'),
                                 pagination_sentinel=v.get('pagination_sentinel'), **us.trim_options(v))
        open(path, 'w').write(text)
    return open(path).read()


def run_prompt(name, key, text):
    v = us.VENUES[key]
    if name in ('prod', 'full', 'minimal', 'scoring'):
        return us.extract_events_with_llm_raw(text[:100000], v['name'], v.get('venue_url') or v['url'],
                                              venue_instruction=v.get('venue_instruction'),
                                              prompt_tier='full' if name == 'prod' else name).get('events', [])
    system = variant(name)(v['name'], v.get('venue_url') or v['url'])
    return us.get_llm_response(system, f"Extract all events:\n\n{text[:100000]}").get('events', [])


def arena_truth(text):
    """{slug words: date} from the venue's own links."""
    out = {}
    for mm, dd, yy, slug in re.findall(r'/event/(\d{2})(\d{2})(\d{2})-([a-z0-9-]+)', text):
        out[slug.replace('-', ' ')] = f'20{yy}-{mm}-{dd}'
    return out


def score_arena(events, truth):
    right, wrong, unmatched = 0, [], []
    for e in events:
        m = re.search(r'/event/(\d{2})(\d{2})(\d{2})-', e.get('event_url') or e.get('ticket_url') or '')
        if m:
            link = f'20{m.group(3)}-{m.group(1)}-{m.group(2)}'
            if e.get('start_date') == link:
                right += 1
            else:
                wrong.append(f"{e.get('start_date')} (link {link}) {e.get('name')}")
            continue
        best = max(truth, key=lambda s: fuzz.token_set_ratio(s, e.get('name') or '', processor=utils.default_process))
        if fuzz.token_set_ratio(best, e.get('name') or '', processor=utils.default_process) < 70:
            unmatched.append(f"{e.get('start_date')} {e.get('name')}")
        elif e.get('start_date') == truth[best]:
            right += 1
        else:
            wrong.append(f"{e.get('start_date')} (link {truth[best]}) {e.get('name')}")
    return right, wrong, unmatched


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('venue')
    ap.add_argument('--prompts', default='min,prod')
    ap.add_argument('--runs', type=int, default=3)
    ap.add_argument('--refetch', action='store_true')
    args = ap.parse_args()
    text = page_text(args.venue, args.refetch)
    truth = arena_truth(text) if args.venue == 'arena_theatre' else None
    print(f'{args.venue}: {len(text)} chars' + (f', {len(truth)} linked shows' if truth else ''))
    results = {}
    for p in args.prompts.split(','):
        for i in range(args.runs):
            evs = run_prompt(p, args.venue, text)
            results.setdefault(p, []).append(evs)
            line = f'{p:22} run {i + 1}: {len(evs):3} events'
            if truth:
                right, wrong, unmatched = score_arena(evs, truth)
                line += f'  right {right}  wrong {len(wrong)}  unmatched {len(unmatched)}'
                for w in wrong[:8]:
                    line += f'\n     wrong: {w}'
                for u in unmatched[:5]:
                    line += f'\n     unmatched: {u}'
            print(line, flush=True)
    path = f'{CACHE}/{args.venue}_{datetime.now():%Y%m%d_%H%M}.json'
    json.dump(results, open(path, 'w'), indent=1)
    print(f'Saved {path}')


if __name__ == '__main__':
    main()
