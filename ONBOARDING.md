# Venue Onboarding Playbook

A standard process for adding a new venue to `universal_scraper.py`'s `VENUES` dict. Follow this in order; update this file when a new venue teaches us something new (a new platform quirk, a new failure mode, a new config option). Per-venue data lives in [VENUES.md](VENUES.md), not here — this file is process only.

---

## 1. Find the source

Look for the venue's **own** events page first — search `<venue name> Houston events`, then check their official site directly for a `/events`, `/calendar`, `/shows`, or similar page.

**Acceptable sources, in order of preference:**
1. The venue's own website (custom calendar, embedded widget, etc.)
2. A pure ticketing backend the venue embeds to sell tickets — Etix, Prekindle, Ticketweb, Square, Timely, SeeTickets. These are infrastructure, not consumer destinations.
3. The venue's own social media (e.g. a Facebook Events tab) — only if it's genuinely the venue's primary announcement channel and reasonably fresh/complete.

**Never acceptable, even as a fallback:** general events aggregators/discovery platforms — Do713, Eventsfy, 365thingsinhouston, and **Eventbrite itself** (including a venue-scoped organizer page). These are direct competitors to this app. If the only lead is one of these, treat the venue as blocked and say so — don't route around it silently. See [VENUES.md](VENUES.md) for precedent (1810 Ojeman was left unonboarded for exactly this reason).

If there's no viable source at all (WAF-blocked ticketing widget with no venue-owned calendar, stale/incomplete social presence, etc.), **skip the venue** and log it as blocked in [VENUES.md](VENUES.md) rather than forcing a bad source in.

## 2. Identify the platform pattern

Popular venue platforms recur. Recognize them so you're not solving the same problem from scratch:

| Platform | Signal | Handling |
|---|---|---|
| Prekindle | URL contains `prekindle.com` | Standard `url` + LLM extraction works fine |
| Etix | URL contains `etix.com` | Often sits behind an AWS WAF bot-challenge (202 status, empty body) — test with a real dry-run before assuming it works |
| SpaceCraft (Wix-based comedy/music club sites) | Page footer says "Created with SpaceCraft" / `gospacecraft.com` | Often paginates via scroll-triggered `IntersectionObserver`, not a clickable "load more" — see §4 below |
| Timely | Shared multi-venue calendar | Use `timely_calendar_id`, bypasses Selenium+LLM entirely (see `scrape_timely_api`) |
| SeeTickets | — | Use `"seetickets": True`, custom parser (`parse_seetickets_html`), no LLM |
| Google/Outlook ICS feed | `.ics` link on the page | Use `ical_url` + `"scraper": "google_ics"` — but this path assumes a **multi-venue** calendar with per-event `LOCATION` filtering; a single-venue ICS feed doesn't need that machinery and is usually better scraped as a normal HTML page instead |

## 3. Write the minimal VENUES entry

Start as small as possible:

```python
"venue_key": {
    "name": "Venue Display Name",
    "url": "https://venue-site.com/events",
    "city": "Houston",
    "state": "TX",
    "wait_time": 5,
},
```

**Do not add `venue_instruction` preemptively.** Test standard extraction first — it's usually good enough, and over-instructing has caused inconsistencies before. Only add `venue_instruction` if a dry-run shows the LLM actually getting something wrong (wrong `event_type`, wrong room/location field, bleed-through from a sibling venue on a shared page, etc.), and keep the instruction narrowly scoped to that specific failure.

Other config knobs, add only as needed:

| Key | Purpose |
|---|---|
| `scroll_count` | Number of scroll iterations for lazy-loaded content |
| `pagination_sentinel` | CSS selector of an infinite-scroll trigger element — scrolls *that element* into view instead of `document.body.scrollHeight`. Use this instead of just cranking `scroll_count` when a page uses IntersectionObserver-based infinite scroll (see §4) |
| `load_more_id` | Element ID of a clickable "Load More" button (clicked repeatedly until it disappears) |
| `paginated` + `next_page_selector` + `max_pages` | Multi-page calendars navigated via a real "next page" link/URL |
| `aliases` | Alternate names this venue is known by elsewhere (ICS feeds, etc.) so cross-source dedup recognizes them |
| `event_type` | Force a single event_type for every event from this venue (only when that's actually true for 100% of their events, e.g. a dedicated comedy club) |
| `allows_multi_day` | Marks this venue as hosting genuine continuous multi-day events (festivals, expos) so `expand_multi_night_events` does NOT split its date ranges into nightly rows |
| `duplicate_threshold` | Override the default fuzzy-match ratio for possible-duplicate detection |
| `auto_approve` | Auto-approve this venue's events on insert (rare — usually an onboarding-run flag instead, not a permanent venue setting) |

## 4. Diagnose pagination/scroll problems properly — don't guess

If a dry-run returns suspiciously few events (e.g. exactly a round number like 12, 20, 25 — that's almost always a widget's default page size), **don't just crank `scroll_count` up blindly and hope**. Diagnose it:

1. Fetch the page's raw HTML/JS (`curl` or WebFetch) and look for the pagination mechanism: is there a `data-collections`/API config embedded (base64 JSON is common), a `next page` link, a numbered "load more" button, or a scroll-triggered `IntersectionObserver`?
2. If it's scroll-triggered, write a standalone Selenium script (see example pattern below) that scrolls, then inspects:
   - `performance.getEntriesByType('resource')` filtered to the site's own API/CDN domain — did a request for more data even fire?
   - The sentinel/trigger element's `getBoundingClientRect()` before and after scrolling — a naive `scrollTo(0, document.body.scrollHeight)` frequently **overshoots** the actual trigger element (trailing footer content pushes it above the viewport before the observer ever sees it), which silently caps results with zero errors logged.
3. If the sentinel is overshot, use `pagination_sentinel` (added to `scrape_page()` specifically for this) — it does `element.scrollIntoView({block:'center'})` on the given selector each iteration instead of jumping to the document's absolute bottom.
4. If zero API requests fire at all even after fixing the scroll target, consider bot detection (`navigator.webdriver`, signed/rate-limited requests) before concluding the page can't be scraped — but don't reverse-engineer a signed private API as a first resort; that's high-effort and fragile. Try the straightforward fixes first, and if none work, surface the finding and ask rather than silently shipping a degraded (e.g. near-term-only) result.

Standalone diagnostic script pattern (adjust selectors/URL):
```python
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
opts = Options()
opts.add_argument('--headless'); opts.add_argument('--no-sandbox')
opts.add_argument('--disable-dev-shm-usage'); opts.add_argument('--disable-gpu')
driver = webdriver.Chrome(options=opts)
driver.get("https://venue-site.com/events")
time.sleep(5)
for i in range(8):
    driver.execute_script("var e=document.querySelector('SENTINEL_SELECTOR'); if(e) e.scrollIntoView({block:'center'});")
    time.sleep(2.5)
print(driver.execute_script("return document.querySelectorAll('ITEM_SELECTOR').length"))
driver.quit()
```

## 5. Dry-run

```
python3 universal_scraper.py --mode onboard --venue <key> --dry-run
```

Check:
- **Event count** — does it roughly match what the venue's own site visibly shows? If it's suspiciously capped, go back to §4.
- **Dates** — spot-check a few, especially any without an explicit year in the source text. Bare `month/day` dates (e.g. "Feb 18") can get extracted with the *current* year even when the actual show is next year — `save_to_database`'s past-date filter then silently drops them as already-expired. This is a known open LLM-extraction risk, not venue-specific; flag it if seen, don't assume it's fixed.
- **Multi-night events** — a date range (e.g. a comedian's Thu–Sat residency) should show as `start_date`/`end_date` differing in the dry-run preview; `expand_multi_night_events` (runs inside `save_to_database`, not shown in dry-run output) will split these into one row per night on the real save, unless the venue has `allows_multi_day: True` for genuine continuous events.
- **event_type / location classification** — only add `venue_instruction` here if something's actually wrong (see §3).
- **Duplicates flagged** — a handful of `possible_duplicate` flags for same-night-different-showtime events (e.g. "8pm" vs "10pm" showcase) is a known fuzzy-match false-positive pattern, not necessarily a bug — but do a quick manual check in the dashboard after saving.

## 6. Report dry-run results back to the user

Before saving anything for real, summarize what the dry-run found — don't just say "looks good." Cover:

- **Hosting platform** identified (Prekindle, SpaceCraft, Etix, custom WordPress, Timely, etc.) and which config knobs from §3 were needed to make it work.
- **Event count** extracted.
- **Capture quality**, field by field: how many events got a clean `start_date`? A `start_time`? A `ticket_url` and/or `event_url`? Call out any gaps or suspicious patterns (e.g. every event missing a time, all URLs pointing to the same generic page).
- **Extra fields caught**, if relevant: `doors_time`, `location`/room, `multi_day` ranges, `openers`, `sold_out`/`date_changed` flags — whatever this venue's page actually exposes.

Then update this venue's row in [VENUES.md](VENUES.md) with these stats before moving on.

## 7. Save for real

```
python3 universal_scraper.py --mode onboard --venue <key>
```
(no `--auto-approve` by default — let events land as `pending` for manual review in the dashboard, unless there's a specific reason to trust the source enough to skip review).

## 8. Update the docs

- Add the venue to CLAUDE.md's **Venues** list.
- Update its row in [VENUES.md](VENUES.md) with final status (Onboarded/Blocked) and the real save's numbers if they differ from the dry-run.
- If you hit a new platform quirk, failure mode, or added a new VENUES config key, add it to this file (§2/§3/§4) so the next onboarding benefits.
