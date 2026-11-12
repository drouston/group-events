import os
import json
import re
import time
import sqlite3
from collections import defaultdict
from dateutil.utils import today
from difflib import SequenceMatcher
from status_model import migrate_status_columns, legacy_status
import event_matching as em
from urllib.parse import urljoin, urlparse
import requests
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from openai import OpenAI
import anthropic

DATABASE_URL = os.environ.get('DATABASE_URL')


def get_db_connection():
    if DATABASE_URL:
        import psycopg2
        db_url = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
        return psycopg2.connect(db_url)
    return sqlite3.connect('events.db')


def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute('''CREATE TABLE IF NOT EXISTS events
                     (id SERIAL PRIMARY KEY, name TEXT, date TEXT,
                      doors_time TEXT, start_time TEXT, venue TEXT,
                      city TEXT, state TEXT, price TEXT, ticket_url TEXT,
                      description TEXT, genre TEXT, confidence TEXT,
                      notes TEXT, status TEXT DEFAULT 'pending',
                      created_at TEXT, approved_at TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS past_events
                     (id SERIAL PRIMARY KEY, name TEXT, date TEXT,
                      doors_time TEXT, start_time TEXT, venue TEXT,
                      city TEXT, state TEXT, price TEXT, ticket_url TEXT,
                      description TEXT, genre TEXT, confidence TEXT,
                      notes TEXT, status TEXT, created_at TEXT,
                      approved_at TEXT, archived_at TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS venue_cache
                     (venue_key TEXT PRIMARY KEY, content_hash TEXT,
                      last_scraped TEXT)''')
    else:
        c.execute('''CREATE TABLE IF NOT EXISTS events
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, date TEXT,
                      doors_time TEXT, start_time TEXT, venue TEXT,
                      city TEXT, state TEXT, price TEXT, ticket_url TEXT,
                      description TEXT, genre TEXT, confidence TEXT,
                      notes TEXT, status TEXT DEFAULT 'pending',
                      created_at TEXT, approved_at TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS past_events
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, date TEXT,
                      doors_time TEXT, start_time TEXT, venue TEXT,
                      city TEXT, state TEXT, price TEXT, ticket_url TEXT,
                      description TEXT, genre TEXT, confidence TEXT,
                      notes TEXT, status TEXT, created_at TEXT,
                      approved_at TEXT, archived_at TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS venue_cache
                     (venue_key TEXT PRIMARY KEY, content_hash TEXT,
                      last_scraped TEXT)''')
    conn.commit()
    conn.close()


# Venue configurations
VENUES = {
    "bad_astronaut": {
        "name": "Bad Astronaut Brewing Co.",
        "url": "https://www.prekindle.com/events/bad-astronaut-brewing-co",
        "venue_url": "https://badastronautbeer.com/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 3,
        # Confirmed 2026-08-14 via the dupe_use_start_date verification procedure (see
        # CLAUDE.md): Prekindle ticket_url is unique per occurrence here too (same signature
        # as Heights Theater) — the one shared-URL case found was a date-drift pair with the
        # same event name on both dates, not a reused generic link.
        # Unified matching: link trust per field (key | evidence | ignore | drop).
        "link_trust": {"ticket_url": "key"},
        "dupe_use_start_date": False
    },
    "white_oak": {
        "name": "White Oak Music Hall",
        "url": "https://whiteoakmusichall.com",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 10,
        "venue_instruction": "Extract all events. For each event, populate the 'location' field with the specific area: 'Downstairs', 'Upstairs', or 'Lawn'. If no area is specified, use null."
    },
    "heights_theater": {
        "name": "Heights Theater",
        "url": "https://www.prekindle.com/events/theheights",
        "venue_url": "https://theheightstheater.com/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 3,
        # Confirmed 2026-07-30: ticket_url is a numeric Prekindle event ID, unique per
        # occurrence (not a reused profile/template link) — safe to match on ticket_url alone
        # and treat a date difference as a correction rather than a new event. This is what
        # let 51 duplicate rows accumulate here undetected before the cleanup that day.
        # Unified matching: link trust per field (key | evidence | ignore | drop).
        "link_trust": {"ticket_url": "key", "event_url": "ignore"},
        "dupe_use_start_date": False,
        # Added 2026-10-22: event_url is always just venue_url verbatim — Heights Theater has
        # no real per-event pages of its own, everything routes through Prekindle. Left
        # non-null, that generic shared value made detect_url_date_conflicts() flag nearly
        # every event here as a date-drift duplicate of whichever row had the lowest id —
        # same class of bug already fixed at HOB/Punch Line/Toyota Center, missed here.
        "unreliable_urls": "event_url"
    },
    "last_concert": {
        "name": "Last Concert Cafe",
        "url": "https://lastconcert.com/calendar/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 3
    },
    "dan_electros": {
        "name": "Dan Electro's Guitar Bar",
        "url": "https://danelectros.com/events/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5
    },
    "713_music_hall": {
        "name": "713 Music Hall",
        "url": "https://www.713musichall.com/shows",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5
    },
    "continental_club": {
        "name": "Continental Club Houston",
        "city": "Houston",
        "state": "TX",
        "timely_calendar_id": "54706359",
        "url": "https://www.continentalclub.com/houston",
    },
    "woodland_pavilion": {
        "name": "Cynthia Woods Mitchell Pavilion",
        "aliases": ["Woodlands Pavilion", "The Woodlands Pavilion"],
        "url": "https://www.woodlandscenter.org/events",
        "city": "The Woodlands",
        "state": "TX",
        "wait_time": 6
    },
    "house_of_blues": {
        "name": "House of Blues Houston",
        "url": "https://houston.houseofblues.com/shows",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        # Added 2026-07-30: page only ever rendered 36 of ~100+ current events regardless of
        # scroll_count (1 through 20 all produced byte-identical output) — the rendered DOM
        # stops at a Chakra UI spinner (<div class="chakra-spinner">Loading...</div>), a React
        # infinite-scroll trigger. scrollTo(0, document.body.scrollHeight) overshoots that
        # element instead of scrolling it into view, so the IntersectionObserver watching it
        # never fires and the next batch never loads — same class of bug pagination_sentinel
        # was built for (see Secret Group). scroll_count still controls how many times the
        # sentinel gets scrolled into view as it moves down the page with each new batch.
        "scroll_count": 12,
        "pagination_sentinel": ".chakra-spinner",
        "venue_instruction": "Extract all events. For each event, populate the 'location' field with the specific room: 'Music Hall', 'The Bronze Peacock', or 'Foundation Room'. If no room is specified, use 'Music Hall'.",
        # Confirmed 2026-09-24: this site has no per-event detail pages, so since the 08-14
        # ticket/event URL prompt split, event_url always comes back as the generic /shows
        # listing — same class as Punch Line's event_url. That one non-unique value (shared
        # across 88 rows / 74 dates) let save_to_database's cross-field URL-match query match
        # the wrong row via LIMIT 1 and abandon the real ticket_url match, and let
        # detect_url_date_conflicts() cross-link unrelated shows sharing it on different dates
        # — together responsible for ~85% of this venue's possible_duplicate backlog. Nulling
        # it forces dedup onto ticket_url (Ticketmaster, confirmed unique per event — no shared
        # ticket_url across >1 start_date found).
        # Unified matching: link trust per field (key | evidence | ignore | drop).
        "link_trust": {"event_url": "ignore"},
        "unreliable_urls": "event_url",
    },
    "smart_financial": {
        "name": "Smart Financial Centre",
        "url": "https://us.atgtickets.com/venues/smart-financial-centre/whats-on/us/concert/",
        "venue_url": "https://smartfinancialcentre.com/events",
        "city": "Sugar Land",
        "state": "TX",
        "wait_time": 6,
        "scroll_count": 3,
        # Added 2026-07-31 after Billy Strings (a real, still-listed show) got wrongly
        # confirmed canceled: the concert-only URL is genre-filtered server-side via the URL
        # path itself (no combined "all genres" view exists -- the bare /us/ path is just a
        # category-selector landing page), and within that one genre the listing itself is
        # paginated ("currentPage":1,"totalPages":2 in the page's own embedded data) with a
        # real MUI pagination link we never followed. Both gaps let real upcoming shows fall
        # outside what we ever saw, so check_canceled_events treated them as gone.
        "paginated": True,
        "next_page_selector": 'a[aria-label="Go to next page"]',
        "urls": [
            "https://us.atgtickets.com/venues/smart-financial-centre/whats-on/us/concert/",
            "https://us.atgtickets.com/venues/smart-financial-centre/whats-on/us/comedy/",
            "https://us.atgtickets.com/venues/smart-financial-centre/whats-on/us/dance/",
            "https://us.atgtickets.com/venues/smart-financial-centre/whats-on/us/events/",
            "https://us.atgtickets.com/venues/smart-financial-centre/whats-on/us/family/",
            "https://us.atgtickets.com/venues/smart-financial-centre/whats-on/us/talk/",
        ],
        # A single late-night show (e.g. Jill Scott, 7:30pm CDT) gets displayed by ATG as a
        # date range ("Sun, Aug 30 - Mon, Aug 31") because it technically runs past midnight --
        # confirmed via the page's own JSON-LD, which shows one performance, not two. Without
        # this flag, expand_multi_night_events() treats that range as a genuine multi-night
        # residency and splits it into two duplicate calendar rows.
        "allows_multi_day": True,
    },
    "bayou_music_center": {
        "name": "Bayou Music Center",
        "url": "https://www.bayoumusiccenter.com/shows",
        "city": "Houston",
        "state": "TX",
        "wait_time": 8,
        "scroll_count": 5
    },
    "improv_tx": {
        "name": "Improv Houston",
        "url": "https://improvtx.com/calendar/houston/-/",
        "venue_url": "https://improvtx.com/houston/calendar/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 1,
        "paginated": True,
        "next_page_selector": "#moreshowsbtn",
        # Added 2026-07-31: /houston/event/<slug>/<digits>/ links carry a trailing numeric
        # segment that looks like a per-event ID but isn't one — confirmed by diffing two
        # "different" IDs' HTML (only the ID echoed back differed) and by a bogus ID still
        # resolving. Stripped so the same show's link is stable across re-scrapes; doesn't
        # touch /houston/comic/<name>/ profile pages (no trailing digits to match).
        "strip_end_url": r'\d+/$',
    },
    "riot_comedy": {
        "name": "The Riot Comedy Club",
        "url": "https://www.theriothtx.com/calendar",
        "city": "Houston",
        "state": "TX",
        "wait_time": 6,
        "scroll_count": 5,
        # Confirmed 2026-09-23: nightly Showcase + Open Mic (or Late Showcase) at different
        # times with near-identical titles — same false-positive class as Secret Group (ce1dcdf).
        "dupe_check_start_time": True,
        "multi_show": True,
    },
    "axelrad": {
        "name": "Axelrad Beer Garden",
        "url": "https://www.axelradhouston.com/music",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
        "venue_instruction": "Extract all upcoming concerts from the 'Upcoming Events' section. Each event follows this exact pattern: first a date like '5.9.26', then a time like '8pm doors' or '6pm doors 7pm music', then the artist name, then the stage/location. Use the doors time as doors_time in HH:MM 24hr format. If a music start time is also listed, use that as start_time. The location is 'The Attic' or 'Main Stage'."    },
    "warehouse_live": {
        "name": "Warehouse Live Midtown",
        "url": "https://warehouselivemidtown.com/calendar/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
        "seetickets": True
    },
    "toyota_center": {
        "name": "Toyota Center",
        "url": "https://www.toyotacenter.com/events",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
        "load_more_id": "loadMoreEvents"
        # unreliable_urls removed 2026-08-16: the site's per-event URL slugs (e.g.
        # /events/detail/grupo-duelo) are unique despite the generic anchor text, and the
        # inline-links fix now surfaces them correctly instead of relying on positional
        # anchor-text pairing.
    },
    "nrg_park": {
        # Renamed 2026-10-01: NRG's naming-rights deal lapsed and the complex reverted to its
        # earlier Reliant branding — confirmed live, not a rumor: nrgpark.com's own event pages
        # already redirect to reliantpark.com, and the old site is being kept up only as a
        # redirect shim. "NRG Park" kept in aliases since in-flight sources (the city ICS
        # calendar, old cached links) will keep using the old name for a while.
        "name": "Reliant Park",
        "url": "https://www.reliantpark.com/events-tickets/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
        "allows_multi_day": True,
        "aliases": ["NRG Park"],
        # Distinct buildings within the complex, not alternate names for the complex itself —
        # the city ICS calendar's per-event venue (derived from the ICS LOCATION field, see
        # scrape_google_ics) reports the specific building, not "Reliant Park"/"NRG Park", so
        # without this the ICS skip-filter's alias check never recognizes them as already
        # covered by this venue and inserts them as untracked separate "venues" (confirmed:
        # this is exactly how 6 Texans-game/Karol-G rows ended up live-duplicated under "NRG
        # Stadium"/"NRG Center" before this fix). Both eras listed since the ICS feed's own
        # LOCATION text hasn't caught up to the Reliant rename yet.
        "sub_venues": ["Reliant Center", "Reliant Stadium", "Reliant Arena",
                       "NRG Stadium", "NRG Center", "NRG Arena", "NRG Astrodome"],
    },
    "houcalendar": {
        "name": "Houston Cultural Events Calendar",
        "ical_url": "https://calendar.google.com/calendar/ical/k762aibsv1ictlhfcpbe3hpruq9900im%40import.calendar.google.com/public/basic.ics",
        "city": "Houston",
        "state": "TX",
        "scraper": "google_ics",
    },
    "houston_city_calendar": {
        "name": "City of Houston",
        "ical_url": "https://outlook.office365.com/owa/calendar/378a27e8d0ee477ebd4c9db5ce3a600f@houstontx.gov/9d0692828d624e8bb74ce935644940f417387657915531835411/calendar.ics",
        "city": "Houston",
        "state": "TX",
        "scraper": "google_ics",
        "duplicate_threshold": 0.7,
    },
    "big_easy": {
        "name": "The Big Easy Social and Pleasure Club",
        "url": "https://www.thebigeasyblues.com/the-big-easy-music-calendar/",
        "city": "Houston",
        "state": "TX",
        "venue_instruction": "Extract all live music events. This is a blues and roots music venue.",
    },
    "punchline_htx": {
        "name": "Punch Line Houston",
        "url": "https://www.punchlinehtx.com/shows",
        "city": "Houston",
        "state": "TX",
        "event_type": "comedy",
        "venue_instruction": "Extract all comedy shows. This is a comedy club.",
        "scrolls": 5,
        # Added 2026-08-19: event_url is always the same generic /shows listing page (never
        # per-event) — same class of bug as Miller Outdoor Theatre's ticket_url, and it made
        # detect_url_date_conflicts() flag nearly every event here as a date-drift duplicate
        # of whichever row had the lowest id. ticket_url is the reliable per-event field.
        "unreliable_urls": "event_url",
        # Confirmed 2026-08-19 via the dupe_use_start_date verification procedure (CLAUDE.md):
        # ticket_url is unique per occurrence, no case of a reused link across different shows.
        # Unified matching: link trust per field (key | evidence | ignore | drop).
        "link_trust": {"ticket_url": "key", "event_url": "ignore"},
        "dupe_use_start_date": False,
        # Confirmed 2026-09-23: same headliner often runs an early + late set (or two different
        # headliners) on one night — same false-positive class as Secret Group (ce1dcdf).
        "dupe_check_start_time": True,
        "multi_show": True,
    },
    "shady_acres": {
        "name": "Shady Acres Saloon",
        "url": "https://shadyacressaloon.com/events/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "paginated": True,
        "next_page_selector": "a.frmcal-next",
        "max_pages": 3,
    },
    "mucky_duck": {
        "name": "McGonigel's Mucky Duck",
        "url": "https://www.mcgonigels.com/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 6,
        "scroll_count": 5,
        # Confirmed 2026-09-23: runs a distinct Early Show (~18:00) and Late Show (~20:30) most
        # nights with different artists — the exact false-positive class dupe_check_start_time
        # (ce1dcdf) was built for; already the canonical example in CLAUDE.md's ML notes.
        "dupe_check_start_time": True,
        "multi_show": True,
    },
    "secret_group": {
        "name": "The Secret Group",
        "url": "https://www.thesecretgrouphtx.com/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 12,
        "pagination_sentinel": ".paginationControls",
        # Confirmed 2026-08-17 via the dupe_use_start_date verification procedure (CLAUDE.md):
        # ticket_url is unique per occurrence, every shared-URL case found was the date-drift
        # signature (same name, wrong date), never a reused link across different shows.
        # Unified matching: link trust per field (key | evidence | ignore | drop).
        "link_trust": {"ticket_url": "key"},
        "dupe_use_start_date": False,
        # This venue runs multiple distinct showtimes nightly (Comedy Showcase 7/9/11pm, Joe
        # List 7PM/9PM) with near-identical titles — safe to treat a differing start_time as
        # proof of separate events here, unlike most venues.
        "dupe_check_start_time": True,
        "multi_show": True,
    },
    "hotel_lucine": {
        "name": "Hotel Lucine",
        "url": "https://www.hotellucine.com/events-calendar/list",
        "city": "Galveston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
    },
    "echoes_htx": {
        "name": "Echoes",
        "aliases": ["Echoes HTX"],
        "url": "https://www.echoeshtx.com/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
        "schedule_group": "afternoon",
        "auto_approve": True,
    },
    "scout_bar": {
        "name": "Scout Bar",
        "url": "https://scoutbar.com/calendar/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
        "seetickets": True,
    },
    "armadillo_palace": {
        "name": "Goode Company Armadillo Palace",
        "aliases": ["Armadillo Palace"],
        "url": "https://thearmadillopalace.com/live-music/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
    },
    "moth": {
        "name": "MOTH Presents",
        "aliases": ["Moth", "Moth HTX"],
        "url": "https://www.mothpresents.com/tickets",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
    },
    "numbers": {
        "name": "Numbers",
        "aliases": ["Numbers Nightclub"],
        "url": "https://numbersnightclub.com/events/month/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "paginated": True,
        "next_page_selector": "a.tribe-events-c-nav__next",
        "max_pages": 3,
    },
    "miller_outdoor": {
        "name": "Miller Outdoor Theatre",
        "url": "https://www.milleroutdoortheatre.com/performance-list/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
        # Added 2026-08-16: ticket_url is always the same generic ticketing-portal page
        # (never per-event) — event_url is the real per-show link. Left non-null, the shared
        # value made detect_url_date_conflicts() flag nearly every event on this venue as a
        # date-drift duplicate of whichever row had the lowest id.
        # Unified matching: link trust per field (key | evidence | ignore | drop).
        "link_trust": {"ticket_url": "ignore"},
        "unreliable_urls": "ticket_url",
    },
    "docs_jazz": {
        "name": "Doc's Jazz Club",
        "aliases": ["Doc's"],
        "url": "https://tickets.docshouston.com/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
        # Updated 2026-08-07: the LLM's domain-based ticket/event rule (both links live on
        # tickets.docshouston.com, same domain as this venue's own url) now reliably puts the
        # good numeric /shows/<id>/<date> link in event_url and leaves ticket_url null, so no
        # longer nulling event_url here.
        # /r/<slug> links are series pages shared across dates (the site's own JSON-LD uses them).
        "link_ignore_patterns": [r"/r/"],
        # Many nights run a 7:00 and a 9:30 show (confirmed 2026-11-02 via JSON-LD).
        "dupe_check_start_time": True,
        "multi_show": True,
    },
    "arena_theatre": {
        "name": "Arena Theatre",
        "url": "https://arenahouston.com/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
    },
    "dosey_doe": {
        "name": "Dosey Doe - The Big Barn",
        "aliases": ["Dosey Doe"],
        "url": "https://www.doseydoetickets.com",
        "city": "The Woodlands",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3,
    },
    "den_comedy": {
        "name": "The Den Comedy Club",
        # Inside Wolfsmiths; tickets are sold as Squarespace store items, one per show.
        "url": "https://www.thedencomedyclub.com/tickets",
        "venue_url": "https://www.thedencomedyclub.com/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
    },
    # --- Austin venues (onboarding in progress) ---
    "antones_austin": {
        "name": "Antone's Nightclub",
        "url": "https://antonesnightclub.com/calendar/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "emos_austin": {
        "name": "Emo's Austin",
        "url": "https://emosaustin.com/shows/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "stubbs_austin": {
        "name": "Stubb's Bar-B-Q",
        "url": "https://stubbsaustin.com/concert-calendar/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "acl_live": {
        "name": "ACL Live at The Moody Theater",
        "url": "https://acllive.com/events/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "moody_center": {
        "name": "Moody Center",
        "url": "https://www.moodycenteratx.com/events",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "mohawk_austin": {
        "name": "The Mohawk",
        "url": "https://mohawkaustin.com/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "continental_club_austin": {
        "name": "Continental Club Austin",
        "url": "https://continentalclub.com/austin",
        "city": "Austin",
        "state": "TX",
        "wait_time": 12,
        "scroll_count": 3,
    },
    "cactus_cafe": {
        "name": "Cactus Cafe",
        "url": "https://universityunions.utexas.edu/cactus-cafe",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "the_parish": {
        "name": "The Parish",
        "url": "https://parishaustin.com/calendar/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "empire_control_room": {
        "name": "Empire Control Room & Garage",
        "url": "https://empireatx.com/events/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "hotel_vegas": {
        "name": "Hotel Vegas",
        "url": "https://texashotelvegas.com/calendar/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "come_and_take_it_live": {
        "name": "Come and Take It Live",
        "url": "https://www.comeandtakeitproductions.com/calendar",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "scoot_inn_austin": {
        "name": "Historic Scoot Inn",
        "url": "https://scootinnaustin.com/shows/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "vulcan_gas_company": {
        "name": "Vulcan Gas Company",
        "url": "https://vulcanatx.com/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "white_horse_austin": {
        "name": "White Horse",
        "url": "https://thewhitehorseaustin.com/calendar-1",
        "city": "Austin",
        "state": "TX",
        "wait_time": 12,
        "scroll_count": 3,
    },
    "broken_spoke": {
        "name": "Broken Spoke",
        "url": "https://www.brokenspokeaustintx.net/events-calendar",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "saxon_pub": {
        "name": "Saxon Pub",
        "url": "https://thesaxonpub.com/events/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
        "paginated": True,
        "next_page_selector": "a.tribe-events-c-nav__next",
        "max_pages": 3,
    },
    "cboys": {
        "name": "C-Boy's Heart & Soul",
        "url": "https://www.cboys.com/calendar",
        "city": "Austin",
        "state": "TX",
        "wait_time": 12,
        "scroll_count": 3,
    },
    "cheer_up_charlies": {
        "name": "Cheer Up Charlie's",
        "url": "https://cheerupcharlies.com/events/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "cap_city_comedy": {
        "name": "Cap City Comedy Club",
        "url": "https://www.capcitycomedy.com/calendar",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
        # Confirmed 2026-09-23: multiple distinct headliners/sets on the same night at
        # different times — same false-positive class as Secret Group (ce1dcdf).
        "dupe_check_start_time": True,
        "multi_show": True,
    },
    "velveeta_room": {
        "name": "The Velveeta Room",
        "url": "https://thevelveetaroom.com/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 8,
        "scroll_count": 3,
    },
    "fallout_theater": {
        "name": "Fallout Theater",
        "url": "https://falloutcomedy.com/comedy-shows/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "esthers_follies": {
        "name": "Esther's Follies",
        "url": "https://esthersfollies.com/tickets/",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "ballroom_29th_street": {
        "name": "29th Street Ballroom",
        "aliases": ["Spider House Ballroom"],
        "url": "https://www.29thstreetballroom.com/upcoming",
        "city": "Austin",
        "state": "TX",
        "wait_time": 5,
    },
    "hole_in_the_wall": {
        "name": "Hole in the Wall",
        "url": "https://www.theholeinthewallaustin.com/shows",
        "city": "Austin",
        "state": "TX",
        "wait_time": 12,
        "scroll_count": 3,
    },
}

def get_content_hash(content):
    """Generate SHA256 hash of scraped content"""
    import hashlib
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

def get_stored_hash(venue_key):
    """Get the stored hash for a venue from venue_cache"""
    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'
    c.execute(f'SELECT content_hash FROM venue_cache WHERE venue_key = {ph}', (venue_key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def update_stored_hash(venue_key, content_hash):
    """Update or insert the hash for a venue in venue_cache"""
    conn = get_db_connection()
    c = conn.cursor()
    now = datetime.now().isoformat()
    if DATABASE_URL:
        c.execute('''INSERT INTO venue_cache (venue_key, content_hash, last_scraped)
                     VALUES (%s, %s, %s)
                     ON CONFLICT (venue_key) DO UPDATE
                     SET content_hash = EXCLUDED.content_hash,
                         last_scraped = EXCLUDED.last_scraped''',
                  (venue_key, content_hash, now))
    else:
        c.execute('''INSERT OR REPLACE INTO venue_cache
                     (venue_key, content_hash, last_scraped)
                     VALUES (?, ?, ?)''',
                  (venue_key, content_hash, now))
    conn.commit()
    conn.close()

def strip_opener_clause(name):
    """Strip a trailing opener/support-act clause ('with X', 'featuring X', 'presents X')
    before computing name similarity. Two unrelated shows that each happen to list an
    opener this way (e.g. "Cowboy Mouth with special guest Lvvrs" vs "Deer Tick with
    special guest Presley Haile") can otherwise look deceptively similar on raw string
    comparison and trigger a false-positive duplicate match against the wrong event."""
    return re.sub(r'\s+(with|featuring|feat\.?|presents)\s+.*$', '', name, flags=re.IGNORECASE).strip()

def strip_venue_reference(name, venue_name):
    """Strip a self-reference to the venue's own name from an event title — a leading
    '<Venue> Presents X' or a 'X Headlines/at [The] <Venue>' mention anywhere else in
    the string (not anchored to the end, since a modifier like an opener credit can
    follow it, e.g. "...Single Release Party at Dan Electro's with support from X").

    This is a deterministic backstop for the LLM's TITLE CLEANUP instruction, which has
    proven unreliable specifically for this rule — confirmed on Riot Comedy Club (~70%
    of its titles keep the venue reference intact) and a couple of branded Dan Electro's
    show types. Most venues never embed their own name in a title at all, so this is a
    no-op for them — see VENUES.md's Open Items for the supporting data.
    """
    words = venue_name.split()
    variants = {venue_name}
    if venue_name.lower().startswith('the '):
        variants.add(venue_name[4:])
    if len(words) >= 2:
        variants.add(' '.join(words[:2]))

    def flex(v):
        # Apostrophes are optional so "Dan Electro's" also matches "Dan Electros"
        return re.escape(v).replace("'", "'?")

    variants_pattern = '|'.join(flex(v) for v in sorted(variants, key=len, reverse=True))

    name = re.sub(
        rf"\s*[-–—]?\s*(headlines?|at|hosted by)\s+(the\s+)?(?:{variants_pattern})\b(\s+in\s+\w+)?",
        '', name, flags=re.IGNORECASE
    )
    name = re.sub(
        rf"^(the\s+)?(?:{variants_pattern})\s+presents?\s+",
        '', name, flags=re.IGNORECASE
    )
    return name.strip()

def check_canceled_events(venue_key, scraped_events):
    """Compare scraped events against DB future events, flag missing as canceled.

    A shared ticket_url/event_url counts as still-found regardless of name drift —
    checked first since it's a stronger identity signal than fuzzy name matching.
    Falls back to fuzzy name matching (to tolerate minor title drift) when a DB
    event has no URL, or its URL isn't present in today's scrape.

    Three layers of protection against a bad scrape masquerading as real
    cancellations (a broken pagination, an incomplete page render, or a flaky API
    response all look identical to "the venue canceled everything" from the
    outside — count alone can't tell them apart, so this doesn't try to guess the
    cause and instead limits the damage any single run can do):

    1. An empty scrape is skipped entirely rather than treated as "0 events found,
       cancel everything" — a real venue essentially never drops to zero upcoming
       shows between runs.
    2. Missing events are only ever *tentatively* flagged (cancel_flagged_at set,
       status untouched) on their first missing observation. Only a SECOND
       consecutive miss confirms the cancellation. A one-off scrape glitch that
       misses an event only sets/leaves a flag; it takes the same event going
       missing on two different runs in a row to actually cancel it.
    3. Even tentative flagging is capped per run: if the number of newly-missing
       events is large relative to the venue's active schedule, the whole run is
       treated as unreliable and skipped (logged, not applied) — this catches a
       deterministic partial failure (e.g. pagination that reliably stops after
       page 1) that would otherwise "confirm" the same wrong cancellations twice.

    Events that reappear (a flagged-but-not-yet-canceled event found again, or an
    already-canceled event found again via a strong URL match) are reactivated —
    self-healing rather than requiring a manual undo.
    """
    venue_name = VENUES[venue_key]['name']

    if not scraped_events:
        # Almost always a fetch failure (network hiccup, API error, empty page
        # load) rather than a venue genuinely having zero upcoming shows.
        print(f"  ↷ Skipping cancellation check for {venue_name} — scrape returned 0 events")
        LAST_CANCEL_CHECK[venue_key] = {'skipped': 'empty'}
        return 0

    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'
    today = datetime.now().strftime('%Y-%m-%d')
    now_iso = datetime.now().isoformat()
    c.execute(f'''SELECT id, name, start_date, ticket_url, event_url, status, cancel_flagged_at
                 FROM events
                 WHERE venue = {ph} AND start_date >= {ph}
                 AND status IN ('approved', 'pending', 'canceled')''',
              (venue_name, today))
    db_events = c.fetchall()

    # Build scraped lookups for efficient comparison
    scraped_by_date = {}
    scraped_ticket_urls = set()
    scraped_event_urls = set()
    for e in scraped_events:
        date = e.get('start_date')
        if date and e.get('name'):
            scraped_by_date.setdefault(date, []).append(e['name'].strip().lower())
        if e.get('ticket_url'):
            scraped_ticket_urls.add(e['ticket_url'])
        if e.get('event_url'):
            scraped_event_urls.add(e['event_url'])

    FUZZY_THRESHOLD = 0.85

    def fuzzy_match(db_name, scraped_names):
        db_lower = db_name.strip().lower()
        for s in scraped_names:
            if db_lower == s:
                return True
            # Handle suffix additions like "(Sold Out)", "(New Date)"
            if db_lower in s or s in db_lower:
                return True
            if SequenceMatcher(None, strip_opener_clause(db_lower), strip_opener_clause(s)).ratio() >= FUZZY_THRESHOLD:
                return True
        return False

    def url_match(db_ticket_url, db_event_url):
        # Cross-field: the LLM doesn't reliably pick the same field for the same link
        # between scrapes.
        if db_ticket_url and (db_ticket_url in scraped_ticket_urls or db_ticket_url in scraped_event_urls):
            return True
        if db_event_url and (db_event_url in scraped_ticket_urls or db_event_url in scraped_event_urls):
            return True
        return False

    active = [row for row in db_events if row[5] != 'canceled']
    already_canceled = [row for row in db_events if row[5] == 'canceled']

    reappeared = []   # active rows found again -> clear any tentative flag
    missing = []      # active rows not found -> candidates for flag/confirm
    for db_id, db_name, db_date, db_ticket_url, db_event_url, status, flagged_at in active:
        found = url_match(db_ticket_url, db_event_url) or fuzzy_match(db_name, scraped_by_date.get(db_date, []))
        if found:
            if flagged_at:
                reappeared.append((db_id, db_name))
        else:
            missing.append((db_id, db_name, db_date, flagged_at))

    # Layer 3: cap how much of the active schedule can be newly flagged/confirmed
    # in one run. Applies only to the destructive side (flagging, confirming) —
    # clearing flags and reactivating are always safe to apply regardless.
    CAP_RATIO = 0.3
    MIN_CAP = 3
    cap = max(MIN_CAP, int(len(active) * CAP_RATIO))
    over_cap = len(missing) > cap

    for db_id, db_name in reappeared:
        c.execute(f"UPDATE events SET cancel_flagged_at = NULL WHERE id = {ph}", (db_id,))
        print(f"  ↺ No longer missing, clearing flag: {db_name}")

    # Reactivate already-canceled events found again. Same match rule (URL or
    # fuzzy name) as everything above — the fuzzy match that would have kept an
    # event from being canceled in the first place is equally trustworthy for
    # reversing it, so there's no reason to hold reactivation to a stricter bar.
    reactivated_count = 0
    for db_id, db_name, db_date, db_ticket_url, db_event_url, status, flagged_at in already_canceled:
        if url_match(db_ticket_url, db_event_url) or fuzzy_match(db_name, scraped_by_date.get(db_date, [])):
            c.execute(f"UPDATE events SET status = 'pending', cancel_flagged_at = NULL WHERE id = {ph}", (db_id,))
            reactivated_count += 1
            print(f"  ↺ Reappeared, reactivated as pending: {db_name} on {db_date}")

    LAST_CANCEL_CHECK[venue_key] = {'missing': [m[0] for m in missing], 'over_cap': over_cap}
    canceled_count = 0
    if over_cap:
        print(f"  ⚠ {len(missing)} events missing at once for {venue_name} "
              f"(cap {cap} of {len(active)} active) — treating as an unreliable "
              f"scrape and skipping cancellation for this run:")
        for _, db_name, db_date, _ in missing:
            print(f"      - {db_name} on {db_date}")
    else:
        for db_id, db_name, db_date, flagged_at in missing:
            if flagged_at:
                c.execute(f"UPDATE events SET status = 'canceled', cancel_flagged_at = NULL WHERE id = {ph}", (db_id,))
                canceled_count += 1
                print(f"  ⚠ Confirmed canceled (missing 2 scrapes in a row): {db_name} on {db_date}")
            else:
                c.execute(f"UPDATE events SET cancel_flagged_at = {ph} WHERE id = {ph}", (now_iso, db_id))
                print(f"  ⚑ Flagged as cancellation candidate (will confirm next scrape): {db_name} on {db_date}")

    conn.commit()
    conn.close()
    print(f"  {canceled_count} events confirmed canceled, {reactivated_count} reactivated for {venue_name}")
    return canceled_count

def scrape_page(url, wait_time=3, debug=False, scroll_count=1, load_more_id=None, pagination_sentinel=None):
    """Scrape a page using Selenium"""
    print(f"Fetching {url}...")
    
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')

    _log_process_counts()
    # Own process group, so every Chrome process this page started can be killed afterwards.
    service = ChromeService(popen_kw={'start_new_session': True})
    driver = webdriver.Chrome(options=chrome_options, service=service)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    try:
        try:
            driver.get(url)
        except TimeoutException:
            # Keep whatever loaded; a hung page otherwise blocks for Chrome's 300s default.
            print(f"  Page load passed {PAGE_LOAD_TIMEOUT}s, using what loaded so far")
            driver.execute_script("window.stop();")
        time.sleep(wait_time)
        
        for i in range(scroll_count):
            if pagination_sentinel:
                # Some infinite-scroll widgets watch a specific sentinel element via
                # IntersectionObserver — jumping to document.body.scrollHeight overshoots
                # it (trailing footer content pushes it above the viewport before the
                # observer ever sees it enter view), so scroll the sentinel itself into view.
                driver.execute_script(
                    "var e=document.querySelector(arguments[0]); if(e) e.scrollIntoView({block:'center'});",
                    pagination_sentinel
                )
            else:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            print(f"  Scroll {i+1}/{scroll_count}")
        
        if load_more_id :
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            while True:
                try:
                    btn = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.ID, load_more_id))
                    )
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(2)
                except:
                    break

        html = driver.page_source
        
        if debug:
            filename = url.split('//')[-1].replace('/', '_') + '.html'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"Saved debug HTML to {filename}")
        
        soup = BeautifulSoup(html, 'html.parser')
        # Strip site chrome (nav menus, header, footer) before extracting text — this
        # is boilerplate that repeats verbatim across every page of a paginated venue
        # (see Numbers onboarding, 2026-07-23), diluting the real event content with
        # noise and, in one confirmed case, causing the LLM to under-extract events
        # from later pages once combined. Doesn't affect the raw `html` return value
        # used by custom parsers (parse_white_oak_html, parse_seetickets_html, etc.)
        # or pagination's next-page-link detection — those re-parse the untouched HTML.
        for tag in soup(['script', 'style', 'header', 'nav', 'footer']):
            tag.decompose()

        # Inline each link's URL into its own anchor text (not a separate list at the
        # end) so the LLM sees it next to the right event instead of having to count
        # positions across two lists.
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            if not href or href.startswith('#') or href.startswith('mailto:'):
                continue
            href = urljoin(url, href)
            label = a.get_text(strip=True)
            a.string = f"{label} [{href}]" if label else f"[{href}]"

        page_text = soup.get_text(separator='\n', strip=True)

        print(f"Extracted {len(page_text)} characters")
        print(f"HTML length: {len(html)} characters")  # Show raw HTML size

        return page_text, html  # Return both

    finally:
        _shutdown_driver(driver, service)


PAGE_LOAD_TIMEOUT = 60


def _shutdown_driver(driver, service):
    """Quit Chrome, kill anything left in its process group, and reap dead children.
    In the cron container Python is PID 1, so orphaned Chrome processes are never reaped
    otherwise and pile up until new sessions can't start."""
    import signal
    pid = service.process.pid if getattr(service, 'process', None) else None
    try:
        driver.quit()
    except Exception as e:
        print(f"  driver.quit() failed: {type(e).__name__}")
    if pid:
        try:
            os.killpg(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    _reap_children()


def _reap_children():
    while True:
        try:
            child, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if child == 0:
            return


def _log_process_counts():
    """Linux only: total and zombie process counts, to watch for leaks across a run."""
    if not os.path.isdir('/proc/self'):
        return
    total = zombies = 0
    for p in os.listdir('/proc'):
        if not p.isdigit():
            continue
        try:
            with open(f'/proc/{p}/stat') as f:
                state = f.read().rsplit(')', 1)[1].split()[0]
        except OSError:
            continue
        total += 1
        zombies += state == 'Z'
    print(f"  [procs] {total} processes, {zombies} zombies")

def classify_event_types(events, llm='gpt4o-mini'):
    """Batch classify event_type for events that don't have one set. Updates dicts in place."""
    unclassified = [e for e in events if not e.get('event_type')]
    if not unclassified:
        return
    items = [{'i': i, 'name': e['name']} for i, e in enumerate(unclassified)]
    system_prompt = """Classify each event into exactly one of these types:
music - concerts, live music, bands, DJs, music festivals
comedy - stand-up comedy, improv, comedy nights
open_mic - open mic nights
happy_hour - happy hour specials
performing_arts - theater, opera, ballet, orchestra, symphony, dance performances
arts - gallery openings, art exhibitions, museum events, visual arts shows
sports - games, matches, sporting events
civic - government meetings, council sessions, committee meetings, public hearings, city business
private_event - private parties, closed or invite-only events
other - anything that doesn't fit above
Return JSON: {"results": [{"i": 0, "type": "music"}, ...]}"""
    try:
        result = get_llm_response(system_prompt, json.dumps(items), llm)
        for item in result.get('results', []):
            idx = item.get('i')
            if isinstance(idx, int) and 0 <= idx < len(unclassified):
                unclassified[idx]['event_type'] = item.get('type', 'other')
    except Exception as e:
        print(f"  ⚠ Event type classification failed: {e}")
    for event in events:
        if not event.get('event_type'):
            event['event_type'] = 'other'


def timely_price(item):
    """Timely's cost_display is a display-mode code ('0', '3'), not a price; use the real cost fields."""
    if item.get('cost'):
        return str(item['cost'])
    if item.get('tickets_min_price'):
        return f"{item.get('tickets_currency_symbol') or '$'}{item['tickets_min_price']}"
    return None


def scrape_timely_api(venue_config, mode='daily'):
    """Fetch events from Timely API for venues using Timely calendar"""
    import time as time_module
    
    calendar_id = venue_config['timely_calendar_id']
    city = venue_config.get('city', '')
    state = venue_config.get('state', '')
    
    start_date_utc = int(time_module.time())
    events = []
    page = 1
    
    while True:
        url = (f"https://events.timely.fun/api/calendars/{calendar_id}/events"
               f"?timezone=America/Chicago&view=posterboard"
               f"&start_date_utc={start_date_utc}&per_page=100&page={page}")
        
        try:
            print(f"  Fetching: {url}")
            response = requests.get(url, timeout=10, headers={
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0',
                'Referer': 'https://events.timely.fun/uggqcowo/posterboard',
                'X-Api-Key': 'c6e5e0363b5925b28552de8805464c66f25ba0ce'
            })
            data = response.json()
            items = data.get('data', {}).get('items', [])
            
            for item in items:
                venue_name = item.get('taxonomies', {}).get('taxonomy_venue', [{}])[0].get('title', venue_config['name'])
                start_dt = item.get('start_datetime', '')

                title_lower = item.get('title', '').lower()
                is_private = any(phrase in title_lower for phrase in ['private party', 'closed for private', 'private event'])
                event_type = 'private_event' if is_private else None

                # Timely's cost_external_url is sometimes a Square ad-hoc checkout-session
                # link (checkout.square.site/merchant/.../checkout/<id>), which is ephemeral
                # and can 404 within days — unlike the permanent *.square.site/product/... page.
                # Since events sit in the DB for weeks before their date, don't trust the
                # ephemeral shape at all; fall back to the stable Timely event_url instead.
                cost_external_url = item.get('cost_external_url')
                ticket_url = cost_external_url if cost_external_url and 'checkout.square.site/merchant' not in cost_external_url else None

                event = {
                    'name': item.get('title', ''),
                    'start_date': start_dt[:10] if start_dt else None,
                    'end_date': None,
                    'multi_day': False,
                    'start_time': start_dt[11:16] if start_dt else None,
                    'end_time': None,
                    'doors_time': None,
                    'venue': venue_name,
                    'venue_url': venue_config.get('venue_url') or venue_config.get('url', ''),
                    'location': None,
                    'city': city,
                    'state': state,
                    'price': timely_price(item),
                    'ticket_url': ticket_url,
                    'event_url': item.get('url'),
                    'description': item.get('description_short', ''),
                    'genre': None,
                    'event_type': event_type,
                    'visible': False,
                    'sold_out': False,
                    'date_changed': False,
                    'openers': None,
                    'confidence': {}
                }

                events.append(event)

            if not data.get('data', {}).get('has_next'):
                break
            page += 1
            
        except Exception as e:
            print(f"  ✗ Timely API error (page {page}): {e}")
            break
    
    classify_event_types(events)
    for event in events:
        event['visible'] = event.get('event_type') in ('music', 'comedy', 'performing_arts', 'arts', 'sports')
    print(f"  ✓ Fetched {len(events)} events from Timely API")
    return events

def parse_seetickets_html(html, venue_config):
    """Parse SeeTickets calendar widget HTML"""
    from bs4 import BeautifulSoup
    import re
    from datetime import datetime
    
    soup = BeautifulSoup(html, 'html.parser')
    city = venue_config.get('city', '')
    state = venue_config.get('state', '')
    venue_name = venue_config['name']
    today = datetime.now()
    events = []
    seen = set()  # deduplicate since HTML has repeated elements
    
    containers = soup.find_all('div', class_='seetickets-calendar-event-container')
    
    with open('/tmp/warehouse.html', 'w') as f:
        f.write(html)
    print(f"  Debug: {len(containers)} containers found in {len(html)} chars of HTML")

    for container in containers:
        # Get ticket URL and date from aria-label
        buy_btn = container.find('a', class_='button-gettickets')
        if not buy_btn:
            continue
        ticket_url = buy_btn.get('href')
        aria_label = buy_btn.get('aria-label', '')

        sold_out = 'button-soldout' in buy_btn.get('class', [])
        
        # Extract date from "Buy Tickets for X on Month Day"
        date_match = re.search(r'on (\w+ \d+)$', aria_label)
        if not date_match:
            continue
        date_str = date_match.group(1)  # e.g. "May 31" or "Jun 04"

        for year in [today.year, today.year + 1]:
            try:
                event_date = datetime.strptime(f"{date_str} {year}", "%b %d %Y")
                if event_date.date() >= today.date():
                    break
            except ValueError:
                try:
                    event_date = datetime.strptime(f"{date_str} {year}", "%B %d %Y")
                    if event_date.date() >= today.date():
                        break
                except ValueError:
                    continue
        
        # Deduplicate by ticket URL
        if ticket_url in seen:
            continue
        seen.add(ticket_url)
        
        # Name
        title_el = container.find('p', class_='bold')
        name = title_el.get_text(strip=True) if title_el else None
        if not name:
            continue
        
        # Openers
        openers_el = container.find('p', class_='supporting-talent')
        openers = openers_el.get_text(strip=True).lstrip('with ') if openers_el else None
        
        # Times
        times_div = container.find('div', class_='seetickets-calendar-event-date')
        start_time = None
        doors_time = None
        if times_div:
            time_els = times_div.find_all('p')
            for p in time_els:
                text = p.get_text(strip=True)
                if 'Show:' in text:
                    t = text.replace('Show:', '').strip()
                    try:
                        start_time = datetime.strptime(t, '%I:%M%p').strftime('%H:%M')
                    except:
                        pass
                elif 'Doors:' in text:
                    t = text.replace('Doors:', '').strip()
                    try:
                        doors_time = datetime.strptime(t, '%I:%M%p').strftime('%H:%M')
                    except:
                        pass
        
        events.append({
            'name': name,
            'start_date': event_date.strftime('%Y-%m-%d'),
            'end_date': None,
            'multi_day': False,
            'start_time': start_time,
            'end_time': None,
            'doors_time': doors_time,
            'venue': venue_name,
            'venue_url': venue_config.get('venue_url') or venue_config.get('url', ''),
            'location': None,
            'city': city,
            'state': state,
            'price': None,
            'ticket_url': ticket_url,
            'event_url': None,
            'description': None,
            'genre': None,
            'event_type': 'music',
            'visible': True,
            'sold_out': sold_out,
            'date_changed': False,
            'openers': openers,
            'confidence': {}
        })
    
    print(f"  ✓ Parsed {len(events)} events from SeeTickets HTML")
    return events

def ajax_scrape(venue_config, mode='daily'):
    """Scrape venues that use AJAX pagination returning HTML fragments"""
    import requests
    
    ajax_url = venue_config['ajax_url']
    params = venue_config.get('ajax_params', {})
    increment = venue_config.get('ajax_increment', 12)
    offset = 0
    all_html = ""
    headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'text/html, */*; q=0.01',
        'Referer': 'https://www.toyotacenter.com/events',
    }
    
    try:
        while True:
            url = ajax_url.format(offset=offset)
            r = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=15
            )
            print(f"  Debug: offset={offset}, response length={len(r.text)}, status={r.status_code}")
            if not r.text.strip():
                break
            all_html += r.text
            offset += increment
    except Exception as e:
        print(f"  ✗ AJAX fetch error: {e}")
        return []
    
    print(f"  ✓ Fetched {offset} events worth of HTML via AJAX")
    events_data = extract_events_with_llm_raw(
        all_html,
        venue_name=venue_config['name'],
        venue_url=venue_config.get('venue_url') or venue_config.get('url', ''),
        is_html=True,
        venue_instruction=venue_config.get('venue_instruction')
    )
    events = events_data.get('events', [])
    for event in events:
        event['venue_url'] = venue_config.get('venue_url') or venue_config.get('url', '')
        event['venue'] = venue_config['name']
        event['city'] = venue_config['city']
        event['state'] = venue_config['state']
        if event.get('name'):
            event['name'] = strip_venue_reference(event['name'], venue_config['name'])
    return events

def scrape_google_ics(venue_config, mode='daily'):
    """Fetch events from a public Google Calendar ICS feed"""
    from icalendar import Calendar
    import recurring_ical_events
    from datetime import datetime, date
    import pytz

    ical_url = venue_config['ical_url']
    city = venue_config.get('city', '')
    state = venue_config.get('state', '')
    venue_name = venue_config['name']
    tz = pytz.timezone('America/Chicago')
    today = datetime.now(tz).date()
    days_out = venue_config.get('days_out', 365)
    end_date = today + timedelta(days=days_out)

    try:
        response = requests.get(ical_url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        cal = Calendar.from_ical(response.content)
        all_events = recurring_ical_events.of(cal).between(today, end_date)
    except Exception as e:
        print(f"  ✗ ICS fetch error: {e}")
        return []

    events = []

    for component in all_events:
        summary = str(component.get('SUMMARY', ''))
        summary_lower = summary.lower()

        # Skip unwanted events
        if any(phrase in summary_lower for phrase in [
            'food truck', 'food:'
        ]):
            continue

        # Get start datetime
        dtstart = component.get('DTSTART').dt

        if isinstance(dtstart, date) and not isinstance(dtstart, datetime):
            event_date = dtstart
            start_time = None
        else:
            if dtstart.tzinfo is None:
                dtstart = pytz.utc.localize(dtstart)
            dtstart = dtstart.astimezone(tz)
            event_date = dtstart.date()
            start_time = dtstart.strftime('%H:%M')
            if start_time == '00:00':
                start_time = None

        # Pre-filter private events; all others classified in batch after loop
        is_private = any(p in summary_lower for p in ['private party', 'closed for private', 'private event'])
        event_type = 'private_event' if is_private else None

        # Clean up summary prefix
        name = summary
        for prefix in ['MUSIC: ', 'Music: ', 'MUSIC:', 'Music:']:
            if name.startswith(prefix):
                name = name[len(prefix):]
                break

        description = str(component.get('DESCRIPTION', '')) or None

        location = str(component.get('LOCATION', '')).strip()
        single_venue = venue_config.get('ics_single_venue')
        # A single-venue feed's LOCATION is just its own address, not a different venue.
        venue = venue_name if single_venue or not location else location.split(',')[0].strip()

        # Skip events from venues we already scrape directly — ICS data is lower quality
        # and would create duplicates. Compare against all non-ICS venue names and known aliases
        # (fuzzy matching alone misses nicknames like "Woodlands Pavilion" for "Cynthia Woods
        # Mitchell Pavilion" that don't share enough characters to clear the ratio threshold).
        # sub_venues covers the opposite case from aliases: distinct buildings within a complex
        # (e.g. Reliant Stadium within Reliant Park) that the ICS feed's LOCATION field reports
        # by their own specific name — low string similarity to the complex's own name, so they
        # need to be listed explicitly rather than caught by the fuzzy ratio check.
        #
        # Checked against the full LOCATION text too, not just its first comma-separated
        # segment (venue_lower) — some calendar entries put the venue name in a different
        # position (e.g. a street address first, venue name second), which venue_lower alone
        # would miss entirely even though the venue name is right there in the location string.
        venue_lower = venue.lower()
        location_lower = location.lower()
        scraped_names = []
        for v in VENUES.values():
            if em.is_multi_venue_feed(v) or v is venue_config:
                continue
            scraped_names.append(v['name'].lower())
            scraped_names.extend(a.lower() for a in v.get('aliases', []))
            scraped_names.extend(s.lower() for s in v.get('sub_venues', []))
        if any(
            venue_lower in sn or sn in venue_lower or
            SequenceMatcher(None, venue_lower, sn).ratio() >= 0.8 or
            (location_lower and sn in location_lower)
            for sn in scraped_names
        ):
            print(f"  ↷ Skipping (already scraped venue): {venue} — {name.strip()}")
            continue

        event = {
            'name': name.strip(),
            'start_date': event_date.strftime('%Y-%m-%d'),
            'end_date': None,
            'multi_day': False,
            'start_time': start_time,
            'end_time': None,
            'doors_time': None,
            'venue': venue,
            'location': None,
            'city': city,
            'state': state,
            'price': None,
            'event_url': str(component.get('URL', '')) or None,
            'ticket_url': None,
            'description': description,
            'genre': None,
            'event_type': event_type,
            'visible': False,
            'sold_out': False,
            'date_changed': False,
            'openers': None,
            'confidence': {}
        }
        events.append(event)

    classify_event_types(events)
    for event in events:
        event['visible'] = event.get('event_type') in ('music', 'comedy', 'performing_arts', 'arts', 'sports')
        # Never publicly visible either way, so there's no public-facing decision for a
        # human to make — auto-approve to keep formulaic civic/generic listings (e.g. City
        # Hall committee meetings) off the review queue instead of piling up unreviewed.
        event['auto_approve'] = not event['visible']
    print(f"  ✓ Fetched {len(events)} events from Google Calendar ICS")
    return events

def get_llm_response(system_prompt, user_prompt, llm='gpt4o-mini'):
    """Route LLM request to the appropriate provider"""
    if llm == 'gpt4o':
        client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
        response = client.chat.completions.create(
            model="gpt-4o",
            temperature=0,
            max_tokens=16000,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        return json.loads(response.choices[0].message.content)

    elif llm == 'gpt4o-mini':
        client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            max_tokens=16000,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        return json.loads(response.choices[0].message.content)

    elif llm == 'claude':
        client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY'))
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            temperature=0,
            system=system_prompt + "\nReturn ONLY valid JSON with an 'events' array, no other text.",
            messages=[{"role": "user", "content": user_prompt}]
        )
        text = response.content[0].text.strip()
        text = text.replace('```json', '').replace('```', '').strip()
        return json.loads(text)

    elif llm == 'groq':
        from groq import Groq
        client = Groq(api_key=os.environ.get('GROQ_API_KEY'))
        response = client.chat.completions.create(
            model="llama-3.1-70b-versatile",
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        return json.loads(response.choices[0].message.content)

    else:
        raise ValueError(f"Unknown LLM provider: {llm}")

def extract_events_with_llm(page_text, venue_name, city, state, llm='gpt4o-mini'):
    """Extract events using LLM"""
    if "White Oak" in venue_name:
        char_limit = 100000
    else:
        char_limit = 100000
    
    system_prompt = f"""You are an expert at extracting structured event data from venue websites. Today's date is {datetime.now().strftime('%Y-%m-%d')}.
Extract ALL events from the provided text into a JSON array. For each event:
- name: Full event name/title
- start_date: YYYY-MM-DD format (use 2026 for dates without year)
- end_date: YYYY-MM-DD format (use 2026 for dates without year, for date ranges or multiple dates for a single event, else null)
- doors_time: HH:MM format (24-hour) or null (look for times like "6:30pm", "7:00pm", "Doors: 7pm", etc.)
- start_time: HH:MM format (24-hour) (24-hour, Central Time - DO NOT CONVERT TIMEZONES) or null 
  IMPORTANT: If you see "8:00 PM" or "8pm", output "20:00" NOT "02:00"
  If you see "7:30 PM", output "19:30" NOT "01:30"
  DO NOT apply any timezone conversion. Keep times exactly as shown on the website.
  Multi-night events: If you see a date range instead of a single date (e.g. "Apr 17-18 Fri-Sat", "April 17 - 18" or "Apr 17/18") assume that each date in the range has a unique event with the same details posted for name, venue, time, etc.
- date_count: Number of consecutive days this event runs. 1 for single-night events, 2 for two nights, etc. For example 'March 29-31' = 3, 'Jun4-6' = 2, 'May 15' = 1.
- venue: "{venue_name}"
- city: "{city}"
- state: "{state}"
- price: Extract if mentioned, otherwise null
- ticket_url: Full URL if present
- description: Brief description if available
- genre: Music genre/category if discernible
- location: Specific room, stage, or area within the venue if mentioned (e.g. 'Main Stage', 'Upstairs', 'Lawn'), otherwise null
- event_type: Classify as one of: 'music', 'comedy', 'open_mic', 'happy_hour', 'performing_arts', 'arts', 'sports', 'civic', 'private_event', or 'other'. Use 'music' as default if unclear.
- confidence: Object with field-level confidence scores (0-1)
IMPORTANT: Look carefully for event times. They may appear as:
- "6:30pm" or "7:00pm"
- "Doors: 7pm, Show: 8pm"
- Times listed separately from event names
- Pay special attention to any numbers followed by "pm" or "am"
Return ONLY valid JSON with an "events" array."""

    user_prompt = f"Extract events from this page:\n\n{page_text[:char_limit]}"
    return get_llm_response(system_prompt, user_prompt, llm=llm)

# Characters to shift the LLM content split by, cycling back/none/forward by day of year.
SPLIT_SHIFT_CHARS = 1500
# Splits per venue name in this run, recorded in scrape_stats.
SPLIT_COUNTS = {}

def extract_events_with_llm_raw(content, venue_name, venue_url, is_html=False, venue_instruction=None, llm='gpt4o-mini', _depth=0):
    """Extract events using LLM from text or HTML.

    venue/city/state are deliberately NOT part of the extraction schema — we already
    know them from config with certainty, so asking the LLM to echo them back only
    wastes output tokens and risks it drifting on the exact string (dangerous, since
    save_to_database does exact-string venue matching everywhere). They're set in code
    after extraction instead, the same way venue_url already is.

    Splits and retries on JSON truncation (a high event count can exceed the model's
    output token cap) rather than failing the whole venue outright. _depth guards
    against infinite recursion if a chunk keeps failing for an unrelated reason.
    """
    venue_note = f"\n\nVENUE NOTE: {venue_instruction}" if venue_instruction else ""
    # Trimmed 2026-11-03: the longer prompt (title-cleanup rules, date_changed, per-field
    # confidence) turned one misread date into a cascade at Arena Theatre.
    system_prompt = f"""You are an expert at extracting structured event data from venue websites. The name of this venue is "{venue_name}", and it's website is "{venue_url}". Today's date is {datetime.now().strftime('%Y-%m-%d')}. Don't apply any timezone conversion.

Fields to extract:
- name: Event name which may include music headliners, opening acts, etc. 
- start_date: YYYY-MM-DD format
- end_date: YYYY-MM-DD format 
- doors_time: HH:MM format (24-hour) or null
- start_time: HH:MM format (24-hour) or null
- end_time: HH:MM format (24-hour) or null
- price: Extract if mentioned, otherwise null
- ticket_url: if external ticketing url link is present, otherwise null
- event_url: if present, otherwise null
- description: Brief description
- genre: Music genre/category if discernible
- location: Specific room, stage, or area within the venue if mentioned (e.g. 'Main Stage', 'Upstairs', 'Lawn'), otherwise null
- event_type: Classify as one of: 'music', 'comedy', 'open_mic', 'happy_hour', 'performing_arts', 'arts', 'sports', 'civic', 'private_event', or 'other'. Use 'music' as default if unclear.
- sold_out: true if event is sold out, false otherwise
- openers: Comma-separated list of opening acts if mentioned, otherwise null
{venue_note}
{"If parsing HTML, look in div classes, data attributes, and any structured elements containing event information." if is_html else ""}
Return ONLY valid JSON with an "events" array containing ALL events found."""
    user_prompt = f"Extract all events:\n\n{content}"
    try:
        return get_llm_response(system_prompt, user_prompt, llm=llm)
    except json.JSONDecodeError:
        if _depth >= 3 or len(content) < 4000:
            raise
        print(f"  ⚠ LLM response truncated ({len(content)} chars sent) — splitting content and retrying")
        SPLIT_COUNTS[venue_name] = SPLIT_COUNTS.get(venue_name, 0) + 1
        # Shift the cut by day of year so an event straddling it is read whole on another run.
        offset = (datetime.now().timetuple().tm_yday % 3 - 1) * SPLIT_SHIFT_CHARS
        target = min(max(len(content) // 2 + offset, 1), len(content) - 1)
        split_at = content.rfind('\n', 0, target)
        if split_at <= 0:
            split_at = target
        result_a = extract_events_with_llm_raw(content[:split_at], venue_name, venue_url, is_html, venue_instruction, llm, _depth + 1)
        result_b = extract_events_with_llm_raw(content[split_at:], venue_name, venue_url, is_html, venue_instruction, llm, _depth + 1)
        # The event on each side of the cut may be a partial read, so drop both; the shifted
        # cut on a later run captures them whole.
        events_a = result_a.get('events', [])[:-1]
        events_b = result_b.get('events', [])[1:]
        return {'events': events_a + events_b}

def parse_white_oak_html(html):
    """Parse White Oak events directly from HTML"""
    from bs4 import BeautifulSoup
    import re
    
    soup = BeautifulSoup(html, 'html.parser')
    events = []
    
    # Find all event sections
    event_sections = soup.find_all('div', class_='tw-section')
    
    for section in event_sections:
        try:
            # Extract event name
            name_elem = section.find('div', class_='tw-name')
            if not name_elem or not name_elem.find('a'):
                continue
            name = name_elem.find('a').text.strip()
            
            # Extract date
            date_elem = section.find('span', class_='tw-event-date')
            month_elem = section.find('span', class_='tw-event-month')
            
            if not date_elem or not month_elem:
                continue
                
            day = date_elem.text.strip()
            month = month_elem.text.strip()
            
            # Convert month name to number
            month_map = {
                'January': '01', 'February': '02', 'March': '03', 'April': '04',
                'May': '05', 'June': '06', 'July': '07', 'August': '08',
                'September': '09', 'October': '10', 'November': '11', 'December': '12'
            }
            month_num = month_map.get(month, '01')
            
            # Extract location (Upstairs/Downstairs/Lawn)
            venue_elem = section.find('span', class_='tw-venue-name')
            venue_detail = venue_elem.text.strip() if venue_elem else None
            # Strip venue name prefix if present
            if venue_detail and ' - ' in venue_detail:
                location = venue_detail.split(' - ', 1)[1].strip()
            elif venue_detail and venue_detail.startswith('White Oak Music Hall '):
                location = venue_detail.replace('White Oak Music Hall ', '').strip()
            else:
                location = None
            
            # Extract ticket URL and year if available
            ticket_link = section.find('a', class_='tw-buy-tix-btn')
            # Also check image link for year
            image_link = section.find('a', attrs={'aria-label': True})
            ticket_url = ticket_link['href'] if ticket_link else None

            # Try to extract year from any link href
            year = str(datetime.now().year)
            for link in section.find_all('a', href=True):
                year_match = re.search(r'[-/](20\d{2})(?:[-/]|$)', link['href'])
                if year_match:
                    year = year_match.group(1)
                    break

            date = f"{year}-{month_num}-{day.zfill(2)}"
            
            # The show time sits in a link label or title ("... | 09 November 6:30 PM"). Since
            # about 2026-11-04 it's no longer on the ticket button, so check every link in the event.
            time_match = None
            for link in ([ticket_link] if ticket_link else []) + section.find_all('a'):
                for attr in ('aria-label', 'title'):
                    time_match = re.search(r'(\d{1,2}):(\d{2})\s*(AM|PM)', link.get(attr) or '')
                    if time_match:
                        break
                if time_match:
                    break
            
            start_time = None
            if time_match:
                hour = int(time_match.group(1))
                minute = time_match.group(2)
                ampm = time_match.group(3)
                
                if ampm == 'PM' and hour != 12:
                    hour += 12
                elif ampm == 'AM' and hour == 12:
                    hour = 0
                    
                start_time = f"{hour:02d}:{minute}"
            
            events.append({
                'name': name,
                'start_date': date,
                'end_date': None,
                'multi_day': False,
                'start_time': start_time,
                'end_time': None,
                'doors_time': None,
                'venue': 'White Oak Music Hall',
                'location': location,
                'city': 'Houston',
                'state': 'TX',
                'price': None,
                'ticket_url': ticket_url,
                'description': None,
                'genre': None,
                'confidence': {
                    'name': 1.0,
                    'start_date': 1.0,
                    'time': 0.8 if start_time else 0.0,
                    'price': 0.0,
                    'genre': 0.0
                }
            })
            
        except Exception as e:
            print(f"Error parsing event: {e}")
            continue

    return {'events': events}

def parse_mucky_duck_html(html):
    """Parse McGonigel's Mucky Duck events directly from HTML (Tessera plugin cards).

    Card dates omit the year entirely, so it's inferred by walking cards in document
    order (which is chronological) and bumping the year whenever data-month rolls
    backward — more reliable than asking an LLM to reason about the Dec->Jan wrap.
    """
    from bs4 import BeautifulSoup
    import re

    soup = BeautifulSoup(html, 'html.parser')
    events = []

    cards = soup.find_all('div', class_='tessera-show-card')

    current_year = datetime.now().year
    last_month = 0

    # Tags that describe show format/logistics rather than genre — keep these out
    # of the genre field so they don't pollute the dashboard's genre filter dropdown.
    non_genre_tag_pattern = re.compile(r'\d|\$|\b(am|pm|ticket|dinner|sing)\b', re.IGNORECASE)

    for card in cards:
        try:
            title_elem = card.find('h4', class_='card-title')
            if not title_elem:
                continue
            name = title_elem.get_text(strip=True)

            date_span = card.find('span', class_='date')
            month_num = card.get('data-month')
            if not date_span or not month_num:
                continue
            month_num = int(month_num)
            day_match = re.search(r'(\d{1,2})\s*$', date_span.get_text(strip=True))
            if not day_match:
                continue
            day = int(day_match.group(1))

            if month_num < last_month:
                current_year += 1
            last_month = month_num
            start_date = f"{current_year}-{month_num:02d}-{day:02d}"

            # First am/pm time mentioned is the door/showtime, matching how these
            # dinner-then-show listings are meant to be read (e.g. "6pm Dinner-8pm Sing").
            start_time = None
            times_elem = card.find('div', class_='tessera-showTimes')
            if times_elem:
                time_text = times_elem.get_text(strip=True)
                time_match = re.search(r'(\d{1,2})(?::(\d{2}))?\s*(am|pm)', time_text, re.IGNORECASE)
                if time_match:
                    hour = int(time_match.group(1))
                    minute = time_match.group(2) or '00'
                    ampm = time_match.group(3).lower()
                else:
                    # No am/pm suffix (e.g. "Showtime 8:30") — every show here is an
                    # evening slot, so a bare hour defaults to PM.
                    bare_match = re.search(r'(\d{1,2}):(\d{2})', time_text)
                    hour = int(bare_match.group(1)) if bare_match else None
                    minute = bare_match.group(2) if bare_match else None
                    ampm = 'pm'
                if hour is not None:
                    if ampm == 'pm' and hour != 12:
                        hour += 12
                    elif ampm == 'am' and hour == 12:
                        hour = 0
                    start_time = f"{hour:02d}:{minute}"

            link = card.find('a', href=True)
            event_url = link['href'] if link else None

            genre = None
            tags = card.get('data-tags', '').strip()
            if tags and not non_genre_tag_pattern.search(tags):
                genre = tags

            events.append({
                'name': name,
                'start_date': start_date,
                'end_date': None,
                'multi_day': False,
                'start_time': start_time,
                'end_time': None,
                'doors_time': None,
                'venue': "McGonigel's Mucky Duck",
                'location': None,
                'city': 'Houston',
                'state': 'TX',
                'price': None,
                'ticket_url': None,
                'event_url': event_url,
                'description': None,
                'genre': genre,
                'confidence': {
                    'name': 1.0,
                    'start_date': 1.0,
                    'time': 0.8 if start_time else 0.0,
                    'price': 0.0,
                    'genre': 0.8 if genre else 0.0
                }
            })

        except Exception as e:
            print(f"Error parsing event: {e}")
            continue

    return {'events': events}

def get_toyota_carousel_tickets(html):
    """Map event name -> real ticket_url from Toyota Center's "on sale now" carousel.

    The carousel is a separate widget from the main events grid, with its own clean
    name+ticket-link pairing per item — unlike the main grid, which has no ticket link at
    all for most concerts. The LLM extracting the whole flattened page text has to notice a
    name mentioned once in the main grid is the same event as a (correctly-linked) mention
    later in the carousel, across dozens of other events — confirmed unreliable in practice
    (e.g. MANÁ came back with Don Omar's real link, John Summit and Leanne Morgan both came
    back with Teddy Swims'). Parsing the carousel directly sidesteps that merge entirely.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    tickets = {}
    for item in soup.select('.owl-item .m-venueframework-eventslist__item'):
        title_el = item.select_one('.m-eventItem__title')
        ticket_el = item.select_one('a.tickets[href]')
        if title_el and ticket_el:
            tickets[title_el.get_text(strip=True).lower()] = ticket_el['href']
    return tickets

def pre_filter_dates(venue_name, page_text):
    """Extract and log new dates found on page vs DB - informational only"""
    import re
    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'
    today = datetime.now().strftime('%Y-%m-%d')
    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
        'january': '01', 'february': '02', 'march': '03', 'april': '04',
        'june': '06', 'july': '07', 'august': '08', 'september': '09',
        'october': '10', 'november': '11', 'december': '12'
    }
    found_dates = set()
    for match in re.finditer(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec\w*)\s+(\d{1,2})',
                             page_text.lower()):
        month_str = match.group(1)[:3]
        day = match.group(2).zfill(2)
        month_num = month_map.get(month_str)
        if month_num:
            event_date = f"2026-{month_num}-{day}"
            if event_date < today:
                event_date = f"2027-{month_num}-{day}"
            found_dates.add(event_date)
    if not found_dates:
        conn.close()
        return
    c.execute(f'''SELECT DISTINCT start_date FROM events
                 WHERE venue = {ph} AND start_date >= {ph}
                 AND status NOT IN ('rejected', 'canceled')''',
              (venue_name, today))
    db_dates = set(row[0] for row in c.fetchall())
    conn.close()
    new_dates = found_dates - db_dates
    if new_dates:
        print(f"  ℹ {len(new_dates)} potentially new date(s) detected")

def _legacy_cancel(venue_key, events, dry_run):
    """Legacy cancellation, unless unified matching owns the venue (it cancels at save time)."""
    if dry_run or (MATCH_ENGINE == 'unified' and not is_city_calendar(venue_key)):
        return 0
    return check_canceled_events(venue_key, events)


def scrape_venue(venue_key, mode='daily', llm='gpt4o-mini', dry_run=False):
    """Scrape a single venue with hash checking and mode support"""
    venue = VENUES[venue_key]

    print(f"\n{'='*60}")
    print(f"Scraping: {venue['name']} [{mode} mode]")
    print(f"{'='*60}")

    # These four branches bypass the LLM/hash-check path below entirely, and each
    # re-fetches live data on every single run regardless of mode (no hash-skip to
    # gate them). check_canceled_events is cheap (a DB comparison, no LLM call), so
    # unlike the hash-checked path below there's no reason to gate it on weekly mode
    # here — run it every time so a canceled/replaced show doesn't sit undetected
    # until someone notices.

    # Timely API venues — bypass Selenium + LLM entirely
    if venue.get('timely_calendar_id'):
        events = scrape_timely_api(venue, mode)
        return events, _legacy_cancel(venue_key, events, dry_run)

    # SeeTickets venues — scrape with Selenium but parse with custom HTML parser, no LLM
    if venue.get('seetickets'):
        _, html = scrape_page(venue['url'], wait_time=venue.get('wait_time', 3),
                            scroll_count=venue.get('scroll_count', 1),load_more_id=venue.get('load_more_id'))
        events = parse_seetickets_html(html, venue)
        return events, _legacy_cancel(venue_key, events, dry_run)

    # AJAX venues — fetch all paginated HTML fragments and parse with LLM in one go
    if venue.get('ajax_url'):
        events = ajax_scrape(venue, mode)
        return events, _legacy_cancel(venue_key, events, dry_run)

    # Google Calendar ICS feed — bypass Selenium + LLM entirely
    if venue.get('ical_url'):
        events = scrape_google_ics(venue, mode)
        return events, _legacy_cancel(venue_key, events, dry_run)

    debug = venue_key in ['white_oak']
    scroll_count = venue.get('scroll_count', 1)

    # Handle paginated venues
    if venue.get('paginated'):
        combined_text = ''
        max_pages = venue.get('max_pages', 10)  # Safety limit, applies per start URL
        next_page_selector = venue.get('next_page_selector', '#moreshowsbtn')
        # 'urls' (plural) covers sites that partition listings server-side into separate
        # sections with no combined view — e.g. Smart Financial Centre's ATG listing pages
        # are filtered by genre in the URL path itself (concert/comedy/dance/etc.), each
        # with its own pagination, and there's no "all genres" URL to fetch instead. Falls
        # back to the single 'url' for venues (like Improv) that just paginate one listing.
        start_urls = venue.get('urls') or [venue['url']]
        total_pages_fetched = 0

        for start_url in start_urls:
            current_url = start_url
            page_num = 1
            while current_url and page_num <= max_pages:
                print(f"  Fetching page {page_num}: {current_url}")
                page_text, html = scrape_page(
                    current_url,
                    wait_time=venue.get('wait_time', 3),
                    debug=debug,
                    scroll_count=scroll_count,
                    load_more_id=venue.get('load_more_id')
                )
                combined_text += '\n' + page_text
                total_pages_fetched += 1

                # Look for next page link
                soup = BeautifulSoup(html, 'html.parser')
                next_link = soup.select_one(next_page_selector)
                if next_link and next_link.get('href'):
                    current_url = urljoin(current_url, next_link['href'])
                    page_num += 1
                else:
                    break

        page_text = combined_text
        html = ''  # Not needed after pagination
        print(f"  ✓ Fetched {total_pages_fetched} pages across {len(start_urls)} start URL(s), {len(page_text)} total characters")

    else:
        page_text, html = scrape_page(
            venue['url'],
            wait_time=venue.get('wait_time', 3),
            debug=debug,
            scroll_count=scroll_count,
            load_more_id=venue.get('load_more_id'),
            pagination_sentinel=venue.get('pagination_sentinel')
        )

    # Hash check — skip LLM if content unchanged (not for onboard mode)
    content_hash = get_content_hash(page_text)
    stored_hash = get_stored_hash(venue_key)
    if mode != 'onboard' and content_hash == stored_hash:
        print(f"  ↷ No changes detected, skipping LLM")
        return [], 0
    print(f"  ✓ Content changed, processing with LLM")

    print(f"  ✓ Content changed, processing with LLM")

    # Log new dates detected (informational)
    pre_filter_dates(venue['name'], page_text)

    #Print html preview in dry run mode for debugging
    if dry_run:
        print(f"\n--- PAGE TEXT PREVIEW ---")
        print(page_text[:15000])
        print(f"--- END PREVIEW ---\n")

    # Use custom parsers for venues with reliable per-event HTML structure
    if venue_key == 'white_oak':
        events_data = parse_white_oak_html(html)
    elif venue_key == 'mucky_duck':
        events_data = parse_mucky_duck_html(html)
    else:
        char_limit = 100000
        venue_instruction = venue.get('venue_instruction', '')
        events_data = extract_events_with_llm_raw(
            page_text[:char_limit],
            venue_name=venue['name'],
            venue_url=venue.get('venue_url') or venue.get('url', ''),
            is_html=False,
            venue_instruction=venue_instruction,
            llm=llm
        )

    events = events_data.get('events', [])
    print(f"  ✓ Found {len(events)} events")

    # Toyota Center's ticket_url can't be trusted from the LLM extraction at all (see
    # get_toyota_carousel_tickets) — rebuilt here directly from the carousel instead, once
    # per scrape rather than once per event.
    toyota_tickets = get_toyota_carousel_tickets(html) if venue_key == 'toyota_center' else None

    _site = urlparse(venue.get('url', ''))
    generic_links = {u.rstrip('/') for u in (venue.get('url'), venue.get('venue_url'),
                                             f"{_site.scheme}://{_site.netloc}" if _site.netloc else None) if u}
    for event in events:
        extracted = {f: event.get(f) for f in ('ticket_url', 'event_url')}
        event['venue_url'] = venue.get('venue_url') or venue.get('url', '')
        event['venue'] = venue['name']
        event['city'] = venue['city']
        event['state'] = venue['state']
        if event.get('name'):
            event['name'] = strip_venue_reference(event['name'], venue['name'])
        # Some venues' per-event links can't be trusted — force the untrustworthy one(s)
        # to null so the calendar's fallback chain (ticket_url -> event_url -> venue_url)
        # lands on the venue's generic events page instead of a wrong specific one.
        # True nulls both, for venues where every event's link carries no reliable
        # distinguishing signal at all (identical anchor text AND a shared/generic href).
        # A field name ('ticket_url' or 'event_url') nulls only that one, for venues where
        # just one field is the problem (e.g. Doc's Jazz Club's event_url — a generic
        # vanity link sometimes misattributed to a neighboring event, sometimes a shared
        # non-specific landing page — while ticket_url there is fine and worth keeping).
        unreliable = venue.get('unreliable_urls')
        if unreliable is True:
            event['ticket_url'] = None
            event['event_url'] = None
        elif unreliable in ('ticket_url', 'event_url'):
            event[unreliable] = None
        # Toyota Center: NBA games have their own reliable, correctly-paired ticket link
        # directly in the main grid (tix.axs.com, confirmed unique per game) — only concerts
        # without one of those have to be cross-checked against the carousel, so this only
        # touches events whose extracted ticket_url already has the specific confirmed-
        # unreliable shape (a shop.axs.com short link, which is what a concert gets when the
        # LLM borrows the wrong carousel entry). Replaced with the carousel's verified value
        # if this event is in it, otherwise nulled — the LLM's own guess for those turned out
        # to be another event's real link, not just missing, so leaving it in place would be
        # worse than nulling it. Matched by containment, not equality — the carousel's own
        # tagline (e.g. "Vivir Sin Aire Tour") only exists in the carousel's markup, not the
        # main grid, so the LLM's final name ("MANÁ Vivir Sin Aire Tour") won't equal the
        # bare carousel title ("MANÁ") even though it's unambiguously the same event.
        if toyota_tickets is not None and event.get('ticket_url') and 'shop.axs.com' in event['ticket_url']:
            event_name_lower = event.get('name', '').strip().lower()
            event['ticket_url'] = next(
                (href for title, href in toyota_tickets.items() if title in event_name_lower),
                None
            )
        # Controls for save_to_database's URL-identity matching. Defaults preserve today's
        # behavior everywhere except event_url's date requirement (see dupe_use_start_date
        # below) — only flip a venue's dupe_use_start_date to False after confirming its
        # ticket_url/event_url is genuinely unique per occurrence (e.g. Heights Theater's
        # numeric Prekindle event ID). Getting this wrong the unsafe direction silently merges
        # distinct shows that share a reused link (a comic's profile page, a recurring weekly
        # show's template page — confirmed real cases at Improv Houston and Dan Electro's
        # respectively).
        event['dupe_use_ticket_url'] = venue.get('dupe_use_ticket_url', True)
        event['dupe_use_event_url'] = venue.get('dupe_use_event_url', True)
        # True (date required alongside the URL) is the safe default for BOTH fields. event_url
        # used to default to False here under the assumption an event_url is always per-occurrence
        # — Dan Electro's recurring weekly shows (Karaoke Mondays, etc.) reuse one templated
        # event_url across different real dates, disproving that assumption, so the safe default
        # is now uniform across both fields.
        event['dupe_use_start_date'] = venue.get('dupe_use_start_date', True)
        # Safe default False: only for venues confirmed to run multiple distinct shows with
        # similar titles on the same night (e.g. The Secret Group's 7pm/9pm/11pm showcases) —
        # for those, a differing start_time is treated as proof of separate events even when
        # names are near-identical. Wrong for most venues, where one show per night is the
        # norm and a real duplicate could coincidentally have a slightly different time typo.
        event['dupe_check_start_time'] = venue.get('dupe_check_start_time', False)
        # General per-venue URL cleanup: a regex pattern matched against the END of
        # ticket_url/event_url and stripped out. Added for Improv Houston, whose event-page
        # links embed a trailing numeric segment that looks like a per-event ID but isn't —
        # diffing the HTML of the same show under two different numbers turned up only the
        # ID itself changing (title tag, one meta ref), confirmed via a bogus ID still
        # resolving to the same page. That noise made every re-scrape look like a new URL,
        # corrupting both save_to_database's URL-match and check_canceled_events' url_match.
        # Regex (vs. a fixed char count or "strip back to the last /") because it only fires
        # on the shape it's meant for — e.g. Improv's r'\d+/$' leaves comic profile pages
        # (/houston/comic/<name>/, no trailing digits) untouched — and generalizes to
        # whatever the next venue's noise turns out to be (a tracking query string, an
        # affiliate suffix) by supplying a different pattern, no new code.
        event['strip_end_url'] = venue.get('strip_end_url')
        if event['strip_end_url']:
            if event.get('ticket_url'):
                event['ticket_url'] = re.sub(event['strip_end_url'], '', event['ticket_url'])
            if event.get('event_url'):
                event['event_url'] = re.sub(event['strip_end_url'], '', event['event_url'])
        # A link to the scraped page or the venue's own site carries nothing about this event;
        # the calendar falls back to venue_url anyway.
        for field in ('ticket_url', 'event_url'):
            if event.get(field) and event[field].rstrip('/') in generic_links:
                event[field] = None
        # Links as extracted, for unified matching: data corrections applied, legacy nulling not.
        # Toyota's carousel check is a correction, so its result stands even when it nulls.
        for field, value in extracted.items():
            if event.get(field) is None and value and not (venue_key == 'toyota_center' and field == 'ticket_url'):
                extracted[field] = re.sub(event['strip_end_url'], '', value) if event['strip_end_url'] else value
            else:
                extracted[field] = event.get(field)
        event['extracted_links'] = extracted

    # A ticket_url/event_url shared by 2+ differently-named events in this same scrape is a
    # strong signal of a generic/shared page (a seasonal campaign link, a venue-wide landing
    # page) rather than a genuine per-event identifier — null it for just those events, not
    # venue-wide, so a venue whose links are normally fine doesn't lose good data over one
    # promotional page. Keyed on name so a legitimately recurring show (same name, same link,
    # many real dates — see the `recurring` column) isn't caught by this. Confirmed real case:
    # Doc's Jazz Club's "christmas-at-docs-2026" campaign page landing in ticket_url for 19
    # different shows — the same root cause already fixed venue-wide for Miller Outdoor/Punch
    # Line/Heights Theater, but those venues' shared link was on EVERY event, not just a
    # seasonal subset, so a blanket unreliable_urls would be too broad here.
    for field in ('ticket_url', 'event_url'):
        names_by_url = defaultdict(set)
        for event in events:
            if event.get(field):
                names_by_url[event[field]].add(event.get('name', ''))
        for event in events:
            if event.get(field) and len(names_by_url[event[field]]) > 1:
                event[field] = None

    # A hash change means the page's content moved for one of three reasons: a new
    # event, a changed event, or a canceled event — so whenever we've actually
    # processed changed content (any mode, not just weekly), check for all three.
    # The hash-check above already skipped this entirely if nothing changed.
    # A dry run must not flag or cancel anything.
    canceled_count = _legacy_cancel(venue_key, events, dry_run)

    # Update stored hash
    if not dry_run:
        update_stored_hash(venue_key, content_hash)

    return events, canceled_count

def scrape_all_venues(mode='daily', llm='gpt4o-mini', auto_approve=False, group=None, dry_run=False):
    """Scrape all venues, save, and log stats per venue.

    group filters down to venues tagged with a matching schedule_group (e.g.
    "afternoon") — lets one Railway cron cover many venues on a shared schedule
    without a separate cron service per venue. Adding/moving a venue between
    schedules is then just a config edit, not new infra.
    """
    venue_keys = VENUES.keys()
    if group:
        venue_keys = [k for k, v in VENUES.items() if v.get('schedule_group') == group]
        if not venue_keys:
            groups = sorted({v['schedule_group'] for v in VENUES.values() if v.get('schedule_group')})
            print(f"✗ No venues tagged with schedule_group '{group}'")
            print(f"  Available groups: {', '.join(groups) if groups else '(none configured)'}")
            return []

    all_events = []
    for venue_key in venue_keys:
        try:
            events, canceled_count = scrape_venue(venue_key, mode=mode, llm=llm, dry_run=dry_run)
            for e in events or []:
                e.setdefault('venue_key', venue_key)
            all_events.extend(events)
            if events is not None and not dry_run:
                stats = save_to_database(events, mode=mode, auto_approve=auto_approve)
                log_scrape_stats(VENUES[venue_key]['name'], mode, stats, canceled_count + (stats or {}).get('canceled', 0))
        except Exception as e:
            print(f"✗ Error scraping {VENUES[venue_key]['name']}: {e}")
            if not dry_run:
                try:
                    log_scrape_stats(VENUES[venue_key]['name'], mode, {}, 0, error=f"{type(e).__name__}: {e}")
                except Exception as log_err:
                    print(f"  ⚠ Could not record the failure: {log_err}")
    return all_events

# Longest date range expanded into nightly rows; longer ones are kept whole and flagged.
MAX_EXPANDED_NIGHTS = 7

# Bracketed title tags the LLM no longer strips: (Sold Out), [Rescheduled], (New Date), ...
SOLD_OUT_TAG = re.compile(r'\s*[\(\[]\s*sold\s*-?\s*out\s*[\)\]]\s*', re.I)
DATE_CHANGED_TAG = re.compile(r'\s*[\(\[]\s*(new\s+date|date\s+changed|rescheduled|postponed)\s*[\)\]]\s*', re.I)


def clean_title_tags(events):
    """Strip sold-out and date-change tags from titles and set the matching flags."""
    for event in events:
        name = event.get('name') or ''
        if SOLD_OUT_TAG.search(name):
            event['sold_out'] = True
            name = SOLD_OUT_TAG.sub(' ', name)
        if DATE_CHANGED_TAG.search(name):
            event['date_changed'] = True
            name = DATE_CHANGED_TAG.sub(' ', name)
        event['name'] = re.sub(r'\s{2,}', ' ', name).strip(' -–|')
    return events


def expand_multi_night_events(events):
    """Expand a date-range event into one row per night, unless the venue is
    configured to allow genuine multi-day continuous events (allows_multi_day).

    The LLM's own multi_day flag has proven unreliable exactly where it matters
    most: venues where a date range means separate nightly bookings (a comedian's
    multi-night comedy-club run), not one continuous event like a festival or expo.
    So this doesn't trust that flag — it goes by venue config instead.
    """
    venue_allows_multi_day = {v['name']: v.get('allows_multi_day', False) for v in VENUES.values()}
    expanded = []
    for event in events:
        start = event.get('start_date')
        end = event.get('end_date')
        if start and end and str(end)[:10] != start and not venue_allows_multi_day.get(event.get('venue'), False):
            try:
                start_d = datetime.strptime(start, '%Y-%m-%d').date()
                end_d = datetime.strptime(str(end)[:10], '%Y-%m-%d').date()
            except ValueError:
                expanded.append(event)
                continue
            # A run longer than a week is more likely a misread range than nightly shows
            # (Cactus Cafe 2026-08-01, Arena 2026-11-03): keep one row and flag it for review.
            if (end_d - start_d).days + 1 > MAX_EXPANDED_NIGHTS:
                event['multi_day'] = False
                event.setdefault('flags', []).append('long_range')
                expanded.append(event)
                continue
            day = start_d
            while day <= end_d:
                night_event = dict(event)
                night_event['start_date'] = day.strftime('%Y-%m-%d')
                night_event['end_date'] = None
                night_event['multi_day'] = False
                expanded.append(night_event)
                day += timedelta(days=1)
        else:
            # multi_day comes from venue config, not the LLM: a range kept unexpanded is one continuous event.
            event['multi_day'] = bool(start and end and str(end)[:10] != start)
            expanded.append(event)
    return expanded

# ---------- unified matching, shadow mode (see event_matching.py) ----------

# Legacy cancellation result per venue_key, captured so shadow mode can compare against it.
LAST_CANCEL_CHECK = {}

MATCH_ROW_COLS = ('id', 'name', 'start_date', 'start_time', 'doors_time', 'location', 'ticket_url',
                  'event_url', 'review_status', 'event_status', 'recurring', 'duplicate_of_id', 'venue', 'source',
                  'cancel_flagged_at')


def _load_match_rows(c, venues, since, keys=()):
    """Candidate rows by venue name. Rows a venue's own scraper wrote (source = its key) are
    also filed under its current name, so a display-name rename doesn't orphan them."""
    if not venues:
        return {}
    ph = '%s'
    keys = [k for k in keys if k]
    key_clause = f" OR source IN ({', '.join([ph] * len(keys))})" if keys else ''
    c.execute(f'''SELECT {', '.join(MATCH_ROW_COLS)} FROM events
                  WHERE (venue IN ({', '.join([ph] * len(venues))}){key_clause}) AND start_date >= {ph}''',
              list(venues) + keys + [since])
    rows_by_venue = {}
    for r in c.fetchall():
        row = dict(zip(MATCH_ROW_COLS, r))
        venue = row['venue']
        if venue not in venues and row['source'] in VENUES:
            venue = VENUES[row['source']]['name']
        rows_by_venue.setdefault(venue, []).append(row)
    return rows_by_venue


def shadow_decide(c, events):
    """Run the unified scorer over a save batch against the pre-batch DB state. Never raises."""
    try:
        since = (datetime.now().date() - timedelta(days=em.HISTORY_DAYS + 1)).isoformat()
        rows_by_venue = _load_match_rows(c, {e.get('venue') for e in events if e.get('venue')}, since,
                                         {e.get('venue_key') for e in events})
        decisions, claimed, best_against, anomalies = em.decide_batch(events, rows_by_venue, VENUES)
        for venue, found in anomalies.items():
            print(f"  ⚠ Shadow matching anomalies at {venue}: {', '.join(found)}")
        return {'decisions': decisions, 'claimed': claimed, 'best_against': best_against,
                'rows_by_venue': rows_by_venue, 'anomalies': anomalies}
    except Exception as e:
        print(f"  ⚠ Shadow matching failed (legacy path unaffected): {e}")
        return None


def _ensure_match_log(c):
    c.execute('''CREATE TABLE IF NOT EXISTS match_log
                 (id SERIAL PRIMARY KEY, logged_at TEXT, mode TEXT, kind TEXT, venue TEXT, venue_key TEXT,
                  event_name TEXT, start_date TEXT, start_time TEXT,
                  shadow_decision TEXT, shadow_target TEXT, shadow_score INTEGER, parts TEXT,
                  runner_up_id TEXT, runner_up_score INTEGER, reason TEXT,
                  legacy_outcome TEXT, legacy_target INTEGER, legacy_new_id INTEGER,
                  agree BOOLEAN, detail TEXT)''')


def shadow_log(c, events, shadow, legacy_outcomes, mode):
    """Log shadow vs legacy decisions for one batch, plus a cancellation comparison per
    directly scraped venue. Never raises."""
    if not shadow or not events:
        return
    try:
        _ensure_match_log(c)
        now = datetime.now().isoformat()
        postponement_targets = set()
        for d, ev, lo in zip(shadow['decisions'], events, legacy_outcomes):
            agree = em.agrees(d, lo) if lo else None
            lo = lo or {'outcome': 'unified' if unified_owns(ev) else 'unknown'}
            if d['decision'] == 'review' and d.get('reason') == 'possible_postponement':
                postponement_targets.add(d['target_id'])
            c.execute('''INSERT INTO match_log (logged_at, mode, kind, venue, venue_key, event_name, start_date,
                         start_time, shadow_decision, shadow_target, shadow_score, parts, runner_up_id,
                         runner_up_score, reason, legacy_outcome, legacy_target, legacy_new_id, agree, detail)
                         VALUES (%s,%s,'save',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                      (now, mode, ev.get('venue'), ev.get('venue_key'), ev.get('name'), ev.get('start_date'),
                       ev.get('start_time'), d['decision'],
                       None if d['target_id'] is None else str(d['target_id']), d['score'],
                       json.dumps(d['parts']) if d['parts'] else None,
                       None if d['runner_up_id'] is None else str(d['runner_up_id']), d['runner_up_score'],
                       d.get('reason'), lo.get('outcome'), lo.get('target_id'), lo.get('new_id'),
                       agree,
                       json.dumps({k: v for k, v in (('recurring', d.get('recurring')),
                                                     ('anomalies', shadow['anomalies'].get(ev.get('venue'))))
                                   if v}) or None))

        today = datetime.now().date().isoformat()
        for venue_key in {e.get('venue_key') for e in events if e.get('venue_key')}:
            legacy = LAST_CANCEL_CHECK.get(venue_key)
            if legacy is None or legacy.get('skipped') or em.is_multi_venue_feed(VENUES.get(venue_key)):
                continue
            venue = VENUES.get(venue_key, {}).get('name')
            legacy_missing = set(legacy.get('missing', []))
            active = [r for r in shadow['rows_by_venue'].get(venue, [])
                      if r['start_date'] >= today and r['review_status'] in ('pending', 'approved')
                      and (r['event_status'] != 'canceled' or r['id'] in legacy_missing)]
            shadow_missing = set(em.missing_rows(active, shadow['claimed'], shadow['best_against'],
                                                 postponement_targets))
            c.execute('''INSERT INTO match_log (logged_at, mode, kind, venue, venue_key, agree, detail)
                         VALUES (%s,%s,'cancel',%s,%s,%s,%s)''',
                      (now, mode, venue, venue_key, shadow_missing == legacy_missing,
                       json.dumps({'shadow_missing': sorted(shadow_missing),
                                   'legacy_missing': sorted(legacy_missing),
                                   'legacy_over_cap': legacy.get('over_cap', False)})))
    except Exception as e:
        print(f"  ⚠ Shadow logging failed (legacy path unaffected): {e}")
        c.connection.rollback()


def match_report(days=7):
    """Agreement between the unified scorer (shadow) and the legacy path. Read-only."""
    conn = get_db_connection()
    c = conn.cursor()
    since = (datetime.now() - timedelta(days=days)).isoformat()
    c.execute('''SELECT shadow_decision, legacy_outcome, agree, COUNT(*) FROM match_log
                 WHERE kind = 'save' AND logged_at >= %s AND shadow_decision != 'excluded'
                 GROUP BY 1,2,3 ORDER BY 4 DESC''', (since,))
    rows = c.fetchall()
    total = sum(r[3] for r in rows)
    agreed = sum(r[3] for r in rows if r[2])
    print(f"\n=== Match report, last {days} days: {agreed}/{total} decisions agree ===")
    for sd, lo, ag, n in rows:
        print(f"  {'✓' if ag else '✗'} shadow={sd:16} legacy={lo:18} {n}")
    c.execute('''SELECT venue, event_name, start_date, start_time, shadow_decision, shadow_target, shadow_score,
                        parts, reason, legacy_outcome, legacy_target
                 FROM match_log WHERE kind = 'save' AND logged_at >= %s AND NOT agree
                 ORDER BY venue, start_date''', (since,))
    print("\n--- Disagreements ---")
    for r in c.fetchall():
        print(f"  [{r[0]}] {r[2]} {r[3] or ''} {r[1]}")
        print(f"      shadow {r[4]} → {r[5]} score={r[6]} {r[8] or ''} {r[7] or ''}")
        print(f"      legacy {r[9]} → {r[10]}")
    c.execute('''SELECT venue, agree, detail FROM match_log
                 WHERE kind = 'cancel' AND logged_at >= %s ORDER BY logged_at''', (since,))
    cancel_rows = c.fetchall()
    print(f"\n--- Cancellation: {sum(1 for r in cancel_rows if r[1])}/{len(cancel_rows)} venue runs agree ---")
    for venue, ag, detail in cancel_rows:
        if not ag:
            d = json.loads(detail)
            print(f"  [{venue}] shadow missing {d['shadow_missing']} vs legacy missing {d['legacy_missing']}")
    conn.close()


def check_arena_dates():
    """Read-only: compare each active future Arena row's date with the date the venue's own
    link carries (/event/MMDDYY-). Verification only; nothing is corrected."""
    conn = get_db_connection()
    c = conn.cursor()
    today = datetime.now().date().isoformat()
    c.execute("""SELECT id, name, start_date, status, created_at, ticket_url, event_url FROM events
                 WHERE venue = %s AND start_date >= %s AND status IN ('pending', 'approved', 'possible_duplicate')
                 ORDER BY start_date""", (VENUES['arena_theatre']['name'], today))
    rows = c.fetchall()
    conn.close()
    wrong, no_link = [], 0
    for rid, name, sd, status, created, tu, eu in rows:
        m = re.search(r'/event/(\d{2})(\d{2})(\d{2})-', eu or tu or '')
        if not m:
            no_link += 1
            continue
        link_date = f"20{m.group(3)}-{m.group(1)}-{m.group(2)}"
        if link_date != sd:
            wrong.append((rid, status, sd, link_date, (created or '')[:10], name))
    print(f"\n=== Arena date check: {len(wrong)} of {len(rows)} active rows disagree with their link "
          f"({no_link} have no link) ===")
    for rid, status, sd, link_date, created, name in wrong:
        print(f"  #{rid} [{status}] stored {sd}, link says {link_date}, created {created}: {name}")
    return wrong


def leak_report(write=False):
    """Same-venue, same-date active pairs scoring 55+ with no duplicate link between them;
    each is a duplicate the save path let through. City calendar venues are skipped.
    write=True records each directly scraped venue's count in scrape_stats for venue health."""
    conn = get_db_connection()
    c = conn.cursor()
    today = datetime.now().date().isoformat()
    c.execute(f'''SELECT {', '.join(MATCH_ROW_COLS)} FROM events
                  WHERE start_date >= %s AND review_status IN ('pending', 'approved')
                  AND event_status != 'canceled' ''', (today,))
    rows = [dict(zip(MATCH_ROW_COLS, r)) for r in c.fetchall()]
    conn.close()
    key_by_name = {v['name']: k for k, v in VENUES.items()}
    groups = {}
    for r in rows:
        groups.setdefault((r['venue'], r['start_date']), []).append(r)
    per_venue = {}
    leaks = []
    for (venue, _), group in groups.items():
        if len(group) < 2 or venue not in key_by_name or em.is_multi_venue_feed(VENUES[key_by_name[venue]]):
            continue
        profile = em.venue_profile(VENUES.get(key_by_name.get(venue)))
        ctx = {'profile': profile, 'batch_ignored': set(), 'pool': group}
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                if a['duplicate_of_id'] == b['id'] or b['duplicate_of_id'] == a['id']:
                    continue
                total, parts = em.score(a, b, ctx)
                if total >= em.REVIEW_AT:
                    per_venue[venue] = per_venue.get(venue, 0) + 1
                    leaks.append((total, venue, a, b, parts))
    print(f"\n=== Leak report: {len(leaks)} unlinked active pairs scoring {em.REVIEW_AT}+ ===")
    for venue, n in sorted(per_venue.items(), key=lambda x: -x[1]):
        print(f"  {n:4}  {venue}")
    for total, venue, a, b, parts in sorted(leaks, key=lambda x: -x[0])[:40]:
        print(f"  {total:4} [{venue}] {a['start_date']} #{a['id']} '{a['name']}' ~ #{b['id']} '{b['name']}' {parts}")
    if write:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("ALTER TABLE scrape_stats ADD COLUMN IF NOT EXISTS leak_pairs INTEGER")
        for key, v in VENUES.items():
            if em.is_multi_venue_feed(v):
                continue
            c.execute('''INSERT INTO scrape_stats (scrape_date, venue, scrape_mode, total_scraped, new_events, leak_pairs)
                         VALUES (%s, %s, 'leak_monitor', 0, 0, %s)''', (today, v['name'], per_venue.get(v['name'], 0)))
        conn.commit()
        conn.close()
    return per_venue


# ---------- unified matching, live writes (MATCH_ENGINE=unified) ----------

# unified (default): scorer decisions are written. shadow: logged only, legacy writes.
# legacy: scorer off. Multi-venue city calendar feeds always use the legacy path.
MATCH_ENGINE = os.environ.get('MATCH_ENGINE', 'unified').strip().lower()

VISIBLE_TYPES = ('music', 'comedy', 'performing_arts', 'arts', 'sports')
UPDATE_TEXT_FIELDS = ('name', 'start_date', 'end_date', 'doors_time', 'start_time', 'end_time', 'price',
                      'ticket_url', 'event_url', 'description', 'genre', 'location', 'openers')
LINK_FIELD_NAMES = ('ticket_url', 'event_url')


def is_city_calendar(venue_key):
    """Multi-venue aggregator feeds: excluded from unified matching (CLAUDE.md to-do)."""
    cfg = VENUES.get(venue_key) or {}
    return bool(cfg.get('ical_url')) and not cfg.get('ics_single_venue')


def unified_owns(event):
    return MATCH_ENGINE == 'unified' and event.get('venue_key') and not is_city_calendar(event.get('venue_key'))


def _row_values(c, ph, row_id, fields):
    c.execute(f"SELECT {', '.join(fields)} FROM events WHERE id = {ph}", (row_id,))
    r = c.fetchone()
    return dict(zip(fields, r)) if r else None


def _apply_update(c, ph, event, row_id, stats):
    """Copy the listing's non-empty details onto an existing row. A canceled row comes back
    as scheduled and pending review; a tentative one is cleared."""
    fields = UPDATE_TEXT_FIELDS + ('sold_out', 'date_changed', 'event_status', 'cancel_flagged_at')
    row = _row_values(c, ph, row_id, fields)
    if row is None:
        return
    changes = {}
    for f in UPDATE_TEXT_FIELDS:
        v = event.get(f)
        if v not in (None, '') and str(v) != str(row[f] or ''):
            changes[f] = v
    if 'sold_out' in event and bool(event['sold_out']) != bool(row['sold_out']):
        changes['sold_out'] = bool(event['sold_out'])
    if event.get('date_changed') and not row['date_changed']:
        changes['date_changed'] = True
    if row['event_status'] == 'canceled':
        changes.update(event_status='scheduled', review_status='pending')
    if row['cancel_flagged_at']:
        changes['cancel_flagged_at'] = None
    if not changes:
        return
    c.execute(f"UPDATE events SET {', '.join(f'{k} = {ph}' for k in changes)} WHERE id = {ph}",
              list(changes.values()) + [row_id])
    stats['updated'] += 1
    if 'name' in changes:
        stats['content_changes'] += 1
    if 'start_date' in changes:
        stats['date_corrections'] += 1
    if any(f in changes for f in LINK_FIELD_NAMES):
        stats['link_changes'] += 1
    label = f"{event.get('name')}"
    print(f"  ↻ Updated #{row_id} ({', '.join(k for k in changes if k not in ('cancel_flagged_at',))}): {label}")


def _apply_insert(c, ph, event, auto_approve, duplicate_of_id, recurring, stats):
    event_type = event.get('event_type', 'music')
    if not event.get('genre') and event_type == 'comedy':
        event['genre'] = 'Comedy'
    cfg = VENUES.get(event.get('venue_key')) or {}
    approve = (auto_approve or cfg.get('auto_approve', False)) and duplicate_of_id is None \
        and 'long_range' not in event.get('flags', []) \
        and not (event.get('end_date') and str(event['end_date'])[:10] != event.get('start_date'))
    review_status = 'approved' if approve else 'pending'
    status = legacy_status(review_status, 'scheduled', duplicate_of_id)
    c.execute(f'''INSERT INTO events
                  (name, start_date, end_date, doors_time, start_time, end_time, multi_day, venue, location,
                   city, state, price, ticket_url, event_url, venue_url, description, genre, confidence,
                   status, review_status, event_status, created_at, event_type, visible, sold_out,
                   date_changed, openers, duplicate_of_id, flags, source, recurring)
                  VALUES ({', '.join([ph] * 31)})''',
              (event['name'], event.get('start_date'), event.get('end_date'), event.get('doors_time'),
               event.get('start_time'), event.get('end_time'), event.get('multi_day', False), event['venue'],
               event.get('location'), event.get('city', ''), event.get('state', ''), event.get('price'),
               event.get('ticket_url'), event.get('event_url'), event.get('venue_url', ''),
               event.get('description'), event.get('genre'), json.dumps(event.get('confidence', {})),
               status, review_status, 'scheduled', datetime.now().isoformat(), event_type,
               event_type in VISIBLE_TYPES, event.get('sold_out', False), event.get('date_changed', False),
               event.get('openers'), duplicate_of_id, json.dumps(event.get('flags', [])),
               event.get('venue_key'), recurring))
    if duplicate_of_id is not None:
        stats['flagged'] += 1
        print(f"  ⚠ Review (linked to #{duplicate_of_id}): {event['name']}")
    else:
        stats['inserted'] += 1


def apply_unified(c, ph, events, shadow, auto_approve, stats):
    """Write the scorer's decision for every event unified matching owns."""
    for d, ev in zip(shadow['decisions'], events):
        if not unified_owns(ev):
            continue
        try:
            decision, target = d['decision'], d['target_id']
            target = int(target) if isinstance(target, int) or (isinstance(target, str) and target.isdigit()) else None
            if decision in ('update', 'reschedule') and target is not None:
                _apply_update(c, ph, ev, target, stats)
                stats['skipped'] += 1
            elif decision == 'review':
                _apply_insert(c, ph, ev, auto_approve, target, None, stats)
            elif decision == 'insert':
                _apply_insert(c, ph, ev, auto_approve, None, d.get('recurring'), stats)
            else:  # batch_duplicate, skip_rejected, excluded, or an update aimed at a same-batch listing
                stats['skipped'] += 1
        except Exception as e:
            print(f"  ✗ Unified write failed for {ev.get('name')}: {e}")


def cancel_unified(c, ph, events, shadow):
    """Cancellation from the scorer: an active future row no listing matched or scored 55+
    against is missing. Same safety layers as check_canceled_events: an empty scrape is
    skipped, a run missing more than max(3, 30%) of active rows is skipped, and a row is
    only canceled on its second consecutive miss."""
    canceled = 0
    today = datetime.now().date().isoformat()
    now = datetime.now().isoformat()
    postponement_targets = {d['target_id'] for d in shadow['decisions'] if d.get('reason') == 'possible_postponement'}
    for venue_key in {e.get('venue_key') for e in events if unified_owns(e)}:
        venue = VENUES[venue_key]['name']
        rows = shadow['rows_by_venue'].get(venue, [])
        active = [r for r in rows if r['start_date'] >= today and r['review_status'] in ('pending', 'approved')
                  and r['event_status'] != 'canceled']
        missing = set(em.missing_rows(active, shadow['claimed'], shadow['best_against'], postponement_targets))
        for r in active:
            if r['id'] not in missing and r.get('cancel_flagged_at'):
                c.execute(f"UPDATE events SET cancel_flagged_at = NULL WHERE id = {ph}", (r['id'],))
                print(f"  ↺ No longer missing, clearing flag: {r['name']}")
        cap = max(3, int(len(active) * 0.3))
        if len(missing) > cap:
            print(f"  ⚠ {len(missing)} events missing at once for {venue} (cap {cap} of {len(active)}) — "
                  f"treating as an unreliable scrape and skipping cancellation")
            continue
        for r in active:
            if r['id'] not in missing:
                continue
            if r.get('cancel_flagged_at'):
                c.execute(f"UPDATE events SET event_status = 'canceled', cancel_flagged_at = NULL WHERE id = {ph}", (r['id'],))
                canceled += 1
                print(f"  ⚠ Confirmed canceled (missing 2 scrapes in a row): {r['name']} on {r['start_date']}")
            else:
                c.execute(f"UPDATE events SET cancel_flagged_at = {ph} WHERE id = {ph}", (now, r['id']))
                print(f"  ⚑ Flagged as cancellation candidate: {r['name']} on {r['start_date']}")
    return canceled


def save_to_database(events, mode='daily', auto_approve=False):
    """Save events to database with exact and partial duplicate detection"""
    from difflib import SequenceMatcher
    dup_threshold_map = {v['name']: v.get('duplicate_threshold', 0.5) for v in VENUES.values()}
    auto_approve_map = {v['name']: v.get('auto_approve', False) for v in VENUES.values()}
    today = datetime.now().strftime('%Y-%m-%d')

    events = expand_multi_night_events(clean_title_tags(events))

    #Current behavior is to filter out past events before saving to DB, but consider revisiting before next year events become common on websites. LLM likely to be used to address.
    before = len(events)
    events = [e for e in events if (e.get('start_date') or '') >= today]
    filtered = before - len(events)
    if filtered:
        print(f"  ↷ Filtered {filtered} past events")

    if not DATABASE_URL:
        print("No DATABASE_URL found - saving to JSON instead")
        with open('gpt_events.json', 'w') as f:
            json.dump({'events': events}, f, indent=2)
        return

    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'

    # Idempotent — cheap no-op after the first run. Must happen here (not only in
    # review_dashboard.py's init_db) since this INSERT runs every daily scrape, not just
    # when the web service happens to restart.
    c.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS flags TEXT")
    # Which scraper produced the row; for tracking aggregator-sourced venues, never read by matching.
    c.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS source TEXT")

    legacy_outcomes = [None] * len(events)
    shadow = shadow_decide(c, events)
    unified_active = MATCH_ENGINE == 'unified' and shadow is not None
    if MATCH_ENGINE == 'unified' and shadow is None:
        print("  ⚠ Unified matching unavailable for this batch; falling back to the legacy path")

    inserted = 0
    skipped = 0
    flagged = 0
    updated = 0
    date_corrections = 0
    # Title drift on an already-saved event (same identity via URL match, different name) —
    # the first of what's meant to be a family of content-change signals (description,
    # openers, etc. later) for venue_health to flag unstable extraction the same way
    # date_corrections already does for dates.
    content_changes = 0
    # Saved events whose ticket or event link changed or was filled in, shown on venue health.
    link_changes = 0

    for i, event in enumerate(events):
        if unified_active and unified_owns(event):
            continue
        try:
            # Computed early — needed both for the URL-match date-correction gate below and
            # the new-insert status decision further down.
            # A long_range row needs a human unless it's a never-public row auto-approved per event.
            venue_auto_approve = event.get('auto_approve', False) or (
                (auto_approve or auto_approve_map.get(event['venue'], False))
                and 'long_range' not in event.get('flags', []))

            # Exact duplicate check: name + start_date + venue + start_time
            c.execute(f'''SELECT id, ticket_url, event_url FROM events
                            WHERE name = {ph} AND start_date = {ph}
                            AND venue = {ph}
                            AND (start_time = {ph} OR (start_time IS NULL AND {ph} IS NULL))
                            AND status != 'canceled'
                            LIMIT 1''',
                        (event['name'], event.get('start_date'), event['venue'],
                        event.get('start_time'), event.get('start_time')))
            exact_match = c.fetchone()
            if exact_match:
                # Backfill ticket_url/event_url if this exact-match row is still missing one
                # a later scrape now has (e.g. Toyota Center's AXS ticket_url, which only
                # exists in the on-sale-now carousel and so is genuinely null on first scrape).
                # Without this, a show whose name/date/time never change would hit this skip
                # every single day and the real link would never get written, even once it
                # becomes available — unlike start_time drift, which the near-match/URL-match
                # paths below already self-heal.
                ex_id, ex_ticket_url, ex_event_url = exact_match
                backfill = {}
                if event.get('ticket_url') and not ex_ticket_url:
                    backfill['ticket_url'] = event['ticket_url']
                if event.get('event_url') and not ex_event_url:
                    backfill['event_url'] = event['event_url']
                if backfill:
                    set_clause = ', '.join(f'{k} = {ph}' for k in backfill)
                    c.execute(f'UPDATE events SET {set_clause} WHERE id = {ph}',
                              list(backfill.values()) + [ex_id])
                    print(f"  ↻ Backfilled ({', '.join(backfill)}): {event['name']}")
                    link_changes += 1
                legacy_outcomes[i] = {'outcome': 'exact_skip', 'target_id': ex_id}
                skipped += 1
                continue

            # URL match: same ticket_url OR same event_url means same event regardless of
            # title/time drift. Checked as a combined OR so a venue that reports both fields
            # only needs ONE of them to still match (e.g. a ticket vendor swaps the ticket_url
            # mid-run but the venue's own event_url page is stable, or vice versa) — requiring
            # both would silently miss a real match and fall through to a duplicate insert.
            #
            # Whether each field requires start_date to also match is controlled per-venue via
            # dupe_use_ticket_url / dupe_use_event_url / dupe_use_start_date on the event dict
            # (stamped in scrape_venue, default True/True/True). True is the safe default for
            # both URL fields: some venues reuse one link across genuinely different dates (a
            # comic's profile page at Improv Houston, a recurring weekly show's template page at
            # Dan Electro's) and a bare match there would silently merge distinct shows. Only
            # flip dupe_use_start_date to False for a venue once its URL is confirmed unique per
            # occurrence (e.g. Heights Theater's numeric Prekindle event ID) — that's what lets a
            # URL match with a *different* start_date be treated as a correction below instead of
            # falling through to a duplicate insert.
            dupe_use_ticket_url = event.get('dupe_use_ticket_url', True)
            dupe_use_event_url = event.get('dupe_use_event_url', True)
            dupe_use_start_date = event.get('dupe_use_start_date', True)

            # Checked cross-field (a new ticket_url can match a stored event_url and vice
            # versa) since the LLM doesn't reliably pick the same field name for the same
            # link between scrapes (confirmed at HOB and Arena Theatre).
            url_paths = []
            url_params = []
            def add_url_check(new_value, scope_by_date):
                if scope_by_date:
                    url_paths.append(f'(ticket_url = {ph} AND start_date = {ph})')
                    url_params.extend([new_value, event.get('start_date')])
                    url_paths.append(f'(event_url = {ph} AND start_date = {ph})')
                    url_params.extend([new_value, event.get('start_date')])
                else:
                    url_paths.append(f'ticket_url = {ph}')
                    url_params.append(new_value)
                    url_paths.append(f'event_url = {ph}')
                    url_params.append(new_value)
            if dupe_use_ticket_url and event.get('ticket_url'):
                add_url_check(event['ticket_url'], dupe_use_start_date)
            if dupe_use_event_url and event.get('event_url'):
                add_url_check(event['event_url'], dupe_use_start_date)

            if url_paths:
                # No LIMIT 1 here — once two+ rows already share a matching URL (the exact
                # scenario this block exists to prevent happening again), an arbitrary single
                # candidate can easily be the wrong one (e.g. an earlier, truncated extraction
                # of the same show) and fail the name-similarity floor below, discarding a
                # perfectly good match and inserting yet another duplicate. Confirmed causing
                # compounding growth at Toyota Center: once 2-3 rows existed for one event,
                # each new scrape became more likely to draw the wrong one and add a 4th, 5th...
                # Fetching all candidates and picking the best-matching name fixes that without
                # weakening the floor itself.
                c.execute(f'''SELECT id, name, start_date, start_time, doors_time, end_time, sold_out, date_changed, openers, price, ticket_url, event_url, status
                                FROM events
                                WHERE venue = {ph}
                                AND status NOT IN ('rejected', 'canceled')
                                AND ({' OR '.join(url_paths)})''',
                            [event['venue']] + url_params)
                url_candidates = c.fetchall()
            else:
                url_candidates = []
            url_match = None
            if url_candidates:
                # A URL match alone isn't sufficient proof of identity when a venue reuses one
                # link across genuinely different shows (a generic storefront link, a comedian's
                # profile page reused across their own multiple nights, etc.) — require the names
                # to be at least plausibly related so two unrelated same-venue/same-date events
                # that happen to share a link don't get silently merged. Legitimate title drift
                # (an opener added, punctuation changes) stays well above this floor.
                NAME_SIMILARITY_FLOOR = 0.4
                best = max(url_candidates,
                           key=lambda cand: SequenceMatcher(None, event['name'].lower(), cand[1].lower()).ratio())
                if SequenceMatcher(None, event['name'].lower(), best[1].lower()).ratio() >= NAME_SIMILARITY_FLOOR:
                    url_match = best
            if url_match:
                (ex_id, ex_name, ex_start_date, ex_start, ex_doors, ex_end, ex_sold_out,
                 ex_date_changed, ex_openers, ex_price, ex_ticket_url, ex_event_url, ex_status) = url_match
                changes = {}
                if event['name'] != ex_name:
                    changes['name'] = event['name']
                if event.get('start_time') is not None and event.get('start_time') != ex_start:
                    changes['start_time'] = event['start_time']
                if event.get('doors_time') is not None and event.get('doors_time') != ex_doors:
                    changes['doors_time'] = event['doors_time']
                if event.get('end_time') is not None and event.get('end_time') != ex_end:
                    changes['end_time'] = event['end_time']
                if event.get('sold_out', False) != ex_sold_out:
                    changes['sold_out'] = event.get('sold_out', False)
                if 'date_changed' in event and event['date_changed'] != ex_date_changed:
                    changes['date_changed'] = event['date_changed']
                if event.get('openers') and event.get('openers') != ex_openers:
                    changes['openers'] = event['openers']
                if event.get('price') and event.get('price') != ex_price:
                    changes['price'] = event['price']
                # Refresh whichever URL field drifted — this is the case the OR-matched query
                # is built for: one field (say event_url) matched and proved identity, while the
                # other (ticket_url) changed, e.g. a ticket vendor rotated the link. Keep the
                # stored value current so future scrapes still match on that field too.
                if event.get('ticket_url') and event.get('ticket_url') != ex_ticket_url:
                    changes['ticket_url'] = event['ticket_url']
                if event.get('event_url') and event.get('event_url') != ex_event_url:
                    changes['event_url'] = event['event_url']
                # Only reachable when the matched field's own start_date requirement was
                # dropped (dupe_use_start_date False) — otherwise the query itself already
                # required start_date equality, so this can't differ. Treat this like any other
                # drifted field on a confirmed-same event — update in place — but an already-
                # public (approved) row getting its date silently changed needs a human's eyes
                # first unless the venue is trusted enough to auto-approve; same trust boundary
                # new inserts already use.
                date_corrected = False
                if event.get('start_date') and event.get('start_date') != ex_start_date:
                    changes['start_date'] = event['start_date']
                    date_corrected = True
                    if ex_status == 'approved' and not venue_auto_approve:
                        changes['status'] = 'pending'
                if changes:
                    set_clause = ', '.join(f'{k} = {ph}' for k in changes)
                    c.execute(f'UPDATE events SET {set_clause} WHERE id = {ph}',
                              list(changes.values()) + [ex_id])
                    label = f"{ex_name} → {event['name']}" if 'name' in changes else ex_name
                    if 'name' in changes:
                        content_changes += 1
                    if 'ticket_url' in changes or 'event_url' in changes:
                        link_changes += 1
                    if date_corrected:
                        date_corrections += 1
                        review_note = ' — sent back to pending for review' if changes.get('status') == 'pending' else ''
                        print(f"  ⚠ Date corrected ({ex_start_date} → {event['start_date']}){review_note}: {label}")
                    else:
                        print(f"  ↻ Updated ({', '.join(changes)}): {label}")
                    updated += 1
                legacy_outcomes[i] = {'outcome': 'url_update', 'target_id': ex_id, 'date_corrected': date_corrected}
                skipped += 1
                continue

            # Near-match check: same name + start_date + venue, different start_time (ICS time update)
            # Single result means it's the same event with an updated time — update in place
            c.execute(f'''SELECT id, start_time FROM events
                            WHERE name = {ph} AND start_date = {ph} AND venue = {ph}
                            AND status != 'canceled' ''',
                        (event['name'], event.get('start_date'), event['venue']))
            near = c.fetchall()
            if len(near) == 1:
                existing_id, existing_time = near[0]
                new_time = event.get('start_time')
                if existing_time != new_time:
                    c.execute(f'''UPDATE events SET start_time = {ph}, end_time = {ph}
                                  WHERE id = {ph}''',
                              (new_time, event.get('end_time'), existing_id))
                    print(f"  ↻ Time updated: {event['name']} ({existing_time} → {new_time})")
                legacy_outcomes[i] = {'outcome': 'near_update', 'target_id': existing_id}
                skipped += 1
                continue

            # Partial duplicate check: same venue + start_date, similar name
            # Only check against pending/approved — excluding possible_duplicate prevents cascade chains
            c.execute(f'''SELECT id, name, start_time FROM events
                            WHERE venue = {ph} AND start_date = {ph}
                            AND status IN ('pending', 'approved') ''',
                        (event['venue'], event.get('start_date')))
            existing = c.fetchall()
            status = 'approved' if venue_auto_approve else 'pending'
            duplicate_of_id = None
            event_type = event.get('event_type', 'music')
            visible = event_type in ('music', 'comedy', 'performing_arts', 'arts', 'sports')
            if not event.get('genre') and event_type == 'comedy':
                event['genre'] = 'Comedy'
            dup_threshold = dup_threshold_map.get(event['venue'], 0.5)
            # Skip fuzzy-duplicate flagging entirely for events auto-approved for being a
            # non-visible type (e.g. ICS-fed civic meetings) — flagging exists to help a
            # human tell two similarly-named events apart, but nothing here is ever shown
            # to a human either way, so a flag would just re-add it to a review surface
            # (possible_duplicate) that auto-approve was specifically meant to avoid.
            if not event.get('auto_approve'):
                for ex_id, ex_name, ex_time in existing:
                    if event.get('dupe_check_start_time'):
                        new_time = event.get('start_time')
                        if new_time and ex_time and new_time != ex_time:
                            continue
                    similarity = SequenceMatcher(
                        None, strip_opener_clause(event['name'].lower()), strip_opener_clause(ex_name.lower())
                    ).ratio()
                    if similarity >= dup_threshold:
                        status = 'possible_duplicate'
                        duplicate_of_id = ex_id
                        print(f"  ⚠ Possible duplicate ({int(similarity*100)}% match): "
                            f"{event['name']} ~ {ex_name}")
                        flagged += 1
                        break
                if status == 'approved' and event.get('end_date') and event['end_date'] != event.get('start_date'):
                    status = 'pending'

            # Insert event
            c.execute(f'''INSERT INTO events
                            (name, start_date, end_date, doors_time, start_time, end_time,
                            multi_day, venue, location, city, state,
                            price, ticket_url, event_url, venue_url, description, genre, confidence, status, created_at,
                            event_type, visible, sold_out, date_changed, openers, duplicate_of_id, flags, source)
                            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
                            RETURNING id''',
                        (event['name'],
                        event.get('start_date'),
                        event.get('end_date'),
                        event.get('doors_time'),
                        event.get('start_time'),
                        event.get('end_time'),
                        event.get('multi_day', False),
                        event['venue'], event.get('location'),
                        event.get('city', ''), event.get('state', ''),
                        event.get('price'), event.get('ticket_url'),
                        event.get('event_url'),
                        event.get('venue_url', ''),
                        event.get('description'), event.get('genre'),
                        json.dumps(event.get('confidence', {})),
                        status, datetime.now().isoformat(),
                        event_type, visible,
                        event.get('sold_out', False),
                        event.get('date_changed', False),
                        event.get('openers'), duplicate_of_id,
                        # Extraction-circumstance flags (e.g. 'split_boundary') — a general,
                        # extensible list rather than a one-off boolean column per signal, so
                        # a future flag (see CLAUDE.md's dupe-hierarchy to-do) is just a new
                        # code, not a new migration.
                        json.dumps(event.get('flags', [])), event.get('venue_key')))
            inserted += 1
            new_id = c.fetchone()[0]
            legacy_outcomes[i] = ({'outcome': 'possible_duplicate', 'target_id': duplicate_of_id, 'new_id': new_id}
                                  if status == 'possible_duplicate' else {'outcome': 'insert', 'new_id': new_id})
        except Exception as e:
            print(f"Error inserting {event['name']}: {e}")
            legacy_outcomes[i] = {'outcome': 'error'}

    canceled = 0
    if unified_active:
        u = dict.fromkeys(('inserted', 'updated', 'date_corrections', 'content_changes', 'link_changes',
                           'skipped', 'flagged'), 0)
        apply_unified(c, ph, events, shadow, auto_approve, u)
        canceled = cancel_unified(c, ph, events, shadow)
        inserted += u['inserted']; updated += u['updated']; date_corrections += u['date_corrections']
        content_changes += u['content_changes']; link_changes += u['link_changes']
        skipped += u['skipped']; flagged += u['flagged']
    conn.commit()
    shadow_log(c, events, shadow, legacy_outcomes, mode)
    conn.commit()
    conn.close()
    print(f"  ✓ Inserted {inserted}, updated {updated} ({date_corrections} date corrections, {content_changes} content changes), skipped {skipped} duplicates, flagged {flagged} possible duplicates")
    return {
        'inserted': inserted,
        'updated': updated,
        'date_corrections': date_corrections,
        'content_changes': content_changes,
        'link_changes': link_changes,
        'canceled': canceled,
        'skipped': skipped,
        'flagged': flagged
    }

def log_scrape_stats(venue_name, mode, stats, canceled_count=0, error=None):
    if not DATABASE_URL:
        return
    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s'
    today = datetime.now().strftime('%Y-%m-%d')

    # Idempotent migration for the two update-tracking columns — cheap no-op after the
    # first run. Lets venue_health surface an updates/date-corrections trend per venue, the
    # signal that would have caught the Heights Theater date-drift bug before 51 duplicate
    # rows piled up undetected.
    c.execute('''ALTER TABLE scrape_stats ADD COLUMN IF NOT EXISTS updated_events INTEGER DEFAULT 0''')
    c.execute('''ALTER TABLE scrape_stats ADD COLUMN IF NOT EXISTS date_corrections INTEGER DEFAULT 0''')
    c.execute('''ALTER TABLE scrape_stats ADD COLUMN IF NOT EXISTS content_changes INTEGER DEFAULT 0''')
    c.execute('''ALTER TABLE scrape_stats ADD COLUMN IF NOT EXISTS link_changes INTEGER DEFAULT 0''')
    c.execute('''ALTER TABLE scrape_stats ADD COLUMN IF NOT EXISTS splits INTEGER DEFAULT 0''')
    c.execute('''ALTER TABLE scrape_stats ADD COLUMN IF NOT EXISTS error TEXT''')

    # Get current totals
    c.execute(f'''SELECT
        COUNT(*) FILTER (WHERE status = 'approved') as approved,
        COUNT(*) FILTER (WHERE status = 'pending') as pending,
        COUNT(*) FILTER (WHERE status = 'rejected') as rejected
        FROM events WHERE venue = {ph}''', (venue_name,))
    row = c.fetchone()

    c.execute(f'''INSERT INTO scrape_stats
        (scrape_date, venue, total_scraped, total_approved, total_rejected,
         new_events, canceled_events, total_pending, scrape_mode, updated_events, date_corrections,
         content_changes, splits, error, link_changes)
        VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})''',
        (today, venue_name,
         stats.get('inserted', 0) + stats.get('skipped', 0),
         row[0], row[2],
         stats.get('inserted', 0),
         canceled_count,
         row[1],
         mode,
         stats.get('updated', 0),
         stats.get('date_corrections', 0),
         stats.get('content_changes', 0),
         SPLIT_COUNTS.pop(venue_name, 0),
         error[:1000] if error else None,
         stats.get('link_changes', 0)))
    conn.commit()
    conn.close()

def detect_existing_duplicates(dry_run=False):
    """Scan the events table for possible duplicates and update their stads"""
    from difflib import SequenceMatcher
    from collections import defaultdict

    # Mirrors the same per-venue dupe_check_start_time toggle save_to_database's partial-match
    # check uses (see ce1dcdf) — without it, this sweep re-flags multi-showtime venues (comedy
    # clubs running distinct early/late sets) that were already carved out at insert time.
    dupe_check_start_time_map = {v['name']: v.get('dupe_check_start_time', False) for v in VENUES.values()}

    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'

    # Only scan active events — skip already-flagged, rejected, canceled
    c.execute('''SELECT id, name, start_date, venue, start_time FROM events
                 WHERE status IN ('pending', 'approved')
                 ORDER BY id ASC''')
    rows = c.fetchall()

    groups = defaultdict(list)
    for event_id, name, start_date, venue, start_time in rows:
        groups[(venue, start_date)].append((event_id, name, start_time))

    flagged = 0
    for (venue, start_date), group in groups.items():
        if len(group) < 2:
            continue
        check_start_time = dupe_check_start_time_map.get(venue, False)
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                id_a, name_a, time_a = group[i]
                id_b, name_b, time_b = group[j]
                if check_start_time and time_a and time_b and time_a != time_b:
                    continue
                similarity = SequenceMatcher(None, name_a.lower(), name_b.lower()).ratio()
                if similarity >= 0.5:
                    # Keep the older event (lower id); flag the newer one
                    orig_id, orig_name = (id_a, name_a) if id_a < id_b else (id_b, name_b)
                    dup_id, dup_name = (id_b, name_b) if id_a < id_b else (id_a, name_a)
                    print(f"  ⚠ ({int(similarity*100)}%) #{dup_id} '{dup_name}' ~ #{orig_id} '{orig_name}' [{venue} {start_date}]")
                    if not dry_run:
                        c.execute(f'''UPDATE events SET status = 'possible_duplicate', duplicate_of_id = {ph}
                                      WHERE id = {ph}''', (orig_id, dup_id))
                    flagged += 1

    if not dry_run:
        conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"{'[DRY RUN] ' if dry_run else ''}Flagged {flagged} possible duplicates")
    print(f"{'='*60}")

def _looks_recurring(dates):
    """True if a list of ISO date strings forms a regular weekly (or other spaced) pattern — the
    shape a legitimate recurring class/show produces (weekly Sunday Yoga), as opposed to the essentially arbitrary small offsets a genuine date-drift
    bug produces (the Heights Theater case this function was originally built for). Same-weekday
    alone requires 3+ dates — on just 2 dates it's too weak a signal, since a bug that misreads
    the wrong row of a weekly calendar grid would also land on the same weekday a clean number
    of weeks off.
    """
    try:
        d = sorted({datetime.strptime(x, '%Y-%m-%d') for x in dates})
    except (ValueError, TypeError):
        return False
    if len(d) < 2:
        return False
    gaps = {(d[i + 1] - d[i]).days for i in range(len(d) - 1)}
    if len(d) == 2:
        # A single gap is trivially "constant" — only trust a weekly one. Back-to-back days
        # are a multi-night run, never recurrence (2026-11-10).
        return gaps == {7}
    if len(gaps) == 1 and min(gaps) > 1:
        return True
    if len(d) >= 3 and len({x.weekday() for x in d}) == 1:
        return True
    return False

def detect_url_date_conflicts(dry_run=False):
    """Scan for active events sharing a ticket_url or event_url with another active event at
    the same venue but a DIFFERENT start_date — the shape of bug that let 51 duplicate rows
    accumulate undetected at Heights Theater (each re-scrape extracting a slightly different,
    wrong date for the same show, and neither the exact-match nor URL-match check in
    save_to_database catches a same-link-different-date pair by design).

    save_to_database's own update-in-place logic (dupe_use_start_date) now self-heals this for
    any venue explicitly confirmed to have a unique-per-event URL. This is the backstop for
    every other venue — where that confirmation hasn't been done, so a genuine date drift still
    falls through as a fresh insert instead of an update — and for detect_existing_duplicates'
    blind spot too (same-date fuzzy-name matching won't catch two rows that disagree on date at
    all). Flags the newer row as possible_duplicate rather than resolving anything automatically
    — the correct date isn't decidable from the DB alone (see the Willow Avalon/Heights cleanup:
    the drift direction wasn't consistent, so "oldest wins" would sometimes be wrong).

    A shared URL can also mean a legitimate recurring class/show (Hotel Lucine's Sunset Pilates,
    Dan Electro's weekly Karaoke) rather than a drift bug — the `recurring` column disambiguates:
    'no' keeps this function's flagging behavior unchanged (safety valve for a drift bug that
    coincidentally looks regular); 'yes' (human-confirmed) is propagated to any new same-URL rows
    and skips flagging entirely, which is how a freshly-scraped instance of an already-confirmed
    series gets recognized immediately, without needing 2+ of its own dates to prove a pattern —
    no separate registry table needed since this function already re-scans the whole table weekly;
    otherwise, a group whose dates form a regular pattern (_looks_recurring) gets stamped
    'possible' instead of flagged, surfaced for a human to confirm via the review dashboard.
    """
    from collections import defaultdict

    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'

    if DATABASE_URL:
        c.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS recurring TEXT")

    c.execute('''SELECT id, name, venue, start_date, ticket_url, event_url, recurring
                 FROM events
                 WHERE status IN ('pending', 'approved')
                 AND (ticket_url IS NOT NULL OR event_url IS NOT NULL)
                 ORDER BY id ASC''')
    rows = c.fetchall()

    # A field a venue's own config has marked unreliable_urls (a generic listing page, never
    # per-event — e.g. Punch Line's /shows, House of Blues' /shows) is never valid duplicate
    # evidence, even on legacy rows scraped before that flag was set (which never got
    # retroactively nulled). Without this, every event sharing that one non-unique value gets
    # cross-linked to whichever row has the lowest id, regardless of date or name — confirmed
    # responsible for the bulk of Punch Line's and House of Blues' possible_duplicate backlog.
    unreliable_map = {v['name']: v.get('unreliable_urls') for v in VENUES.values()}

    groups = defaultdict(list)
    for event_id, name, venue, start_date, ticket_url, event_url, recurring in rows:
        unreliable = unreliable_map.get(venue)
        if ticket_url and unreliable not in (True, 'ticket_url'):
            groups[('ticket_url', venue, ticket_url)].append((event_id, name, start_date, recurring))
        if event_url and unreliable not in (True, 'event_url'):
            groups[('event_url', venue, event_url)].append((event_id, name, start_date, recurring))

    flagged = 0
    recurring_stamped = 0
    seen_ids = set()
    for (field, venue, url), group in groups.items():
        dates_in_group = {r[2] for r in group}
        if len(dates_in_group) < 2:
            continue

        recurring_values = {r[3] for r in group}
        null_ids = [r[0] for r in group if r[3] is None]

        if 'no' in recurring_values:
            pass  # safety valve — fall through to normal flagging below, unchanged
        elif 'yes' in recurring_values or _looks_recurring(dates_in_group):
            new_value = 'yes' if 'yes' in recurring_values else 'possible'
            if null_ids:
                print(f"  ↻ {field} looks recurring ({new_value}): stamping {len(null_ids)} row(s) "
                      f"for '{group[0][1]}' [{venue}] instead of flagging as duplicates")
                if not dry_run:
                    id_placeholders = ','.join(ph for _ in null_ids)
                    c.execute(f'''UPDATE events SET recurring = {ph} WHERE id IN ({id_placeholders})''',
                              [new_value] + null_ids)
                recurring_stamped += len(null_ids)
            continue

        group.sort(key=lambda r: r[0])
        canonical_id, canonical_name, canonical_date = group[0][0], group[0][1], group[0][2]
        for event_id, name, start_date, _recurring in group[1:]:
            if start_date == canonical_date or event_id in seen_ids:
                continue
            print(f"  ⚠ {field} shared across dates: #{event_id} '{name}' ({start_date}) "
                  f"~ #{canonical_id} '{canonical_name}' ({canonical_date}) [{venue}]")
            if not dry_run:
                c.execute(f'''UPDATE events SET status = 'possible_duplicate', duplicate_of_id = {ph}
                              WHERE id = {ph}''', (canonical_id, event_id))
            seen_ids.add(event_id)
            flagged += 1

    if not dry_run:
        conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"{'[DRY RUN] ' if dry_run else ''}Flagged {flagged} URL/date conflicts, "
          f"stamped {recurring_stamped} recurring row(s)")
    print(f"{'='*60}")
    return flagged

def archive_past_events(buffer_days=1):
    """Move past events (of any review status, including never-reviewed pending
    ones) to the past_events table. pending is included deliberately — an event
    whose start_date has already passed and was never reviewed still needs to
    leave the active dashboard, and archiving preserves its status as 'pending'
    rather than silently deciding it for the reviewer."""
    if not DATABASE_URL:
        print("No DATABASE_URL — skipping archive (local dev)")
        return
    conn = get_db_connection()
    c = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=buffer_days)).strftime('%Y-%m-%d')
    print(f"\n{'='*60}")
    print(f"Archiving events before {cutoff}...")
    c.execute('''SELECT id, name, start_date, end_date, doors_time, start_time, end_time,
                 multi_day, venue, location, city, state,
                 price, ticket_url, description, genre, confidence, notes, status,
                 created_at, approved_at, event_type, visible, sold_out, date_changed,
                 openers, event_url, review_status, event_status
                 FROM events
                 WHERE status IN ('approved', 'possible_duplicate', 'rejected', 'canceled', 'pending') AND start_date < %s''', (cutoff,))
    rows = c.fetchall()
    archived_at = datetime.now().isoformat()
    archived = 0
    for row in rows:
        try:
            c.execute('''INSERT INTO past_events
                         (name, start_date, end_date, doors_time, start_time, end_time,
                          multi_day, venue, location, city, state,
                          price, ticket_url, description, genre, confidence, notes, status,
                          created_at, approved_at, archived_at, event_type, visible, sold_out,
                          date_changed, openers, event_url, review_status, event_status)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                      (row[1], row[2], row[3], row[4], row[5], row[6], row[7],
                       row[8], row[9], row[10], row[11], row[12],
                       row[13], row[14], row[15], row[16], row[17], row[18], row[19],
                       row[20], archived_at, row[21], row[22], row[23], row[24],
                       row[25], row[26], row[27], row[28]))
            c.execute('DELETE FROM events WHERE id = %s', (row[0],))
            archived += 1
        except Exception as e:
            print(f"  ✗ Error archiving {row[1]}: {e}")
    conn.commit()
    conn.close()
    print(f"✓ Archived {archived} past events")
    print(f"{'='*60}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Houston Music Events Scraper')
    parser.add_argument('--mode', choices=['daily', 'weekly', 'onboard'], default='daily',
                        help='Scrape mode: daily, weekly, or onboard (new venue full scrape)')
    parser.add_argument('--auto-approve', action='store_true',
                        help='Auto-approve events (use with onboard for trusted venues)')
    parser.add_argument('--venue', type=str, default=None,
                        help='Scrape a single venue by key (e.g. white_oak)')
    parser.add_argument('--group', type=str, default=None,
                        help="Scrape only venues tagged with this schedule_group (e.g. afternoon). Ignored if --venue is set.")
    parser.add_argument('--llm', choices=['gpt4o', 'gpt4o-mini', 'claude', 'groq'], default='gpt4o-mini',
                        help='LLM provider to use for extraction')
    parser.add_argument('--dry-run', action='store_true',
                        help='Extract and print events without writing to DB')
    parser.add_argument('--detect-duplicates', action='store_true',
                        help='Scan existing events table for duplicates and flag them')
    parser.add_argument('--match-report', type=int, nargs='?', const=7, default=None, metavar='DAYS',
                        help='Compare unified-matching shadow decisions with the legacy path (read-only)')
    parser.add_argument('--check-arena-dates', action='store_true',
                        help="Compare active Arena rows' dates with the dates in their links (read-only)")
    parser.add_argument('--leak-report', action='store_true',
                        help='List active same-day pairs the scorer says are unlinked duplicates (read-only)')
    args = parser.parse_args()

    if args.match_report is not None:
        match_report(args.match_report)
        exit(0)
    if args.leak_report:
        leak_report()
        exit(0)
    if args.check_arena_dates:
        check_arena_dates()
        exit(0)

    if DATABASE_URL and not args.dry_run:
        _conn = get_db_connection()
        migrate_status_columns(_conn.cursor(), True)
        _conn.commit()
        _conn.close()

    init_db()

    if args.detect_duplicates:
        print(f"=== Duplicate Detection {'[DRY RUN] ' if args.dry_run else ''}===\n")
        detect_existing_duplicates(dry_run=args.dry_run)
        exit(0)

    # Random delay so cron-triggered scrapes don't hit venue sites at a predictable
    # fixed time every day (rate-limit avoidance). The afternoon schedule_group gets a
    # shorter window since it's already anchored to a narrower part of the day than the
    # main daily/weekly 6am runs. Skipped for dry-run and onboard — both are interactive,
    # need-it-now runs (checking extraction output, bringing a new venue online) where a
    # multi-hour wait defeats the point. SKIP_DELAY bypasses it for any other manual run.
    if not os.environ.get('SKIP_DELAY') and not args.dry_run and args.mode != 'onboard':
        import random
        delay_max = 3 * 60 * 60 if args.group == 'afternoon' else 12 * 60 * 60
        delay = random.randint(0, delay_max)
        print(f"Sleeping {delay // 3600}h {(delay % 3600) // 60}m before scraping...")
        time.sleep(delay)

    print(f"=== Houston Music Events Scraper [{args.mode} mode] ===\n")

    # Weekly maintenance sweep: archive past events, and flag duplicates that only
    # per-venue scrape logic can't catch — same-date fuzzy-name collisions
    # (detect_existing_duplicates) and same-link-different-date collisions
    # (detect_url_date_conflicts), the latter added after the Heights Theater date-drift
    # cleanup on 2026-07-30.
    if args.mode == 'weekly':
        archive_past_events(buffer_days=1)
        try:
            leak_report(write=True)
        except Exception as e:
            print(f"  ⚠ Leak monitor failed: {e}")
        if MATCH_ENGINE != 'unified':
            print(f"\n=== Weekly duplicate sweep ===")
            detect_existing_duplicates(dry_run=False)
            detect_url_date_conflicts(dry_run=False)

    if args.venue:
        if args.venue not in VENUES:
            print(f"✗ Unknown venue key: {args.venue}")
            print(f"  Available: {', '.join(VENUES.keys())}")
        else:
            events, canceled_count = scrape_venue(args.venue, mode=args.mode, llm=args.llm, dry_run=args.dry_run)
            if args.dry_run:
                print(f"\n{'='*60}")
                print(f"DRY RUN — {len(events)} events extracted, not saved")
                for e in events:
                    ticket = e.get('ticket_url') or e.get('event_url') or '—'
                    print(f"  {e.get('start_date')} | {e.get('end_date')} |{e.get('start_time')} | {e.get('venue')} | {e.get('name')} | {ticket}")
                print(f"{'='*60}")
            else:
                for e in events:
                    e.setdefault('venue_key', args.venue)
                stats = save_to_database(events, mode=args.mode, auto_approve=args.auto_approve)
                log_scrape_stats(VENUES[args.venue]['name'], args.mode, stats, canceled_count + (stats or {}).get('canceled', 0))
    else:
        all_events = scrape_all_venues(mode=args.mode, llm=args.llm, auto_approve=args.auto_approve, group=args.group,
                                       dry_run=args.dry_run)
        if args.dry_run:
            print(f"\n{'='*60}")
            print(f"DRY RUN — {len(all_events)} events extracted, not saved")
            for e in all_events:
                print(f"  {e.get('start_date')} | {e.get('start_time')} | {e.get('venue')} | {e.get('location')} | {e.get('name')}")
            print(f"{'='*60}")
    print(f"✓ Scrape complete")
