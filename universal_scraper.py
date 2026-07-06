import os
import json
import time
import sqlite3
from dateutil.utils import today
import requests
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
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
        "city": "Houston",
        "state": "TX",
        "wait_time": 3
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
        "city": "Houston",
        "state": "TX",
        "wait_time": 3
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
        "venue_instruction": "Extract all events. For each event, populate the 'location' field with the specific room: 'Main Stage', 'Bronze Peacock', or 'Foundation Room'. If no room is specified, use 'Main Stage'."
    },
    "smart_financial": {
        "name": "Smart Financial Centre",
        "url": "https://us.atgtickets.com/venues/smart-financial-centre/whats-on/us/concert/",
        "city": "Sugar Land",
        "state": "TX",
        "wait_time": 6,
        "scroll_count": 3
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
        "page_param": "start",
        "venue_instruction": "This page covers multiple Texas Improv locations. Only extract events for 'Houston Improv' — ignore Addison Improv, Arlington Improv, LOL San Antonio and any other locations. Set venue to 'Improv Houston' and city to 'Houston' for all extracted events.",
    },
    "riot_comedy": {
        "name": "The Riot Comedy Club",
        "url": "https://theriothtx.com/headlining-comedians-at-the-riot-comedy-club-in-houston-texas/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 2
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
    },
    "nrg_park": {
        "name": "NRG Park",
        "url": "https://www.nrgpark.com/events-tickets/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 3
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

def check_canceled_events(venue_key, scraped_events):
    """Compare scraped events against DB future events, flag missing as canceled.
    Uses fuzzy name matching to avoid false positives from minor title drift."""
    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'
    venue_name = VENUES[venue_key]['name']
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute(f'''SELECT id, name, start_date FROM events
                 WHERE venue = {ph} AND start_date >= {ph}
                 AND status IN ('approved', 'pending')''',
              (venue_name, today))
    db_events = c.fetchall()

    # Build scraped names grouped by date for efficient same-day comparison
    scraped_by_date = {}
    for e in scraped_events:
        date = e.get('start_date')
        if date:
            scraped_by_date.setdefault(date, []).append(e['name'].strip().lower())

    FUZZY_THRESHOLD = 0.85

    def fuzzy_match(db_name, scraped_names):
        db_lower = db_name.strip().lower()
        for s in scraped_names:
            if db_lower == s:
                return True
            # Handle suffix additions like "(Sold Out)", "(New Date)"
            if db_lower in s or s in db_lower:
                return True
            if SequenceMatcher(None, db_lower, s).ratio() >= FUZZY_THRESHOLD:
                return True
        return False

    canceled_count = 0
    for db_id, db_name, db_date in db_events:
        same_day = scraped_by_date.get(db_date, [])
        if not fuzzy_match(db_name, same_day):
            c.execute(f'''UPDATE events SET status = 'canceled'
                         WHERE id = {ph}''', (db_id,))
            canceled_count += 1
            print(f"  ⚠ Flagged as canceled: {db_name} on {db_date}")
    conn.commit()
    conn.close()
    print(f"  {canceled_count} events flagged as canceled for {venue_name}")
    return canceled_count

def scrape_page(url, wait_time=3, debug=False, scroll_count=1, load_more_id=None):
    """Scrape a page using Selenium"""
    print(f"Fetching {url}...")
    
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        driver.get(url)
        time.sleep(wait_time)
        
        for i in range(scroll_count):
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
        for tag in soup(['script', 'style']):
            tag.decompose()
        page_text = soup.get_text(separator='\n', strip=True)

        # Append all external links so the LLM can extract ticket_url / event_url
        links_seen = set()
        link_lines = []
        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            if not href or href.startswith('#') or href.startswith('mailto:') or href in links_seen:
                continue
            links_seen.add(href)
            label = a.get_text(strip=True) or ''
            link_lines.append(f"{label} -> {href}")
        if link_lines:
            page_text += '\n\nLINKS:\n' + '\n'.join(link_lines)

        print(f"Extracted {len(page_text)} characters")
        print(f"HTML length: {len(html)} characters")  # Show raw HTML size

        return page_text, html  # Return both
        
    finally:
        driver.quit()

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

                event = {
                    'name': item.get('title', ''),
                    'start_date': start_dt[:10] if start_dt else None,
                    'end_date': None,
                    'multi_day': False,
                    'start_time': start_dt[11:16] if start_dt else None,
                    'end_time': None,
                    'doors_time': None,
                    'venue': venue_name,
                    'location': None,
                    'city': city,
                    'state': state,
                    'price': item.get('cost_display'),
                    'ticket_url': item.get('cost_external_url'),
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
    return extract_events_with_llm_raw(
        all_html,
        venue_config['name'],
        venue_config['city'],
        venue_config['state'],
        is_html=True,
        venue_instruction=venue_config.get('venue_instruction')
    )

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
        venue = location.split(',')[0].strip() if location else venue_name  
        
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

def extract_events_with_llm_raw(content, venue_name, city, state, is_html=False, venue_instruction=None, llm='gpt4o-mini'):
    """Extract events using LLM from text or HTML"""
    content_type = "HTML code" if is_html else "text"
    venue_note = f"\n\nVENUE NOTE: {venue_instruction}" if venue_instruction else f'\n- venue: "{venue_name}"'
    system_prompt = f"""You are an expert at extracting structured event data from venue websites. Today's date is {datetime.now().strftime('%Y-%m-%d')}.
Extract ALL events from the provided {content_type} into a JSON array. For each event:

TITLE CLEANUP (apply before setting name):
- Remove prefixes like [Date Changed], (New Date), [Rescheduled], (Postponed) — but set date_changed: true if found
- Remove (Sold Out), [Sold Out] — but set sold_out: true if found
- Remove trailing venue references such as '...at The Big Top', '...Headlines the Riot HTX', '...at White Oak Music Hall', or any '...at/presented by/headlines [venue name]' suffix
- Keep opener/supporting act names in the title if they appear there (e.g. 'Headliner and Opener') 
  — do NOT strip them from name, put them in openers field AS WELL but keep the full title intact
- Do NOT otherwise modify or shorten the event name

Fields to extract:
- name: Clean event name/title (see TITLE CLEANUP above)
- start_date: YYYY-MM-DD format
- end_date: YYYY-MM-DD format 
- multi_day: true if this is a single continuous multi-day event (expo, festival, conference, multi-day sports tournament). false for single-day events AND for multi-night comedy/concert runs where the same act performs on separate nights.
- doors_time: HH:MM format (24-hour) or null
- start_time: HH:MM format (24-hour) or null
- end_time: HH:MM format (24-hour) or null
  DO NOT apply any timezone conversion. Keep times exactly as shown.
- venue: "{venue_name}" (see venue note below if present)
- city: "{city}"
- state: "{state}"
- price: Extract if mentioned, otherwise null
- ticket_url: Full URL to ticketing page if present, otherwise null
- event_url: Full URL to the event's own detail page if present (distinct from ticket URL), otherwise null
- description: Brief description
- genre: Music genre/category if discernible
- location: Specific room, stage, or area within the venue if mentioned (e.g. 'Main Stage', 'Upstairs', 'Lawn'), otherwise null
- event_type: Classify as one of: 'music', 'comedy', 'open_mic', 'happy_hour', 'performing_arts', 'arts', 'sports', 'civic', 'private_event', or 'other'. Use 'music' as default if unclear.
- sold_out: true if event is sold out, false otherwise
- date_changed: true if event has been rescheduled or date changed, false otherwise
- openers: Comma-separated list of opening acts if mentioned, otherwise null
- confidence: Object with field-level confidence scores (0-1)
{venue_note}
{"If parsing HTML, look in div classes, data attributes, and any structured elements containing event information." if is_html else ""}
Return ONLY valid JSON with an "events" array containing ALL events found."""
    user_prompt = f"Extract all events:\n\n{content}"
    return get_llm_response(system_prompt, user_prompt, llm=llm)

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
            
            # Try to extract time from ticket URL or other elements
            # Time might be in the aria-label or title
            time_match = None
            if ticket_link and ticket_link.get('aria-label'):
                time_match = re.search(r'(\d{1,2}):(\d{2})\s*(AM|PM)', ticket_link['aria-label'])
            
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

def scrape_venue(venue_key, mode='daily', llm='gpt4o-mini', dry_run=False):
    """Scrape a single venue with hash checking and mode support"""
    venue = VENUES[venue_key]

    print(f"\n{'='*60}")
    print(f"Scraping: {venue['name']} [{mode} mode]")
    print(f"{'='*60}")

    # Timely API venues — bypass Selenium + LLM entirely
    if venue.get('timely_calendar_id'):
        return scrape_timely_api(venue, mode)
    
    # SeeTickets venues — scrape with Selenium but parse with custom HTML parser, no LLM
    if venue.get('seetickets'):
        _, html = scrape_page(venue['url'], wait_time=venue.get('wait_time', 3),
                            scroll_count=venue.get('scroll_count', 1),load_more_id=venue.get('load_more_id'))
        return parse_seetickets_html(html, venue)
    
    # AJAX venues — fetch all paginated HTML fragments and parse with LLM in one go
    if venue.get('ajax_url'):
        return ajax_scrape(venue, mode)

    # Google Calendar ICS feed — bypass Selenium + LLM entirely
    if venue.get('ical_url'):
        return scrape_google_ics(venue, mode)

    debug = venue_key in ['white_oak']
    scroll_count = venue.get('scroll_count', 1)

    # Handle paginated venues
    if venue.get('paginated') and venue.get('page_param'):
        combined_text = ''
        current_url = venue['url']
        page_num = 1
        max_pages = 10  # Safety limit

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

            # Look for next page link
            soup = BeautifulSoup(html, 'html.parser')
            next_link = soup.find('a', id='moreshowsbtn')
            if next_link and next_link.get('href'):
                href = next_link['href']
                # Build full URL if relative
                if href.startswith('?'):
                    base = venue['url'].split('?')[0]
                    current_url = base + href
                else:
                    current_url = href
                page_num += 1
            else:
                break

        page_text = combined_text
        html = ''  # Not needed after pagination
        print(f"  ✓ Fetched {page_num} pages, {len(page_text)} total characters")

    else:
        page_text, html = scrape_page(
            venue['url'],
            wait_time=venue.get('wait_time', 3),
            debug=debug,
            scroll_count=scroll_count,
            load_more_id=venue.get('load_more_id')
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

    # Use custom parser for White Oak
    if venue_key == 'white_oak':
        events_data = parse_white_oak_html(html)
    else:
        char_limit = 100000
        venue_instruction = venue.get('venue_instruction', '')
        events_data = extract_events_with_llm_raw(
            page_text[:char_limit],
            venue['name'],
            venue['city'],
            venue['state'],
            is_html=False,
            venue_instruction=venue_instruction,
            llm=llm
        )
    
    events = events_data.get('events', [])
    print(f"  ✓ Found {len(events)} events")

    for event in events:
        event['venue_url'] = venue.get('venue_url') or venue.get('url', '')

    # Weekly mode: check for canceled events
    canceled_count = 0
    if mode == 'weekly':
        canceled_count = check_canceled_events(venue_key, events)

    # Update stored hash
    if not args.dry_run:
        update_stored_hash(venue_key, content_hash)

    return events, canceled_count

def scrape_all_venues(mode='daily', llm='gpt4o-mini', auto_approve=False):
    """Scrape all venues, save, and log stats per venue"""
    all_events = []
    for venue_key in VENUES.keys():
        try:
            events, canceled_count = scrape_venue(venue_key, mode=mode, llm=llm)
            all_events.extend(events)
            if events is not None:
                stats = save_to_database(events, mode=mode, auto_approve=auto_approve)
                log_scrape_stats(venue_key, VENUES[venue_key]['name'], mode, stats)
        except Exception as e:
            print(f"✗ Error scraping {VENUES[venue_key]['name']}: {e}")
    return all_events

def save_to_database(events, mode='daily', auto_approve=False):
    """Save events to database with exact and partial duplicate detection"""
    from difflib import SequenceMatcher
    dup_threshold_map = {v['name']: v.get('duplicate_threshold', 0.5) for v in VENUES.values()}
    auto_approve_map = {v['name']: v.get('auto_approve', False) for v in VENUES.values()}
    today = datetime.now().strftime('%Y-%m-%d')

    #Current behavior is to filter out past events before saving to DB, but consider revisiting before next year events become common on websites. LLM likely to be used to address.
    before = len(events)
    events = [e for e in events if e.get('start_date', '') >= today]
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

    inserted = 0
    skipped = 0
    flagged = 0
    updated = 0

    for event in events:
        try:
            # Exact duplicate check: name + start_date + venue + start_time
            c.execute(f'''SELECT id FROM events
                            WHERE name = {ph} AND start_date = {ph}
                            AND venue = {ph}
                            AND (start_time = {ph} OR (start_time IS NULL AND {ph} IS NULL))
                            AND status != 'canceled'
                            LIMIT 1''',
                        (event['name'], event.get('start_date'), event['venue'],
                        event.get('start_time'), event.get('start_time')))
            if c.fetchone():
                skipped += 1
                continue

            # Ticket URL match: same ticket_url means same event regardless of title/time drift.
            # Update mutable fields in place rather than inserting a new row.
            if event.get('ticket_url'):
                c.execute(f'''SELECT id, name, start_time, doors_time, end_time, sold_out, date_changed, openers, price
                                FROM events
                                WHERE ticket_url = {ph}
                                AND status NOT IN ('rejected', 'canceled')
                                LIMIT 1''',
                            (event['ticket_url'],))
                url_match = c.fetchone()
                if url_match:
                    ex_id, ex_name, ex_start, ex_doors, ex_end, ex_sold_out, ex_date_changed, ex_openers, ex_price = url_match
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
                    if event.get('date_changed', False) != ex_date_changed:
                        changes['date_changed'] = event.get('date_changed', False)
                    if event.get('openers') and event.get('openers') != ex_openers:
                        changes['openers'] = event['openers']
                    if event.get('price') and event.get('price') != ex_price:
                        changes['price'] = event['price']
                    if changes:
                        set_clause = ', '.join(f'{k} = {ph}' for k in changes)
                        c.execute(f'UPDATE events SET {set_clause} WHERE id = {ph}',
                                  list(changes.values()) + [ex_id])
                        label = f"{ex_name} → {event['name']}" if 'name' in changes else ex_name
                        print(f"  ↻ Updated ({', '.join(changes)}): {label}")
                        updated += 1
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
                skipped += 1
                continue

            # Partial duplicate check: same venue + start_date, similar name
            # Only check against pending/approved — excluding possible_duplicate prevents cascade chains
            c.execute(f'''SELECT id, name FROM events
                            WHERE venue = {ph} AND start_date = {ph}
                            AND status IN ('pending', 'approved') ''',
                        (event['venue'], event.get('start_date')))
            existing = c.fetchall()
            venue_auto_approve = auto_approve or auto_approve_map.get(event['venue'], False)
            status = 'approved' if venue_auto_approve else 'pending'
            duplicate_of_id = None
            event_type = event.get('event_type', 'music')
            visible = event_type in ('music', 'comedy', 'performing_arts', 'arts', 'sports')
            if not event.get('genre') and event_type == 'comedy':
                event['genre'] = 'Comedy'
            dup_threshold = dup_threshold_map.get(event['venue'], 0.5)
            for ex_id, ex_name in existing:
                similarity = SequenceMatcher(
                    None, event['name'].lower(), ex_name.lower()
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
                            event_type, visible, sold_out, date_changed, openers, duplicate_of_id)
                            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})''',
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
                        event.get('openers'), duplicate_of_id))
            inserted += 1
        except Exception as e:
            print(f"Error inserting {event['name']}: {e}")

    conn.commit()
    conn.close()
    print(f"  ✓ Inserted {inserted}, updated {updated}, skipped {skipped} duplicates, flagged {flagged} possible duplicates")
    return {
        'inserted': inserted,
        'updated': updated,
        'skipped': skipped,
        'flagged': flagged
    }

def log_scrape_stats(venue_key, venue_name, mode, stats, canceled_count=0):
    if not DATABASE_URL:
        return
    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s'
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Get current totals
    c.execute(f'''SELECT 
        COUNT(*) FILTER (WHERE status = 'approved') as approved,
        COUNT(*) FILTER (WHERE status = 'pending') as pending,
        COUNT(*) FILTER (WHERE status = 'rejected') as rejected
        FROM events WHERE venue = {ph}''', (venue_name,))
    row = c.fetchone()
    
    c.execute(f'''INSERT INTO scrape_stats 
        (scrape_date, venue, total_scraped, total_approved, total_rejected,
         new_events, canceled_events, total_pending, scrape_mode)
        VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})''',
        (today, venue_key,
         stats.get('inserted', 0) + stats.get('skipped', 0),
         row[0], row[2],
         stats.get('inserted', 0),
         canceled_count,
         row[1],
         mode))
    conn.commit()
    conn.close()

def detect_existing_duplicates(dry_run=False):
    """Scan the events table for possible duplicates and update their stads"""
    from difflib import SequenceMatcher
    from collections import defaultdict

    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'

    # Only scan active events — skip already-flagged, rejected, canceled
    c.execute('''SELECT id, name, start_date, venue FROM events
                 WHERE status IN ('pending', 'approved')
                 ORDER BY id ASC''')
    rows = c.fetchall()

    groups = defaultdict(list)
    for event_id, name, start_date, venue in rows:
        groups[(venue, start_date)].append((event_id, name))

    flagged = 0
    for (venue, start_date), group in groups.items():
        if len(group) < 2:
            continue
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                id_a, name_a = group[i]
                id_b, name_b = group[j]
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

def archive_past_events(buffer_days=1):
    """Move approved past events to past_events table"""
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
                 openers, event_url
                 FROM events
                 WHERE status IN ('approved', 'possible_duplicate', 'rejected', 'canceled') AND start_date < %s''', (cutoff,))
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
                          date_changed, openers, event_url)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                      (row[1], row[2], row[3], row[4], row[5], row[6], row[7],
                       row[8], row[9], row[10], row[11], row[12],
                       row[13], row[14], row[15], row[16], row[17], row[18], row[19],
                       row[20], row[21], archived_at, row[22], row[23], row[24],
                       row[25], row[26]))
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
    parser.add_argument('--llm', choices=['gpt4o', 'gpt4o-mini', 'claude', 'groq'], default='gpt4o-mini',
                        help='LLM provider to use for extraction')
    parser.add_argument('--dry-run', action='store_true',
                        help='Extract and print events without writing to DB')
    parser.add_argument('--detect-duplicates', action='store_true',
                        help='Scan existing events table for duplicates and flag them')
    args = parser.parse_args()

    init_db()

    if args.detect_duplicates:
        print(f"=== Duplicate Detection {'[DRY RUN] ' if args.dry_run else ''}===\n")
        detect_existing_duplicates(dry_run=args.dry_run)
        exit(0)

    print(f"=== Houston Music Events Scraper [{args.mode} mode] ===\n")

    # Archive past events on weekly run
    if args.mode == 'weekly':
        archive_past_events(buffer_days=1)

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
                stats = save_to_database(events, mode=args.mode, auto_approve=args.auto_approve)
                log_scrape_stats(args.venue, VENUES[args.venue]['name'], args.mode, stats)
    else:
        all_events = scrape_all_venues(mode=args.mode, llm=args.llm, auto_approve=args.auto_approve)
        if args.dry_run:
            print(f"\n{'='*60}")
            print(f"DRY RUN — {len(all_events)} events extracted, not saved")
            for e in all_events:
                print(f"  {e.get('start_date')} | {e.get('start_time')} | {e.get('venue')} | {e.get('location')} | {e.get('name')}")
            print(f"{'='*60}")
    print(f"✓ Scrape complete")
