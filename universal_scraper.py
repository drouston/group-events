import os
import json
import time
import sqlite3
from datetime import datetime

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


def save_event_to_db(event):
    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'

    # Skip if exact duplicate already exists
    c.execute(f'''SELECT id FROM events
                         WHERE name = {ph} AND date = {ph}
                         AND venue = {ph} AND start_time = {ph}
                         LIMIT 1''',
                      (event['name'], event['date'], event['venue'],
                       event.get('start_time')))
    if c.fetchone():
        print(f"  Duplicate skipped: {event['name']}")
        conn.close()
        return False

    now = datetime.now().isoformat()
    c.execute(f'''INSERT INTO events
                 (name, date, doors_time, start_time, venue, city, state,
                  price, ticket_url, description, genre, confidence, notes,
                  status, created_at, approved_at)
                 VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})''',
              (event['name'], event['date'], event.get('doors_time'),
               event.get('start_time'), event['venue'], event.get('city', ''),
               event.get('state', ''), event.get('price'), event.get('ticket_url'),
               event.get('description'), event.get('genre'),
               json.dumps(event.get('confidence', {})),
               event.get('notes', ''), 'approved', now, now))
    conn.commit()
    conn.close()
    return True

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
        "url": "https://events.timely.fun/uggqcowo/tile?categories=677491865&nofilters=1",
        "city": "Houston",
        "state": "TX",
        "wait_time": 10,
        "scroll_count": 1,
        "venue_instruction": "This page covers two venues. Use venue 'Continental Club Houston' for most events. Use venue 'Big Top Charlies Shoeshine Lounge' for events described as being at 'The Big Top' in the title or description."
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
        "url": "https://improvtx.com/calendar/-/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 1,
        "paginated": True,
        "next_page_selector": "#moreshowsbtn",
        "page_param": "start",
        "venue_instruction": "This page covers multiple Texas Improv locations. Only extract events for 'Houston Improv' — ignore Addison Improv, Arlington Improv, LOL San Antonio and any other locations. Set venue to 'Improv Houston' and city to 'Houston' for all extracted events. CRITICAL: When an event spans multiple nights (e.g. 'Apr 17-18 Fri-Sat' or 'Apr 24-26 Fri-Sun'), you MUST create a SEPARATE event entry for EACH individual night. For example 'Apr 17-18' = two events: one on 2026-04-17 AND one on 2026-04-18.",
    },
    "riot_comedy": {
        "name": "The Riot Comedy Club",
        "url": "https://theriothtx.com/headlining-comedians-at-the-riot-comedy-club-in-houston-texas/",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "scroll_count": 2
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
    """Compare scraped events against DB future events, flag missing as canceled"""
    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'
    venue_name = VENUES[venue_key]['name']
    today = datetime.now().strftime('%Y-%m-%d')

    # Get all future approved/pending events for this venue
    c.execute(f'''SELECT id, name, date FROM events
                 WHERE venue = {ph} AND date >= {ph}
                 AND status IN ('approved', 'pending')''',
              (venue_name, today))
    db_events = c.fetchall()

    # Build set of scraped event identifiers
    scraped_set = set(
        (e['name'].strip().lower(), e['date'])
        for e in scraped_events
    )

    canceled_count = 0
    for db_id, db_name, db_date in db_events:
        if (db_name.strip().lower(), db_date) not in scraped_set:
            c.execute(f'''UPDATE events SET status = 'canceled'
                         WHERE id = {ph}''', (db_id,))
            canceled_count += 1
            print(f"  ⚠ Flagged as canceled: {db_name} on {db_date}")

    conn.commit()
    conn.close()
    print(f"  {canceled_count} events flagged as canceled for {venue_name}")

def scrape_page(url, wait_time=3, debug=False, scroll_count=1):
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
        
        print(f"Extracted {len(page_text)} characters")
        print(f"HTML length: {len(html)} characters")  # Show raw HTML size
        
        return page_text, html  # Return both
        
    finally:
        driver.quit()

def get_llm_response(system_prompt, user_prompt, llm='gpt4o-mini'):
    """Route LLM request to the appropriate provider"""
    if llm == 'gpt4o':
        client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
        response = client.chat.completions.create(
            model="gpt-4o",
            temperature=0,
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
        char_limit = 20000
    
    system_prompt = f"""You are an expert at extracting structured event data from venue websites.
Extract ALL events from the provided text into a JSON array. For each event:
- name: Full event name/title
- date: YYYY-MM-DD format (use 2026 for dates without year)
- doors_time: HH:MM format (24-hour) or null (look for times like "6:30pm", "7:00pm", "Doors: 7pm", etc.)
- start_time: HH:MM format (24-hour) (24-hour, Central Time - DO NOT CONVERT TIMEZONES) or null 
  IMPORTANT: If you see "8:00 PM" or "8pm", output "20:00" NOT "02:00"
  If you see "7:30 PM", output "19:30" NOT "01:30"
  DO NOT apply any timezone conversion. Keep times exactly as shown on the website.
- venue: "{venue_name}"
- city: "{city}"
- state: "{state}"
- price: Extract if mentioned, otherwise null
- ticket_url: Full URL if present
- description: Brief description if available
- genre: Music genre/category if discernible
- location: Specific room, stage, or area within the venue if mentioned (e.g. 'Main Stage', 'Upstairs', 'Lawn'), otherwise null
- event_type: Classify the event as one of: 'music', 'comedy', 'open_mic', 'happy_hour', 'private_event', or 'other'. Use 'music' as default if unclear.
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

    system_prompt = f"""You are an expert at extracting structured event data from venue websites.
Extract ALL events from the provided {content_type} into a JSON array. For each event:
- name: Full event name/title
- date: YYYY-MM-DD format (use 2026 for dates without year)
- doors_time: HH:MM format (24-hour) or null
- start_time: HH:MM format (24-hour) or null
  IMPORTANT: If you see "8:00 PM" or "8pm", output "20:00" NOT "02:00"
  If you see "7:30 PM", output "19:30" NOT "01:30"
  DO NOT apply any timezone conversion. Keep times exactly as shown on the website.
- venue: "{venue_name}" (see venue note below if present)
- city: "{city}"
- state: "{state}"
- price: Extract if mentioned
- ticket_url: Full URL if present
- description: Brief description
- genre: Music genre/category if discernible
- location: Specific room, stage, or area within the venue if mentioned (e.g. 'Main Stage', 'Upstairs', 'Lawn'), otherwise null
- event_type: Classify the event as one of: 'music', 'comedy', 'open_mic', 'happy_hour', 'private_event', or 'other'. Use 'music' as default if unclear.
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
            
            # Assume 2026 for now (you can make this smarter)
            date = f"2026-{month_num}-{day.zfill(2)}"
            
            # Extract venue (Upstairs/Downstairs/Lawn)
            venue_elem = section.find('span', class_='tw-venue-name')
            venue_detail = venue_elem.text.strip() if venue_elem else "White Oak Music Hall"
            
            # Extract ticket URL
            ticket_link = section.find('a', class_='tw-buy-tix-btn')
            ticket_url = ticket_link['href'] if ticket_link else None
            
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
                'date': date,
                'doors_time': None,
                'start_time': start_time,
                'venue': venue_detail,
                'city': 'Houston',
                'state': 'TX',
                'price': None,
                'ticket_url': ticket_url,
                'description': None,
                'genre': None,
                'confidence': {
                    'name': 1.0,
                    'date': 1.0,
                    'time': 0.8 if start_time else 0.0,
                    'price': 0.0,
                    'genre': 0.0
                }
            })
            
        except Exception as e:
            print(f"Error parsing event: {e}")
            continue
    
    return {'events': events}

def scrape_venue(venue_key, mode='daily', llm='gpt4o-mini'):
    """Scrape a single venue with hash checking and mode support"""
    venue = VENUES[venue_key]

    print(f"\n{'='*60}")
    print(f"Scraping: {venue['name']} [{mode} mode]")
    print(f"{'='*60}")

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
                scroll_count=scroll_count
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
            scroll_count=scroll_count
        )

    # Hash check — skip LLM if content unchanged (not for onboard mode)
    content_hash = get_content_hash(page_text)
    stored_hash = get_stored_hash(venue_key)
    if mode != 'onboard' and content_hash == stored_hash:
        print(f"  ↷ No changes detected, skipping LLM")
        return []
    print(f"  ✓ Content changed, processing with LLM")

    # Use custom parser for White Oak
    if venue_key == 'white_oak':
        events_data = parse_white_oak_html(html)
    else:
        char_limit = 60000 if venue.get('paginated') else 20000
        events_data = extract_events_with_llm_raw(
            page_text[:char_limit],
            venue['name'],
            venue['city'],
            venue['state'],
            is_html=False,
            venue_instruction=venue.get('venue_instruction'),
            llm=llm
        )

    events = events_data.get('events', [])
    print(f"  ✓ Found {len(events)} events")

    # Weekly mode: check for canceled events
    if mode == 'weekly':
        check_canceled_events(venue_key, events)

    # Update stored hash
    update_stored_hash(venue_key, content_hash)

    return events

def scrape_all_venues(mode='daily', llm='gpt4o-mini'):
    """Scrape all venues and combine results"""
    all_events = []

    for venue_key in VENUES.keys():
        try:
            events = scrape_venue(venue_key, mode=mode, llm=llm)
            all_events.extend(events)
        except Exception as e:
            print(f"✗ Error scraping {VENUES[venue_key]['name']}: {e}")

    return all_events

def save_to_database(events, mode='daily', auto_approve=False):
    """Save events to database with exact and partial duplicate detection"""
    from difflib import SequenceMatcher
    today = datetime.now().strftime('%Y-%m-%d')

    # Onboard mode: filter out past events
    if mode == 'onboard':
        before = len(events)
        events = [e for e in events if e.get('date', '') >= today]
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

    for event in events:
        try:
            # Exact duplicate check: venue + name + date + start_time
            c.execute(f'''SELECT id FROM events
                         WHERE name = {ph} AND date = {ph}
                         AND venue = {ph} AND start_time = {ph}
                         LIMIT 1''',
                      (event['name'], event['date'], event['venue'],
                       event.get('start_time')))

            if c.fetchone():
                skipped += 1
                print(f"  ⊘ Exact duplicate skipped: {event['name']}")
                continue

            # Partial duplicate check: same venue + date, similar name (>80%)
            c.execute(f'''SELECT id, name FROM events
                         WHERE venue = {ph} AND date = {ph}
                         AND status NOT IN ('rejected', 'canceled')''',
                      (event['venue'], event['date']))
            existing = c.fetchall()

            status = 'approved' if auto_approve else 'pending'
            event_type = event.get('event_type', 'music')
            visible = event_type in ('music', 'comedy')
            for ex_id, ex_name in existing:
                similarity = SequenceMatcher(
                    None, event['name'].lower(), ex_name.lower()
                ).ratio()
                if similarity >= 0.8:
                    status = 'possible_duplicate'
                    print(f"  ⚠ Possible duplicate ({int(similarity*100)}% match): "
                          f"{event['name']} ~ {ex_name}")
                    flagged += 1
                    break

            # Insert event
            c.execute(f'''INSERT INTO events
                         (name, date, doors_time, start_time, venue, location, city, state,
                          price, ticket_url, description, genre, confidence, status, created_at,
                          event_type, visible)
                         VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})''',
                      (event['name'], event['date'], event.get('doors_time'),
                       event.get('start_time'), event['venue'], event.get('location'),
                       event.get('city', ''), event.get('state', ''),
                       event.get('price'), event.get('ticket_url'),
                       event.get('description'), event.get('genre'),
                       json.dumps(event.get('confidence', {})),
                       status, datetime.now().isoformat(),
                       event_type, visible))
            inserted += 1

        except Exception as e:
            print(f"Error inserting {event['name']}: {e}")

    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"✓ Inserted {inserted} new events")
    print(f"⚠ Flagged {flagged} possible duplicates")
    print(f"⊘ Skipped {skipped} exact duplicates")
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
    args = parser.parse_args()

    print(f"=== Houston Music Events Scraper [{args.mode} mode] ===\n")

    init_db()

    if args.venue:
        if args.venue not in VENUES:
            print(f"✗ Unknown venue key: {args.venue}")
            print(f"  Available: {', '.join(VENUES.keys())}")
        else:
            events = scrape_venue(args.venue, mode=args.mode)
            if args.dry_run:
                print(f"\n{'='*60}")
                print(f"DRY RUN — {len(events)} events extracted, not saved")
                for e in events:
                    print(f"  {e.get('date')} | {e.get('start_time')} | {e.get('venue')} | {e.get('location')} | {e.get('name')}")
                print(f"{'='*60}")
            else:
                if args.dry_run:
                    print(f"\n{'='*60}")
                    print(f"DRY RUN — {len(events)} events extracted, not saved")
                    for e in events:
                        print(f"  {e.get('date')} | {e.get('start_time')} | {e.get('venue')} | {e.get('location')} | {e.get('name')}")
                    print(f"{'='*60}")
                else:
                    save_to_database(events, mode=args.mode, auto_approve=args.auto_approve)
    else:
        all_events = scrape_all_venues(mode=args.mode, llm=args.llm)
        if args.dry_run:
            print(f"\n{'='*60}")
            print(f"DRY RUN — {len(all_events)} events extracted, not saved")
            for e in all_events:
                print(f"  {e.get('date')} | {e.get('start_time')} | {e.get('venue')} | {e.get('location')} | {e.get('name')}")
            print(f"{'='*60}")
        else:
            save_to_database(all_events, mode=args.mode, auto_approve=args.auto_approve)

    print(f"\n{'='*60}")
    print(f"✓ Scrape complete")
    print(f"{'='*60}")