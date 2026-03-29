import os
import json
import time
import sqlite3
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from openai import OpenAI

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
    else:
        c.execute('''CREATE TABLE IF NOT EXISTS events
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, date TEXT,
                      doors_time TEXT, start_time TEXT, venue TEXT,
                      city TEXT, state TEXT, price TEXT, ticket_url TEXT,
                      description TEXT, genre TEXT, confidence TEXT,
                      notes TEXT, status TEXT DEFAULT 'pending',
                      created_at TEXT, approved_at TEXT)''')
    conn.commit()
    conn.close()


def save_event_to_db(event):
    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'

    # Skip if exact duplicate already exists
    c.execute(f'''SELECT id FROM events
                 WHERE name = {ph} AND date = {ph} AND venue = {ph}
                 AND status = 'approved' LIMIT 1''',
              (event['name'], event['date'], event['venue']))
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
        "scroll_count": 10  # Scroll 10 times to load all events
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
        "url": "https://continentalclub.com/houston",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5,
        "venue_instruction": "This page covers two venues. Use venue 'Continental Club Houston' for most events. Use venue 'Big Top Charlies Shoeshine Lounge' for events described as being at 'The Big Top' in the title or description."
    },
    "woodland_pavilion": {
        "name": "Cynthia Woods Mitchell Pavilion",
        "url": "https://www.woodlandscenter.org/events",
        "city": "The Woodlands",
        "state": "TX",
        "wait_time": 6
    },
    "house_of_blues_main": {
        "name": "House of Blues Houston",
        "url": "https://houston.houseofblues.com/shows",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5
    },
    "house_of_blues_peacock": {
        "name": "House of Blues Houston - The Bronze Peacock",
        "url": "https://houston.houseofblues.com/shows/rooms/the-bronze-peacock",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5
    },
    "house_of_blues_foundation": {
        "name": "House of Blues Houston - The Foundation Room",
        "url": "https://houston.houseofblues.com/shows/rooms/foundation-room",
        "city": "Houston",
        "state": "TX",
        "wait_time": 5
    },
    "smart_financial": {
        "name": "Smart Financial Centre",
        "url": "https://us.atgtickets.com/venues/smart-financial-centre/whats-on/us/concert/",
        "city": "Sugar Land",
        "state": "TX",
        "wait_time": 6,
        "scroll_count": 3
    }
}

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
        page_text = soup.get_text(separator='\n', strip=True)
        
        print(f"Extracted {len(page_text)} characters")
        print(f"HTML length: {len(html)} characters")  # Show raw HTML size
        
        return page_text, html  # Return both
        
    finally:
        driver.quit()

def extract_events_with_llm(page_text, venue_name, city, state):
    """Extract events using GPT"""
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', 'your-api-key-here')
    client = OpenAI(api_key=OPENAI_API_KEY)

    # White Oak needs much more text
    if "White Oak" in venue_name:
        char_limit = 100000  # GPT-4o can handle this
    else:
        char_limit = 20000
    
    # ... rest of function stays the same
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": f"""You are an expert at extracting structured event data from venue websites.

Extract ALL events from the provided text into a JSON array. For each event:
- name: Full event name/title
- date: YYYY-MM-DD format (use 2026 for dates without year)
- doors_time: HH:MM format (24-hour) or null
- start_time: HH:MM format (24-hour) or null (look for times like "6:30pm", "7:00pm", "Doors: 7pm", etc.)
- venue: "{venue_name}"
- city: "{city}"
- state: "{state}"
- price: Extract if mentioned, otherwise null
- ticket_url: Full URL if present
- description: Brief description if available
- genre: Music genre/category if discernible
- confidence: Object with field-level confidence scores (0-1)

IMPORTANT: Look carefully for event times. They may appear as:
- "6:30pm" or "7:00pm"
- "Doors: 7pm, Show: 8pm"
- Times listed separately from event names
- Pay special attention to any numbers followed by "pm" or "am"

Return ONLY valid JSON with an "events" array."""
            },
            {
                "role": "user",
                "content": f"Extract events from this page:\n\n{page_text[:char_limit]}"
            }
        ],
        response_format={"type": "json_object"}
    )
    
    return json.loads(response.choices[0].message.content)

def extract_events_with_llm_raw(content, venue_name, city, state, is_html=False, venue_instruction=None):
    """Extract events using GPT from text or HTML"""
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', 'your-api-key-here')
    client = OpenAI(api_key=OPENAI_API_KEY)

    content_type = "HTML code" if is_html else "text"
    venue_note = f"\n\nVENUE NOTE: {venue_instruction}" if venue_instruction else f'\n- venue: "{venue_name}"'

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": f"""You are an expert at extracting structured event data from venue websites.

Extract ALL events from the provided {content_type} into a JSON array. For each event:
- name: Full event name/title
- date: YYYY-MM-DD format (use 2026 for dates without year)
- doors_time: HH:MM format (24-hour) or null
- start_time: HH:MM format (24-hour) or null
- venue: "{venue_name}" (see venue note below if present)
- city: "{city}"
- state: "{state}"
- price: Extract if mentioned
- ticket_url: Full URL if present
- description: Brief description
- genre: Music genre if discernible
- confidence: Field-level confidence scores
{venue_note}
{"If parsing HTML, look in div classes, data attributes, and any structured elements containing event information." if is_html else ""}

Return ONLY valid JSON with an "events" array containing ALL events found."""
            },
            {
                "role": "user",
                "content": f"Extract all events:\n\n{content}"
            }
        ],
        response_format={"type": "json_object"}
    )

    return json.loads(response.choices[0].message.content)

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

def scrape_venue(venue_key):
    """Scrape a single venue"""
    venue = VENUES[venue_key]
    
    print(f"\n{'='*60}")
    print(f"Scraping: {venue['name']}")
    print(f"{'='*60}")
    
    debug = venue_key in ['white_oak']
    scroll_count = venue.get('scroll_count', 1)
    
    page_text, html = scrape_page(
        venue['url'], 
        wait_time=venue.get('wait_time', 3),
        debug=debug,
        scroll_count=scroll_count
    )
    
    # Use custom parser for White Oak
    if venue_key == 'white_oak':
        events_data = parse_white_oak_html(html)
    else:
        events_data = extract_events_with_llm_raw(
            page_text[:20000],
            venue['name'],
            venue['city'],
            venue['state'],
            is_html=False,
            venue_instruction=venue.get('venue_instruction')
        )
    
    events = events_data.get('events', [])
    print(f"✓ Found {len(events)} events")
    
    return events

def scrape_all_venues():
    """Scrape all venues and combine results"""
    all_events = []
    
    for venue_key in VENUES.keys():
        try:
            events = scrape_venue(venue_key)
            all_events.extend(events)
        except Exception as e:
            print(f"✗ Error scraping {VENUES[venue_key]['name']}: {e}")
    
    return all_events

if __name__ == "__main__":
    print("=== Houston Music Events Scraper ===\n")

    init_db()

    all_events = scrape_all_venues()

    saved = 0
    skipped = 0
    for event in all_events:
        if save_event_to_db(event):
            saved += 1
        else:
            skipped += 1

    print(f"\n{'='*60}")
    print(f"✓ Total scraped:  {len(all_events)}")
    print(f"✓ Saved to DB:    {saved}")
    print(f"✓ Duplicates skipped: {skipped}")
    print(f"{'='*60}")