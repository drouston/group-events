import requests
from bs4 import BeautifulSoup
import json
from openai import OpenAI
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
# or: from anthropic import Anthropic

#requests based html scraper - not working for Bad Astronaut, likely due to dynamic content loading or anti-scraping measures. Will switch to Selenium for better results.
"""def scrape_bad_astronaut():
    url = "https://www.prekindle.com/events/bad-astronaut-brewing-co"
    
    print("headers")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print("fetching page...")
    # Fetch the page
    
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Save raw HTML to file
    with open('raw_page.html', 'w', encoding='utf-8') as f:
        f.write(response.text)
    print("Saved raw HTML to raw_page.html")

    # Extract text content
    page_text = soup.get_text(separator='\n', strip=True)

    # Right after scrape_bad_astronaut()
    print("First 1000 characters of scraped text:")
    print(page_text[:1000])
    print("\n---\n")

    return page_text"""

#selenium based scraper - should handle dynamic content and anti-scraping better, but is more complex to set up and run. Will use headless Chrome for this.
def scrape_bad_astronaut():
    print("Launching browser...")
    
    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Run without opening browser window
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        url = "https://www.prekindle.com/events/bad-astronaut-brewing-co"
        driver.get(url)
        
        # Wait for events to load
        print("Waiting for page to load...")
        time.sleep(3)
        
        # Get page source after JavaScript has run
        html = driver.page_source
        
        # Save it
        with open('raw_page.html', 'w', encoding='utf-8') as f:
            f.write(html)
        print("Saved raw HTML")
        
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text(separator='\n', strip=True)
        
        print(f"Extracted {len(page_text)} characters")
        
        return page_text
        
    finally:
        driver.quit()

def extract_events_gpt(html_text):
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', 'your-api-key-here')
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": """You are an expert at extracting structured event data from venue websites.

Extract ALL events from the provided text into a JSON array. For each event:
- name: Full event name/title
- date: YYYY-MM-DD format
- doors_time: HH:MM format (24-hour) or null
- start_time: HH:MM format (24-hour) or null
- venue: "Bad Astronaut Brewing Co."
- city: "Houston"
- state: "TX"
- price: Extract if mentioned, otherwise null
- ticket_url: Full URL if present
- description: Brief description if available
- genre: Music genre/category if discernible
- confidence: Object with field-level confidence scores (0-1) for each field. 1.0 = certain, 0.5 = uncertain, 0.0 = missing/guessed

Example confidence object:
{
  "name": 1.0,
  "date": 1.0,
  "time": 0.8,
  "price": 0.0,
  "genre": 0.6
}

Handle these date formats:
- "Friday March 13" → use year 2026
- "Doors 7:00pm, Start 8:00pm" → extract both times
- Convert 12-hour to 24-hour time

Return ONLY valid JSON with an "events" array."""
            },
            {
                "role": "user",
                "content": f"Extract events from this page:\n\n{html_text[:15000]}"
            }
        ],
        response_format={"type": "json_object"}
    )
    
    return json.loads(response.choices[0].message.content)



# Run it
if __name__ == "__main__":
    print("Scraping Bad Astronaut...")
    page_text = scrape_bad_astronaut()
    
    print("\nExtracting with GPT...")
    gpt_events = extract_events_gpt(page_text)
    
    # Compare results
    print(f"\nGPT found {len(gpt_events.get('events', []))} events")
    
    # Show first event from each
    if gpt_events.get('events'):
        print("\nGPT first event:", json.dumps(gpt_events['events'][0], indent=2))
    
# Save results
with open('gpt_events.json', 'w') as f:
    json.dump(gpt_events, f, indent=2)

print("\nSaved to gpt_events.json")