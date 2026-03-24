import requests
from bs4 import BeautifulSoup
import json
from anthropic import Anthropic

def scrape_bad_astronaut():
    url = "https://www.prekindle.com/events/bad-astronaut-brewing-co"
    
    # Fetch the page
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract text content
    page_text = soup.get_text(separator='\n', strip=True)
    
    return page_text

def extract_events_claude(html_text):
    from anthropic import Anthropic
    
    client = Anthropic(api_key="your-api-key")
    
    message = client.messages.create(
        model="claude-sonnet-4.5",
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": f"""You are an expert at extracting structured event data from venue websites.

Extract ALL events from the provided text into a JSON object with an "events" array.

For each event include:
- name: Full event name/title
- date: YYYY-MM-DD format
- doors_time: HH:MM 24-hour format or null
- start_time: HH:MM 24-hour format or null  
- venue: "Bad Astronaut Brewing Co."
- city: "Houston"
- state: "TX"
- price: If mentioned, otherwise null
- ticket_url: Full URL if present
- description: Brief description if available
- genre: Music genre if discernible

Current date: March 13, 2026

Handle formats like:
- "Friday March 13" 
- "Doors 7:00pm, Start 8:00pm" 
- Convert PM/AM to 24-hour

Here is the page content:

{html_text[:15000]}

Return ONLY valid JSON."""
            }
        ]
    )
    
    return json.loads(message.content[0].text)

# Run it
if __name__ == "__main__":
    print("Scraping Bad Astronaut...")
    page_text = scrape_bad_astronaut()
    
    print("\nExtracting with Claude...")
    claude_events = extract_events_claude(page_text)
    
    # Compare results
    print(f"Claude found {len(claude_events.get('events', []))} events")
    
    # Show first event from each
    if claude_events.get('events'):
        print("\nClaude first event:", json.dumps(claude_events['events'][0], indent=2))