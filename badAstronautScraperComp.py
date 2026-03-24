import requests
from bs4 import BeautifulSoup
import json
from openai import OpenAI
from anthropic import Anthropic

def scrape_bad_astronaut():
    url = "https://www.prekindle.com/events/bad-astronaut-brewing-co"
    
    # Fetch the page
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract text content
    page_text = soup.get_text(separator='\n', strip=True)
    
    return page_text

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

Handle these date formats:
- "Friday March 13" → use current/next occurrence
- "Doors 7:00pm, Start 8:00pm" → extract both times
- Convert 12-hour to 24-hour time

Return ONLY valid JSON with an "events" array."""
            },
            {
                "role": "user",
                "content": f"Extract events from this page:\n\n{html_text[:15000]}"  # Limit for token usage
            }
        ],
        response_format={"type": "json_object"}
    )
    
    return json.loads(response.choices[0].message.content)

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
    
    print("\nExtracting with GPT...")
    gpt_events = extract_events_gpt(page_text)
    
    print("\nExtracting with Claude...")
    claude_events = extract_events_claude(page_text)
    
    # Compare results
    print(f"\nGPT found {len(gpt_events.get('events', []))} events")
    print(f"Claude found {len(claude_events.get('events', []))} events")
    
    # Show first event from each
    if gpt_events.get('events'):
        print("\nGPT first event:", json.dumps(gpt_events['events'][0], indent=2))
    
    if claude_events.get('events'):
        print("\nClaude first event:", json.dumps(claude_events['events'][0], indent=2))