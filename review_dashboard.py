from flask import Flask, render_template, request, jsonify
import json
import sqlite3
import os
from datetime import datetime, timedelta
from difflib import SequenceMatcher

app = Flask(__name__)

# Database configuration - use PostgreSQL if DATABASE_URL exists (production), else SQLite (local)
DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db_connection():
    """Get database connection - PostgreSQL in production, SQLite locally"""
    if DATABASE_URL:
        # PostgreSQL connection
        import psycopg2
        # Railway provides postgres:// but psycopg2 needs postgresql://
        db_url = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
        return psycopg2.connect(db_url)
    else:
        # SQLite connection (local development)
        return sqlite3.connect('events.db')

def init_db():
    """Initialize database tables"""
    conn = get_db_connection()
    c = conn.cursor()
    
    if DATABASE_URL:
        # PostgreSQL syntax
        c.execute('''CREATE TABLE IF NOT EXISTS events
                     (id SERIAL PRIMARY KEY,
                      name TEXT,
                      date TEXT,
                      doors_time TEXT,
                      start_time TEXT,
                      venue TEXT,
                      city TEXT,
                      state TEXT,
                      price TEXT,
                      ticket_url TEXT,
                      description TEXT,
                      genre TEXT,
                      confidence TEXT,
                      notes TEXT,
                      status TEXT DEFAULT 'pending',
                      created_at TEXT,
                      approved_at TEXT)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS scrape_stats
                     (id SERIAL PRIMARY KEY,
                      scrape_date TEXT,
                      venue TEXT,
                      total_scraped INTEGER,
                      total_approved INTEGER,
                      total_rejected INTEGER)''')
    else:
        # SQLite syntax
        c.execute('''CREATE TABLE IF NOT EXISTS events
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      name TEXT,
                      date TEXT,
                      doors_time TEXT,
                      start_time TEXT,
                      venue TEXT,
                      city TEXT,
                      state TEXT,
                      price TEXT,
                      ticket_url TEXT,
                      description TEXT,
                      genre TEXT,
                      confidence TEXT,
                      notes TEXT,
                      status TEXT DEFAULT 'pending',
                      created_at TEXT,
                      approved_at TEXT)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS scrape_stats
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      scrape_date TEXT,
                      venue TEXT,
                      total_scraped INTEGER,
                      total_approved INTEGER,
                      total_rejected INTEGER)''')
    
    conn.commit()
    conn.close()

# Load events from scraper output
def load_pending_events():
    try:
        with open('gpt_events.json', 'r') as f:
            data = json.load(f)
            return data.get('events', [])
    except FileNotFoundError:
        return []

# Check for duplicates
def find_duplicates(event_name, event_date, event_venue):
    conn = get_db_connection()
    c = conn.cursor()
    
    # Check exact match
    if DATABASE_URL:
        c.execute('''SELECT id, name, date, venue FROM events 
                     WHERE status = 'approved' 
                     AND date = %s AND venue = %s''', (event_date, event_venue))
    else:
        c.execute('''SELECT id, name, date, venue FROM events 
                     WHERE status = 'approved' 
                     AND date = ? AND venue = ?''', (event_date, event_venue))
    
    exact_matches = []
    similar_matches = []
    
    for row in c.fetchall():
        existing_name = row[1]
        similarity = SequenceMatcher(None, event_name.lower(), existing_name.lower()).ratio()
        
        if similarity > 0.9:
            exact_matches.append({
                'id': row[0],
                'name': existing_name,
                'similarity': similarity
            })
        elif similarity > 0.7:
            similar_matches.append({
                'id': row[0],
                'name': existing_name,
                'similarity': similarity
            })
    
    conn.close()
    
    return {
        'exact': exact_matches,
        'similar': similar_matches
    }

# Calculate overall confidence score
def calculate_confidence(event):
    if not event.get('confidence'):
        return 0.5  # Default medium confidence
    
    conf = event['confidence']
    scores = []
    
    # Weight important fields more
    if 'name' in conf:
        scores.append(conf['name'] * 2)  # Name is critical
    if 'date' in conf:
        scores.append(conf['date'] * 2)  # Date is critical
    if 'time' in conf:
        scores.append(conf.get('time', 0.5))
    if 'genre' in conf:
        scores.append(conf.get('genre', 0.5))
    
    return sum(scores) / len(scores) if scores else 0.5

# Save event to database
def save_event(event_data):
    conn = get_db_connection()
    c = conn.cursor()
    
    # Check for duplicates first
    if DATABASE_URL:
        c.execute('''SELECT id FROM events 
                     WHERE name = %s AND date = %s AND venue = %s AND status = 'approved' 
                     LIMIT 1''', 
                  (event_data['name'], event_data['date'], event_data['venue']))
    else:
        c.execute('''SELECT id FROM events 
                     WHERE name = ? AND date = ? AND venue = ? AND status = 'approved' 
                     LIMIT 1''', 
                  (event_data['name'], event_data['date'], event_data['venue']))
    
    if c.fetchone():
        print(f"Duplicate prevented: {event_data['name']} on {event_data['date']}")
        conn.close()
        return  # Already exists, don't insert
    
    # Insert if not duplicate
    if DATABASE_URL:
        c.execute('''INSERT INTO events 
                     (name, date, doors_time, start_time, venue, city, state, 
                      price, ticket_url, description, genre, confidence, notes, status, created_at, approved_at)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                  (event_data['name'], event_data['date'], event_data.get('doors_time'),
                   event_data.get('start_time'), event_data['venue'], event_data['city'],
                   event_data['state'], event_data.get('price'), event_data.get('ticket_url'),
                   event_data.get('description'), event_data.get('genre'),
                   json.dumps(event_data.get('confidence', {})),
                   event_data.get('notes', ''),
                   'approved',
                   datetime.now().isoformat(), datetime.now().isoformat()))
    else:
        c.execute('''INSERT INTO events 
                     (name, date, doors_time, start_time, venue, city, state, 
                      price, ticket_url, description, genre, confidence, notes, status, created_at, approved_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (event_data['name'], event_data['date'], event_data.get('doors_time'),
                   event_data.get('start_time'), event_data['venue'], event_data['city'],
                   event_data['state'], event_data.get('price'), event_data.get('ticket_url'),
                   event_data.get('description'), event_data.get('genre'),
                   json.dumps(event_data.get('confidence', {})),
                   event_data.get('notes', ''),
                   'approved',
                   datetime.now().isoformat(), datetime.now().isoformat()))
    
    conn.commit()
    conn.close()

@app.route('/published')
def published_events():
    """View and edit already published events"""
    conn = get_db_connection()
    c = conn.cursor()
    
    if DATABASE_URL:
        c.execute('''SELECT id, name, date, doors_time, start_time, venue, city, state,
                     price, ticket_url, description, genre, notes
                     FROM events 
                     WHERE status = 'approved'
                     ORDER BY date ASC''')
    else:
        c.execute('''SELECT id, name, date, doors_time, start_time, venue, city, state,
                     price, ticket_url, description, genre, notes
                     FROM events 
                     WHERE status = 'approved'
                     ORDER BY date ASC''')
    
    rows = c.fetchall()
    events = []
    
    for row in rows:
        events.append({
            'id': row[0],
            'name': row[1],
            'date': row[2],
            'doors_time': row[3],
            'start_time': row[4],
            'venue': row[5],
            'city': row[6],
            'state': row[7],
            'price': row[8],
            'ticket_url': row[9],
            'description': row[10],
            'genre': row[11],
            'notes': row[12]
        })
    
    conn.close()
    
    return render_template('published.html', events=events)

@app.route('/update_published', methods=['POST'])
def update_published():
    """Update an already published event"""
    event_data = request.json
    event_id = event_data.get('id')
    
    conn = get_db_connection()
    c = conn.cursor()
    
    if DATABASE_URL:
        c.execute('''UPDATE events 
                     SET name = %s, date = %s, doors_time = %s, start_time = %s,
                         venue = %s, price = %s, genre = %s, notes = %s
                     WHERE id = %s''',
                  (event_data['name'], event_data['date'], event_data.get('doors_time'),
                   event_data.get('start_time'), event_data['venue'], event_data.get('price'),
                   event_data.get('genre'), event_data.get('notes'), event_id))
    else:
        c.execute('''UPDATE events 
                     SET name = ?, date = ?, doors_time = ?, start_time = ?,
                         venue = ?, price = ?, genre = ?, notes = ?
                     WHERE id = ?''',
                  (event_data['name'], event_data['date'], event_data.get('doors_time'),
                   event_data.get('start_time'), event_data['venue'], event_data.get('price'),
                   event_data.get('genre'), event_data.get('notes'), event_id))
    
    conn.commit()
    conn.close()
    
    return jsonify({'status': 'success'})

@app.route('/delete_published', methods=['POST'])
def delete_published():
    """Delete a published event"""
    event_id = request.json.get('id')
    
    conn = get_db_connection()
    c = conn.cursor()
    
    if DATABASE_URL:
        c.execute('DELETE FROM events WHERE id = %s', (event_id,))
    else:
        c.execute('DELETE FROM events WHERE id = ?', (event_id,))
    
    conn.commit()
    conn.close()
    
    return jsonify({'status': 'success'})

@app.route('/')
def index():
    events = load_pending_events()
    
    # Add duplicate detection and confidence to each event
    for event in events:
        event['duplicates'] = find_duplicates(
            event['name'], 
            event['date'], 
            event['venue']
        )
        event['overall_confidence'] = calculate_confidence(event)
    
    # Sort by confidence (lowest first = needs most review)
    events.sort(key=lambda x: x['overall_confidence'])
    
    return render_template('review.html', events=events)

@app.route('/debug')
def debug():
    import os
    db_url = os.environ.get('DATABASE_URL', 'NOT SET')
    masked = db_url[:30] + '...' if len(db_url) > 30 else db_url
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM events WHERE status = 'approved'")
        count = c.fetchone()[0]
        conn.close()
        return jsonify({'DATABASE_URL': masked, 'approved_events': count})
    except Exception as e:
        return jsonify({'DATABASE_URL': masked, 'error': str(e)})

@app.route('/stats')
def stats():
    conn = get_db_connection()
    c = conn.cursor()
    
    # Total events by status
    c.execute("SELECT status, COUNT(*) FROM events GROUP BY status")
    status_counts = dict(c.fetchall())
    
    # Events by venue
    c.execute("""SELECT venue, COUNT(*) FROM events 
                 WHERE status = 'approved' GROUP BY venue""")
    venue_counts = dict(c.fetchall())
    
    # Upcoming events (next 30 days)
    today = datetime.now().date().isoformat()
    future = (datetime.now().date() + timedelta(days=30)).isoformat()
    c.execute("""SELECT COUNT(*) FROM events 
                 WHERE status = 'approved' 
                 AND date >= ? AND date <= ?""", (today, future))
    upcoming_count = c.fetchone()[0]
    
    # Events by genre
    c.execute("""SELECT genre, COUNT(*) FROM events 
                 WHERE status = 'approved' AND genre IS NOT NULL 
                 GROUP BY genre""")
    genre_counts = dict(c.fetchall())
    
    conn.close()
    
    return jsonify({
        'status': status_counts,
        'venues': venue_counts,
        'upcoming': upcoming_count,
        'genres': genre_counts
    })

@app.route('/calendar')
def calendar():
    conn = get_db_connection()
    c = conn.cursor()

    today = datetime.now().date().isoformat()

    if DATABASE_URL:
        c.execute('''SELECT name, date, doors_time, start_time, venue, city,
                     price, genre, description
                     FROM events
                     WHERE status = 'approved'
                     AND date >= %s
                     ORDER BY date ASC''', (today,))
    else:
        c.execute('''SELECT name, date, doors_time, start_time, venue, city,
                     price, genre, description
                     FROM events
                     WHERE status = 'approved'
                     AND date >= ?
                     ORDER BY date ASC''', (today,))
    
    events = []
    for row in c.fetchall():
        events.append({
            'name': row[0],
            'date': row[1],
            'doors_time': row[2],
            'start_time': row[3],
            'venue': row[4],
            'city': row[5],
            'price': row[6],
            'genre': row[7],
            'description': row[8]
        })
    
    conn.close()
    
    return render_template('calendar.html', events=events)

@app.route('/api/events')
def api_events():
    conn = get_db_connection()
    c = conn.cursor()
    
    venue = request.args.get('venue')
    genre = request.args.get('genre')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    query = "SELECT * FROM events WHERE status = 'approved'"
    params = []
    
    if venue:
        query += " AND venue = ?"
        params.append(venue)
    if genre:
        query += " AND genre = ?"
        params.append(genre)
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    
    query += " ORDER BY date ASC"
    
    c.execute(query, params)
    events = c.fetchall()
    conn.close()
    
    return jsonify({'events': [dict(zip([col[0] for col in c.description], row)) for row in events]})

@app.route('/approve', methods=['POST'])
def approve_event():
    event_data = request.json
    save_event(event_data)
    return jsonify({'status': 'success'})

@app.route('/batch_approve', methods=['POST'])
def batch_approve():
    events_data = request.json.get('events', [])
    for event_data in events_data:
        save_event(event_data)
    return jsonify({'status': 'success', 'count': len(events_data)})

@app.route('/batch_reject', methods=['POST'])
def batch_reject():
    events_data = request.json.get('events', [])
    for event_data in events_data:
        print(f"Rejected: {event_data['name']}")
    return jsonify({'status': 'success', 'count': len(events_data)})

@app.route('/reject', methods=['POST'])
def reject_event():
    event_name = request.json.get('name')
    print(f"Rejected: {event_name}")
    return jsonify({'status': 'success'})

@app.route('/update', methods=['POST'])
def update_event():
    event_data = request.json
    save_event(event_data)
    return jsonify({'status': 'success'})

if __name__ == '__main__':
    init_db()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)  # debug=False for production