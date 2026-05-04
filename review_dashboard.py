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
    """Load pending events from database"""
    conn = get_db_connection()
    c = conn.cursor()
    ph = '%s' if DATABASE_URL else '?'
    
    c.execute(f'''SELECT id, name, date, doors_time, start_time, venue, location, city, state,
                 price, ticket_url, description, genre, confidence, notes, event_type, visible,
                 sold_out, date_changed, openers, event_url
                 FROM events WHERE status = 'pending' ORDER BY date ASC''')
    
    events = []
    for row in c.fetchall():
        events.append({
            'id': row[0],
            'name': row[1],
            'date': row[2],
            'doors_time': row[3],
            'start_time': row[4],
            'venue': row[5],
            'location': row[6],
            'city': row[7],
            'state': row[8],
            'price': row[9],
            'ticket_url': row[10],
            'description': row[11],
            'genre': row[12],
            'confidence': json.loads(row[13]) if row[13] else {},
            'notes': row[14] or '',
            'event_type': row[15] or 'music',
            'visible': row[16] if row[16] is not None else True,
            'sold_out': row[17] if row[17] is not None else False,
            'date_changed': row[18] if row[18] is not None else False,
            'openers': row[19],
            'event_url': row[20]
        })
    
    conn.close()
    return events

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
                   'pending',
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
                   'pending',
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

@app.route('/dashboard-uinizti3')
def index():
    events = load_pending_events()
    
    for event in events:
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

@app.route('/')
def calendar():
    conn = get_db_connection()
    c = conn.cursor()
    today = datetime.now().date().isoformat()
    if DATABASE_URL:
        ph = '%s' if DATABASE_URL else '?'
        vis = 'true' if DATABASE_URL else '1'

        c.execute(f'''SELECT name, date, doors_time, start_time, venue, location, city,
                            price, genre, description, event_type, sold_out, event_url, ticket_url
                            FROM events
                            WHERE status = 'approved'
                            AND visible = {vis}
                            AND date >= {ph}
                            ORDER BY date ASC''', (today,))
    else:
        ph = '%s' if DATABASE_URL else '?'
        vis = 'true' if DATABASE_URL else '1'

        c.execute(f'''SELECT name, date, doors_time, start_time, venue, location, city,
                            price, genre, description, event_type, sold_out, event_url, ticket_url
                            FROM events
                            WHERE status = 'approved'
                            AND visible = {vis}
                            AND date >= {ph}
                            ORDER BY date ASC''', (today,))

    events = []
    for row in c.fetchall():
        events.append({
            'name': row[0],
            'date': row[1],
            'doors_time': row[2],
            'start_time': row[3],
            'venue': row[4],
            'location': row[5],
            'city': row[6],
            'price': row[7],
            'genre': row[8],
            'description': row[9],
            'event_type': row[10],
            'sold_out': row[11] if row[11] is not None else False,
            'event_url': row[12],
            'ticket_url': row[13]
        })

    conn.close()

    # Build dynamic venue and genre lists from events
    venues = sorted(set(e['venue'] for e in events if e['venue']))
    genres = sorted(set(e['genre'] for e in events if e['genre']))

    return render_template('calendar.html', events=events, venues=venues, genres=genres)

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

def do_approve_event(event_data):
    conn = get_db_connection()
    c = conn.cursor()
    now = datetime.now().isoformat()
    if DATABASE_URL:
        c.execute('''UPDATE events SET status = 'approved', approved_at = %s
                     WHERE id = %s''',
                  (now, event_data['id']))
    else:
        c.execute('''UPDATE events SET status = 'approved', approved_at = ?
                     WHERE id = ?''',
                  (now, event_data['id']))
    conn.commit()
    conn.close()

@app.route('/approve', methods=['POST'])
def approve_event():
    event_data = request.json
    do_approve_event(event_data)
    return jsonify({'status': 'success'})

@app.route('/batch_approve', methods=['POST'])
def batch_approve():
    events_data = request.json.get('events', [])
    for event_data in events_data:
        do_approve_event(event_data)
    return jsonify({'status': 'success', 'count': len(events_data)})

def do_reject_event(event_data):
    conn = get_db_connection()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute('UPDATE events SET status = %s WHERE id = %s',
                  ('rejected', event_data['id']))
    else:
        c.execute('UPDATE events SET status = ? WHERE id = ?',
                  ('rejected', event_data['id']))
    conn.commit()
    conn.close()

def do_update_event(event_data):
    conn = get_db_connection()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute('''UPDATE events SET name=%s, date=%s, doors_time=%s, start_time=%s,
                     venue=%s, location=%s, price=%s, genre=%s, notes=%s,
                     event_type=%s, visible=%s
                     WHERE id=%s''',
                  (event_data['name'], event_data['date'], event_data.get('doors_time'),
                   event_data.get('start_time'), event_data['venue'], event_data.get('location'),
                   event_data.get('price'), event_data.get('genre'),
                   event_data.get('notes', ''), event_data.get('event_type', 'music'),
                   event_data.get('visible', True), event_data['id']))
    else:
        c.execute('''UPDATE events SET name=?, date=?, doors_time=?, start_time=?,
                     venue=?, location=?, price=?, genre=?, notes=?,
                     event_type=?, visible=?
                     WHERE id=?''',
                  (event_data['name'], event_data['date'], event_data.get('doors_time'),
                   event_data.get('start_time'), event_data['venue'], event_data.get('location'),
                   event_data.get('price'), event_data.get('genre'),
                   event_data.get('notes', ''), event_data.get('event_type', 'music'),
                   event_data.get('visible', True), event_data['id']))
    conn.commit()
    conn.close()

@app.route('/batch_reject', methods=['POST'])
def batch_reject():
    events_data = request.json.get('events', [])
    for event_data in events_data:
        do_reject_event(event_data)
    return jsonify({'status': 'success', 'count': len(events_data)})

@app.route('/reject', methods=['POST'])
def reject_event():
    event_data = request.json
    do_reject_event(event_data)
    return jsonify({'status': 'success'})

@app.route('/update', methods=['POST'])
def update_event():
    event_data = request.json
    do_update_event(event_data)

@app.route('/api/filter_events', methods=['POST'])
def filter_events():
    """Filter and sort events based on criteria"""
    filters = request.json
    
    conn = get_db_connection()
    c = conn.cursor()
    
    # Base query
    query = '''SELECT id, name, date, doors_time, start_time, venue, location, city, state,
               price, ticket_url, description, genre, confidence, notes, status, created_at,
               event_type, visible, sold_out, date_changed, openers, event_url
               FROM events WHERE 1=1'''
    params = []
    
    # Status filter
    if filters.get('status') and filters['status'] != 'all':
        if DATABASE_URL:
            query += ' AND status = %s'
        else:
            query += ' AND status = ?'
        params.append(filters['status'])
    
    # Venue filter
    if filters.get('venue') and filters['venue'] != 'all':
        if DATABASE_URL:
            query += ' AND venue = %s'
        else:
            query += ' AND venue = ?'
        params.append(filters['venue'])
    
    # Genre filter
    if filters.get('genre') and filters['genre'] != 'all':
        if DATABASE_URL:
            query += ' AND genre = %s'
        else:
            query += ' AND genre = ?'
        params.append(filters['genre'])
    
    # Date range filter
    if filters.get('date_from'):
        if DATABASE_URL:
            query += ' AND date >= %s'
        else:
            query += ' AND date >= ?'
        params.append(filters['date_from'])
    
    if filters.get('date_to'):
        if DATABASE_URL:
            query += ' AND date <= %s'
        else:
            query += ' AND date <= ?'
        params.append(filters['date_to'])
    
    # Search filter
    if filters.get('search'):
        if DATABASE_URL:
            query += ' AND LOWER(name) LIKE %s'
            params.append(f"%{filters['search'].lower()}%")
        else:
            query += ' AND LOWER(name) LIKE ?'
            params.append(f"%{filters['search'].lower()}%")
    
    # Missing data filter
    if filters.get('missing_data'):
        query += ' AND (price IS NULL OR genre IS NULL OR start_time IS NULL)'
    
    # Sort
    sort_by = filters.get('sort_by', 'date')
    sort_order = filters.get('sort_order', 'asc')
    
    sort_mapping = {
        'date': 'date',
        'confidence': 'confidence',
        'venue': 'venue',
        'created': 'created_at',
        'name': 'name'
    }
    
    sort_col = sort_mapping.get(sort_by, 'date')
    query += f' ORDER BY {sort_col} {sort_order.upper()}'
    
    # Execute query
    c.execute(query, params)
    rows = c.fetchall()
    
    events = []
    for row in rows:
        event = {
            'id': row[0],
            'name': row[1],
            'date': row[2],
            'doors_time': row[3],
            'start_time': row[4],
            'venue': row[5],
            'location': row[6],
            'city': row[7],
            'state': row[8],
            'price': row[9],
            'ticket_url': row[10],
            'description': row[11],
            'genre': row[12],
            'confidence': json.loads(row[13]) if row[13] else {},
            'notes': row[14],
            'status': row[15],
            'created_at': row[16],
            'event_type': row[17] or 'music',
            'visible': row[18] if row[18] is not None else True,
            'sold_out': row[19] if row[19] is not None else False,
            'date_changed': row[20] if row[20] is not None else False,
            'openers': row[21],
            'event_url': row[22]
        }
        
        # Calculate overall confidence
        event['overall_confidence'] = calculate_confidence(event)
        
        events.append(event)

    conn.close()

    # Apply confidence filter (can't do in SQL easily)
    if filters.get('confidence_level'):
        if filters['confidence_level'] == 'low':
            events = [e for e in events if e['overall_confidence'] < 0.6]
        elif filters['confidence_level'] == 'medium':
            events = [e for e in events if 0.6 <= e['overall_confidence'] < 0.8]
        elif filters['confidence_level'] == 'high':
            events = [e for e in events if e['overall_confidence'] >= 0.8]

    # Apply duplicates filter
    if filters.get('has_duplicates'):
        events = [e for e in events if e['status'] == 'possible_duplicate']
    
    return jsonify({'events': events, 'count': len(events)})

@app.route('/api/venues')
def get_venues():
    """Get list of all unique venues"""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT DISTINCT venue FROM events ORDER BY venue')
    venues = [row[0] for row in c.fetchall()]
    conn.close()
    return jsonify({'venues': venues})

@app.route('/api/genres')
def get_genres():
    """Get list of all unique genres"""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT DISTINCT genre FROM events WHERE genre IS NOT NULL ORDER BY genre')
    genres = [row[0] for row in c.fetchall()]
    conn.close()
    return jsonify({'genres': genres})

if __name__ == '__main__':
    init_db()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)  # debug=False for production