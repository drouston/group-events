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
        c.execute('''CREATE TABLE IF NOT EXISTS events
                     (id SERIAL PRIMARY KEY,
                      name TEXT,
                      start_date TEXT,
                      end_date TEXT,
                      doors_time TEXT,
                      start_time TEXT,
                      end_time TEXT,
                      multi_day BOOLEAN DEFAULT FALSE,
                      venue TEXT,
                      location TEXT,
                      city TEXT,
                      state TEXT,
                      price TEXT,
                      ticket_url TEXT,
                      event_url TEXT,
                      description TEXT,
                      genre TEXT,
                      confidence TEXT,
                      notes TEXT,
                      status TEXT DEFAULT 'pending',
                      created_at TEXT,
                      approved_at TEXT,
                      event_type TEXT DEFAULT 'music',
                      visible BOOLEAN DEFAULT TRUE,
                      sold_out BOOLEAN DEFAULT FALSE,
                      date_changed BOOLEAN DEFAULT FALSE,
                      openers TEXT,
                      duplicate_of_id INTEGER)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS past_events
                     (id SERIAL PRIMARY KEY,
                      name TEXT,
                      start_date TEXT,
                      end_date TEXT,
                      doors_time TEXT,
                      start_time TEXT,
                      end_time TEXT,
                      multi_day BOOLEAN DEFAULT FALSE,
                      venue TEXT,
                      location TEXT,
                      city TEXT,
                      state TEXT,
                      price TEXT,
                      ticket_url TEXT,
                      event_url TEXT,
                      description TEXT,
                      genre TEXT,
                      confidence TEXT,
                      notes TEXT,
                      status TEXT,
                      created_at TEXT,
                      approved_at TEXT,
                      archived_at TEXT,
                      event_type TEXT DEFAULT 'music',
                      visible BOOLEAN DEFAULT TRUE,
                      sold_out BOOLEAN DEFAULT FALSE,
                      date_changed BOOLEAN DEFAULT FALSE,
                      openers TEXT,
                      duplicate_of_id INTEGER)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS scrape_stats
                     (id SERIAL PRIMARY KEY,
                      scrape_date TEXT,
                      venue TEXT,
                      total_scraped INTEGER,
                      total_approved INTEGER,
                      total_rejected INTEGER)''')
    
    else:
        c.execute('''CREATE TABLE IF NOT EXISTS events
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      name TEXT,
                      start_date TEXT,
                      end_date TEXT,
                      doors_time TEXT,
                      start_time TEXT,
                      end_time TEXT,
                      multi_day BOOLEAN DEFAULT FALSE,
                      venue TEXT,
                      location TEXT,
                      city TEXT,
                      state TEXT,
                      price TEXT,
                      ticket_url TEXT,
                      event_url TEXT,
                      description TEXT,
                      genre TEXT,
                      confidence TEXT,
                      notes TEXT,
                      status TEXT DEFAULT 'pending',
                      created_at TEXT,
                      approved_at TEXT,
                      event_type TEXT DEFAULT 'music',
                      visible BOOLEAN DEFAULT TRUE,
                      sold_out BOOLEAN DEFAULT FALSE,
                      date_changed BOOLEAN DEFAULT FALSE,
                      openers TEXT,
                      duplicate_of_id INTEGER)''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS past_events
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      name TEXT,
                      start_date TEXT,
                      end_date TEXT,
                      doors_time TEXT,
                      start_time TEXT,
                      end_time TEXT,
                      multi_day BOOLEAN DEFAULT FALSE,
                      venue TEXT,
                      location TEXT,
                      city TEXT,
                      state TEXT,
                      price TEXT,
                      ticket_url TEXT,
                      event_url TEXT,
                      description TEXT,
                      genre TEXT,
                      confidence TEXT,
                      notes TEXT,
                      status TEXT,
                      created_at TEXT,
                      approved_at TEXT,
                      archived_at TEXT,
                      event_type TEXT DEFAULT 'music',
                      visible BOOLEAN DEFAULT TRUE,
                      sold_out BOOLEAN DEFAULT FALSE,
                      date_changed BOOLEAN DEFAULT FALSE,
                      openers TEXT,
                      duplicate_of_id INTEGER)''')
        
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
    
    c.execute(f'''SELECT id, name, start_date, end_date, doors_time, start_time, end_time,
             multi_day, venue, location, city, state,
             price, ticket_url, event_url, venue_url, description, genre, confidence, notes, event_type, visible,
             sold_out, date_changed, openers
             FROM events WHERE status = 'pending' ORDER BY start_date ASC''')

    events = []
    for row in c.fetchall():
        events.append({
            'id': row[0],
            'name': row[1],
            'start_date': row[2],
            'end_date': row[3],
            'doors_time': str(row[4]) if row[4] is not None else None,
            'start_time': str(row[5]) if row[5] is not None else None,
            'end_time': str(row[6]) if row[6] is not None else None,
            'multi_day': row[7],
            'venue': row[8],
            'location': row[9],
            'city': row[10],
            'state': row[11],
            'price': row[12],
            'ticket_url': row[13],
            'event_url': row[14],
            'venue_url': row[15],
            'description': row[16],
            'genre': row[17],
            'confidence': json.loads(row[18]) if row[18] else {},
            'notes': row[19] or '',
            'event_type': row[20] or 'music',
            'visible': row[21] if row[21] is not None else True,
            'sold_out': row[22] if row[22] is not None else False,
            'date_changed': row[23] if row[23] is not None else False,
            'openers': row[24]
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
    if 'start_date' in conf:
        scores.append(conf['start_date'] * 2)  # Date is critical
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
                     WHERE name = %s AND start_date = %s AND venue = %s AND status = 'approved' 
                     LIMIT 1''', 
                  (event_data['name'], event_data['start_date'], event_data['venue']))
    else:
        c.execute('''SELECT id FROM events 
                     WHERE name = ? AND start_date = ? AND venue = ? AND status = 'approved' 
                     LIMIT 1''', 
                  (event_data['name'], event_data['start_date'], event_data['venue']))
    
    if c.fetchone():
        print(f"Duplicate prevented: {event_data['name']} on {event_data['start_date']}")
        conn.close()
        return  # Already exists, don't insert
    
    # Insert if not duplicate
    if DATABASE_URL:
        c.execute('''INSERT INTO events 
                    (name, start_date, end_date, doors_time, start_time, end_time,
                    multi_day, venue, location, city, state, 
                    price, ticket_url, event_url, description, genre, confidence, notes,
                    status, created_at, event_type, visible, sold_out, date_changed, openers)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (event_data['name'], event_data.get('start_date'),
                event_data.get('end_date'), event_data.get('doors_time'),
                event_data.get('start_time'), event_data.get('end_time'),
                event_data.get('multi_day', False),
                event_data['venue'], event_data.get('location'),
                event_data.get('city', ''), event_data.get('state', ''),
                event_data.get('price'), event_data.get('ticket_url'),
                event_data.get('event_url'), event_data.get('description'),
                event_data.get('genre'), json.dumps(event_data.get('confidence', {})),
                event_data.get('notes', ''), 'pending',
                datetime.now().isoformat(),
                event_data.get('event_type', 'music'),
                event_data.get('visible', True),
                event_data.get('sold_out', False),
                event_data.get('date_changed', False),
                event_data.get('openers')))
    else:
        c.execute('''INSERT INTO events 
                    (name, start_date, end_date, doors_time, start_time, end_time,
                    multi_day, venue, location, city, state, 
                    price, ticket_url, event_url, description, genre, confidence, notes,
                    status, created_at, event_type, visible, sold_out, date_changed, openers)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (event_data['name'], event_data.get('start_date'),
                event_data.get('end_date'), event_data.get('doors_time'),
                event_data.get('start_time'), event_data.get('end_time'),
                event_data.get('multi_day', False),
                event_data['venue'], event_data.get('location'),
                event_data.get('city', ''), event_data.get('state', ''),
                event_data.get('price'), event_data.get('ticket_url'),
                event_data.get('event_url'), event_data.get('description'),
                event_data.get('genre'), json.dumps(event_data.get('confidence', {})),
                event_data.get('notes', ''), 'pending',
                datetime.now().isoformat(),
                event_data.get('event_type', 'music'),
                event_data.get('visible', True),
                event_data.get('sold_out', False),
                event_data.get('date_changed', False),
                event_data.get('openers')))
    conn.commit()
    conn.close()

@app.route('/published')
def published_events():
    """View and edit already published events"""
    conn = get_db_connection()
    c = conn.cursor()
    if DATABASE_URL:
        c.execute('''SELECT id, name, start_date, end_date, doors_time, start_time, end_time,
                     multi_day, venue, location, city, state,
                     price, ticket_url, event_url, venue_url, description, genre, notes
                     FROM events 
                     WHERE status = 'approved'
                     ORDER BY start_date ASC''')
    else:
        c.execute('''SELECT id, name, start_date, end_date, doors_time, start_time, end_time,
                     multi_day, venue, location, city, state,
                     price, ticket_url, event_url, venue_url, description, genre, notes
                     FROM events 
                     WHERE status = 'approved'
                     ORDER BY start_date ASC''')
    rows = c.fetchall()
    events = []
    for row in rows:
        events.append({
            'id': row[0],
            'name': row[1],
            'start_date': row[2],
            'end_date': row[3],
            'doors_time': str(row[4]) if row[4] is not None else None,
            'start_time': str(row[5]) if row[5] is not None else None,
            'end_time': str(row[6]) if row[6] is not None else None,
            'multi_day': row[7],
            'venue': row[8],
            'location': row[9],
            'city': row[10],
            'state': row[11],
            'price': row[12],
            'ticket_url': row[13],
            'event_url': row[14],
            'venue_url': row[15],
            'description': row[16],
            'genre': row[17],
            'notes': row[18]
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
                     SET name = %s, start_date = %s, end_date = %s, doors_time = %s,
                         start_time = %s, end_time = %s, multi_day = %s,
                         venue = %s, location = %s, price = %s, genre = %s, notes = %s
                     WHERE id = %s''',
                  (event_data['name'], event_data.get('start_date'), event_data.get('end_date'),
                   event_data.get('doors_time'), event_data.get('start_time'),
                   event_data.get('end_time'), event_data.get('multi_day', False),
                   event_data['venue'], event_data.get('location'),
                   event_data.get('price'), event_data.get('genre'),
                   event_data.get('notes'), event_id))
    else:
        c.execute('''UPDATE events 
                     SET name = ?, start_date = ?, end_date = ?, doors_time = ?,
                         start_time = ?, end_time = ?, multi_day = ?,
                         venue = ?, location = ?, price = ?, genre = ?, notes = ?
                     WHERE id = ?''',
                  (event_data['name'], event_data.get('start_date'), event_data.get('end_date'),
                   event_data.get('doors_time'), event_data.get('start_time'),
                   event_data.get('end_time'), event_data.get('multi_day', False),
                   event_data['venue'], event_data.get('location'),
                   event_data.get('price'), event_data.get('genre'),
                   event_data.get('notes'), event_id))
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
    ph = '%s' if DATABASE_URL else '?'

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
    c.execute(f"""SELECT COUNT(*) FROM events
                 WHERE status = 'approved'
                 AND start_date >= {ph} AND start_date <= {ph}""", (today, future))
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
    ph = '%s' if DATABASE_URL else '?'
    vis = 'true' if DATABASE_URL else '1'
    c.execute(f'''SELECT name, start_date, end_date, doors_time, start_time, end_time,
                         multi_day, venue, location, city,
                         price, genre, description, event_type, sold_out, event_url, ticket_url, venue_url
                         FROM events
                         WHERE status = 'approved'
                         AND visible = {vis}
                         AND start_date >= {ph}
                         ORDER BY start_date ASC''', (today,))
    events = []
    for row in c.fetchall():
        events.append({
            'name': row[0],
            'start_date': row[1],
            'end_date': row[2],
            'doors_time': row[3],
            'start_time': row[4],
            'end_time': row[5],
            'multi_day': row[6],
            'venue': row[7],
            'location': row[8],
            'city': row[9],
            'price': row[10],
            'genre': row[11],
            'description': row[12],
            'event_type': row[13],
            'sold_out': row[14] if row[14] is not None else False,
            'event_url': row[15],
            'ticket_url': row[16],
            'venue_url': row[17]
        })
    conn.close()
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
        query += " AND start_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND start_date <= ?"
        params.append(end_date)
    
    query += " ORDER BY start_date ASC"
    
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
        c.execute('''UPDATE events SET name=%s, start_date=%s, end_date=%s, doors_time=%s,
                     start_time=%s, end_time=%s, multi_day=%s,
                     venue=%s, location=%s, price=%s, genre=%s, notes=%s,
                     event_type=%s, visible=%s
                     WHERE id=%s''',
                  (event_data['name'], event_data.get('start_date'), event_data.get('end_date'),
                   event_data.get('doors_time'), event_data.get('start_time'),
                   event_data.get('end_time'), event_data.get('multi_day', False),
                   event_data['venue'], event_data.get('location'),
                   event_data.get('price'), event_data.get('genre'),
                   event_data.get('notes', ''), event_data.get('event_type', 'music'),
                   event_data.get('visible', True), event_data['id']))
    else:
        c.execute('''UPDATE events SET name=?, start_date=?, end_date=?, doors_time=?,
                     start_time=?, end_time=?, multi_day=?,
                     venue=?, location=?, price=?, genre=?, notes=?,
                     event_type=?, visible=?
                     WHERE id=?''',
                  (event_data['name'], event_data.get('start_date'), event_data.get('end_date'),
                   event_data.get('doors_time'), event_data.get('start_time'),
                   event_data.get('end_time'), event_data.get('multi_day', False),
                   event_data['venue'], event_data.get('location'),
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
    ph = '%s' if DATABASE_URL else '?'
    
    query = '''SELECT id, name, start_date, end_date, doors_time, start_time, end_time,
               multi_day, venue, location, city, state,
               price, ticket_url, venue_url, description, genre, confidence, notes, status, created_at,
               event_type, visible, sold_out, date_changed, openers, event_url, duplicate_of_id
               FROM events WHERE 1=1'''
    params = []
    
    if filters.get('status') and filters['status'] != 'all':
        query += f' AND status = {ph}'
        params.append(filters['status'])
    
    if filters.get('venue') and filters['venue'] != 'all':
        query += f' AND venue = {ph}'
        params.append(filters['venue'])
    
    if filters.get('genre') and filters['genre'] != 'all':
        query += f' AND genre = {ph}'
        params.append(filters['genre'])
    
    if filters.get('date_from'):
        query += f' AND start_date >= {ph}'
        params.append(filters['date_from'])
    
    if filters.get('date_to'):
        query += f' AND start_date <= {ph}'
        params.append(filters['date_to'])
    
    if filters.get('search'):
        query += f' AND LOWER(name) LIKE {ph}'
        params.append(f"%{filters['search'].lower()}%")
    
    if filters.get('missing_data'):
        query += ' AND (price IS NULL OR genre IS NULL OR start_time IS NULL)'
    
    if filters.get('has_end_date'):
        query += ' AND end_date IS NOT NULL AND end_date != start_date'

    sort_by = filters.get('sort_by', 'start_date')
    sort_order = filters.get('sort_order', 'asc')
    
    sort_mapping = {
        'start_date': 'start_date',
        'confidence': 'confidence',
        'venue': 'venue',
        'created': 'created_at',
        'name': 'name'
    }
    
    sort_col = sort_mapping.get(sort_by, 'start_date')
    query += f' ORDER BY {sort_col} {sort_order.upper()}'
    
    c.execute(query, params)
    rows = c.fetchall()
    
    events = []
    for row in rows:
        event = {
            'id': row[0],
            'name': row[1],
            'start_date': row[2],
            'end_date': row[3],
            'doors_time': str(row[4]) if row[4] is not None else None,
            'start_time': str(row[5]) if row[5] is not None else None,
            'end_time': str(row[6]) if row[6] is not None else None,
            'multi_day': row[7],
            'venue': row[8],
            'location': row[9],
            'city': row[10],
            'state': row[11],
            'price': row[12],
            'ticket_url': row[13],
            'venue_url': row[14],
            'description': row[15],
            'genre': row[16],
            'confidence': json.loads(row[17]) if row[17] else {},
            'notes': row[18],
            'status': row[19],
            'created_at': row[20],
            'event_type': row[21] or 'music',
            'visible': row[22] if row[22] is not None else True,
            'sold_out': row[23] if row[23] is not None else False,
            'date_changed': row[24] if row[24] is not None else False,
            'openers': row[25],
            'event_url': row[26],
            'duplicate_of_id': row[27]
        }
        event['overall_confidence'] = calculate_confidence(event)
        events.append(event)
    
    orig_ids = [e['duplicate_of_id'] for e in events if e.get('duplicate_of_id')]
    if orig_ids:
        ph_list = ','.join([ph] * len(orig_ids))
        c.execute(f'SELECT id, name, start_date, venue FROM events WHERE id IN ({ph_list})', orig_ids)
        orig_map = {row[0]: {'name': row[1], 'start_date': row[2], 'venue': row[3]} for row in c.fetchall()}
        for event in events:
            if event.get('duplicate_of_id') and event['duplicate_of_id'] in orig_map:
                event['duplicate_of'] = orig_map[event['duplicate_of_id']]
    
    conn.close()
    
    if filters.get('confidence_level'):
        if filters['confidence_level'] == 'low':
            events = [e for e in events if e['overall_confidence'] < 0.6]
        elif filters['confidence_level'] == 'medium':
            events = [e for e in events if 0.6 <= e['overall_confidence'] < 0.8]
        elif filters['confidence_level'] == 'high':
            events = [e for e in events if e['overall_confidence'] >= 0.8]
    
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