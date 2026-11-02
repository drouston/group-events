"""Two-axis event status: review_status (the reviewer's decision) and event_status (what is
happening to the show). The legacy single `status` column is kept in sync by a Postgres
trigger in both directions, so code that still writes `status` keeps working and a code
rollback needs no data migration.

review_status: pending | approved | rejected
event_status:  scheduled | tentative | postponed | canceled
A row is a possible duplicate when review_status is pending and duplicate_of_id is set.
"""

REVIEW_STATUSES = ('pending', 'approved', 'rejected')
EVENT_STATUSES = ('scheduled', 'tentative', 'postponed', 'canceled')

# SQL predicates, safe for both Postgres and SQLite.
PUBLIC_SQL = "review_status = 'approved' AND event_status IN ('scheduled', 'tentative', 'postponed')"
NEEDS_REVIEW_SQL = "review_status = 'pending' AND event_status != 'canceled'"
POSSIBLE_DUPLICATE_SQL = "review_status = 'pending' AND duplicate_of_id IS NOT NULL AND event_status != 'canceled'"
ACTIVE_SQL = "review_status IN ('pending', 'approved') AND event_status != 'canceled'"


def legacy_status(review_status, event_status, duplicate_of_id):
    """Python mirror of the trigger's legacy mapping (used for the SQLite dev path)."""
    if event_status == 'canceled':
        return 'canceled'
    if review_status in ('approved', 'rejected'):
        return review_status
    return 'possible_duplicate' if duplicate_of_id else 'pending'


_PG_FUNCTION = """
CREATE OR REPLACE FUNCTION sync_event_status() RETURNS trigger AS $$
DECLARE
    new_axes_changed boolean;
BEGIN
    IF TG_OP = 'INSERT' THEN
        new_axes_changed := NEW.review_status IS NOT NULL OR NEW.event_status IS NOT NULL;
    ELSE
        new_axes_changed := NEW.review_status IS DISTINCT FROM OLD.review_status
                            OR NEW.event_status IS DISTINCT FROM OLD.event_status;
    END IF;

    IF NOT new_axes_changed AND (TG_OP = 'INSERT' OR NEW.status IS DISTINCT FROM OLD.status) THEN
        -- A legacy writer set `status`: derive both axes from it.
        IF NEW.status = 'canceled' THEN
            NEW.event_status := 'canceled';
            IF TG_OP = 'INSERT' OR OLD.review_status IS NULL THEN
                NEW.review_status := CASE WHEN NEW.approved_at IS NOT NULL THEN 'approved' ELSE 'pending' END;
            END IF;
        ELSE
            NEW.review_status := CASE NEW.status
                WHEN 'approved' THEN 'approved'
                WHEN 'rejected' THEN 'rejected'
                ELSE 'pending' END;
            IF TG_OP = 'INSERT' OR OLD.event_status IS NULL OR OLD.event_status = 'canceled' THEN
                NEW.event_status := 'scheduled';
            END IF;
        END IF;
    END IF;

    NEW.review_status := COALESCE(NEW.review_status, 'pending');
    NEW.event_status := COALESCE(NEW.event_status, 'scheduled');

    -- The tentative-cancellation timestamp and the tentative state always agree.
    IF NEW.event_status IN ('scheduled', 'tentative') THEN
        NEW.event_status := CASE WHEN NEW.cancel_flagged_at IS NOT NULL THEN 'tentative' ELSE 'scheduled' END;
    END IF;

    NEW.status := CASE
        WHEN NEW.event_status = 'canceled' THEN 'canceled'
        WHEN NEW.review_status IN ('approved', 'rejected') THEN NEW.review_status
        WHEN NEW.duplicate_of_id IS NOT NULL THEN 'possible_duplicate'
        ELSE 'pending' END;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""


def migrate_status_columns(c, is_postgres):
    """Idempotent. Adds the two status columns, backfills them from `status`, and installs the
    sync trigger. Safe to run from both the web service and the scraper on every start."""
    if is_postgres:
        for table in ('events', 'past_events'):
            c.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS review_status TEXT")
            c.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS event_status TEXT")
        c.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS cancel_flagged_at TEXT")
        c.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS duplicate_of_id INTEGER")
        c.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS approved_at TEXT")
        c.execute("ALTER TABLE past_events ADD COLUMN IF NOT EXISTS approved_at TEXT")
        for table in ('events', 'past_events'):
            flag = "cancel_flagged_at IS NOT NULL" if table == 'events' else "FALSE"
            c.execute(f"""UPDATE {table} SET
                review_status = CASE
                    WHEN status = 'approved' THEN 'approved'
                    WHEN status = 'rejected' THEN 'rejected'
                    WHEN status = 'canceled' AND approved_at IS NOT NULL THEN 'approved'
                    ELSE 'pending' END,
                event_status = CASE
                    WHEN status = 'canceled' THEN 'canceled'
                    WHEN {flag} THEN 'tentative'
                    ELSE 'scheduled' END
                WHERE review_status IS NULL OR event_status IS NULL""")
        c.execute(_PG_FUNCTION)
        c.execute("SELECT 1 FROM pg_trigger WHERE tgname = 'events_status_sync'")
        if not c.fetchone():
            c.execute("""CREATE TRIGGER events_status_sync BEFORE INSERT OR UPDATE ON events
                         FOR EACH ROW EXECUTE FUNCTION sync_event_status()""")
    else:
        for table in ('events', 'past_events'):
            c.execute(f"PRAGMA table_info({table})")
            cols = {row[1] for row in c.fetchall()}
            for col in ('review_status', 'event_status'):
                if col not in cols:
                    c.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
            c.execute(f"""UPDATE {table} SET
                review_status = CASE WHEN status IN ('approved', 'rejected') THEN status ELSE 'pending' END,
                event_status = CASE WHEN status = 'canceled' THEN 'canceled' ELSE 'scheduled' END
                WHERE review_status IS NULL OR event_status IS NULL""")
