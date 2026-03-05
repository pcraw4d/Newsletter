"""
database.py — SQLite schema and connection management for Briefly.

Tables:
  newsletters  → one row per raw inbound email
  takeaways    → AI-extracted bullet points (populated in Phase 2)
  articles     → linked articles fetched from newsletter body (Phase 2)
  themes       → cross-newsletter synthesis per day (Phase 2)
"""

import sqlite3
import os
from datetime import datetime, timedelta, timezone, date

_DEFAULT_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "briefly.db")
DB_PATH = os.getenv("DB_PATH", _DEFAULT_DB)


def get_conn():
    """Return a connection with row_factory set so rows behave like dicts."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # better concurrent read perf
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """
    Create all tables if they don't already exist.
    Safe to call on every startup — uses IF NOT EXISTS throughout.
    """
    conn = get_conn()
    with conn:
        conn.executescript("""
            -- ----------------------------------------------------------------
            -- newsletters: one row per inbound email
            -- ----------------------------------------------------------------
            CREATE TABLE IF NOT EXISTS newsletters (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_email  TEXT    NOT NULL,
                sender_name   TEXT,
                subject       TEXT,
                received_at   TEXT    NOT NULL,   -- ISO-8601 UTC
                raw_html      TEXT,               -- original HTML body
                plain_text    TEXT,               -- stripped plaintext
                processed     INTEGER DEFAULT 0,  -- 0 = pending, 1 = done
                category      TEXT,
                created_at    TEXT    DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_newsletters_received
                ON newsletters(received_at DESC);

            CREATE INDEX IF NOT EXISTS idx_newsletters_processed
                ON newsletters(processed);

            -- ----------------------------------------------------------------
            -- takeaways: AI-extracted bullets, linked to a newsletter
            -- ----------------------------------------------------------------
            CREATE TABLE IF NOT EXISTS takeaways (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                newsletter_id   INTEGER NOT NULL REFERENCES newsletters(id),
                content         TEXT    NOT NULL,
                created_at      TEXT    DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_takeaways_newsletter
                ON takeaways(newsletter_id);

            -- ----------------------------------------------------------------
            -- articles: outbound links found in newsletters, fetched & summarized
            -- ----------------------------------------------------------------
            CREATE TABLE IF NOT EXISTS articles (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                newsletter_id   INTEGER NOT NULL REFERENCES newsletters(id),
                url             TEXT    NOT NULL,
                title           TEXT,
                extracted_text  TEXT,   -- raw text pulled from the page
                summary         TEXT,   -- AI-generated summary (Phase 2)
                fetch_status    TEXT    DEFAULT 'pending',  -- pending/ok/failed
                created_at      TEXT    DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_articles_newsletter
                ON articles(newsletter_id);

            CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_url
                ON articles(url);       -- deduplicate across newsletters

            -- ----------------------------------------------------------------
            -- themes: cross-newsletter synthesis, one or more per day
            -- ----------------------------------------------------------------
            CREATE TABLE IF NOT EXISTS themes (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date            TEXT    NOT NULL,  -- YYYY-MM-DD
                tag             TEXT,              -- e.g. 'MACRO SIGNAL'
                title           TEXT    NOT NULL,
                summary         TEXT,
                source_ids      TEXT,              -- JSON array of newsletter ids
                confidence      TEXT,              -- HIGH / MEDIUM / LOW
                created_at      TEXT    DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_themes_date
                ON themes(date DESC);

            -- ----------------------------------------------------------------
            -- job_analyses: one row per weekly analysis run
            -- ----------------------------------------------------------------
            CREATE TABLE IF NOT EXISTS job_analyses (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date          TEXT NOT NULL,       -- YYYY-MM-DD (Monday of the week)
                queries           TEXT,                -- JSON array of search terms used
                locations         TEXT,                -- JSON array of locations searched
                postings_analyzed INTEGER DEFAULT 0,
                created_at        TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_job_analyses_date
                ON job_analyses(run_date DESC);

            -- ----------------------------------------------------------------
            -- job_postings: raw fetched job listings (deduped by external_id)
            -- ----------------------------------------------------------------
            CREATE TABLE IF NOT EXISTS job_postings (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_id  INTEGER NOT NULL REFERENCES job_analyses(id),
                external_id  TEXT UNIQUE,
                title        TEXT,
                company      TEXT,
                location     TEXT,
                description  TEXT,
                posted_at    TEXT,
                source       TEXT DEFAULT 'adzuna',
                created_at   TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_job_postings_analysis
                ON job_postings(analysis_id);

            CREATE INDEX IF NOT EXISTS idx_job_postings_external
                ON job_postings(external_id);

            -- ----------------------------------------------------------------
            -- job_skills: AI-extracted skills aggregated per analysis run
            -- ----------------------------------------------------------------
            CREATE TABLE IF NOT EXISTS job_skills (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_id      INTEGER NOT NULL REFERENCES job_analyses(id),
                skill            TEXT NOT NULL,
                category         TEXT,               -- Technical | Tool | Domain | Soft Skill | Credential
                mention_count    INTEGER DEFAULT 1,
                pct_of_jobs      REAL DEFAULT 0.0,   -- 0.0–1.0
                trend            TEXT DEFAULT 'new', -- new | rising | stable | declining
                prior_pct        REAL,               -- pct_of_jobs from previous week's analysis
                example_companies TEXT,              -- JSON array of up to 3 company names
                created_at       TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_job_skills_analysis
                ON job_skills(analysis_id);

            -- ----------------------------------------------------------------
            -- _meta: key-value store for maintenance state (e.g. last_vacuum)
            -- ----------------------------------------------------------------
            CREATE TABLE IF NOT EXISTS _meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        try:
            conn.execute("ALTER TABLE newsletters ADD COLUMN category TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
        try:
            conn.execute("ALTER TABLE newsletters ADD COLUMN content_fingerprint TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
        try:
            conn.execute("ALTER TABLE newsletters ADD COLUMN skipped_reason TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
        try:
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_newsletters_fingerprint
                    ON newsletters(content_fingerprint)
            """)
        except sqlite3.OperationalError:
            pass  # index already exists
    conn.close()
    print(f"[db] Database initialised at {DB_PATH}")


# ---------------------------------------------------------------------------
# Helper functions used by the ingest layer
# ---------------------------------------------------------------------------

def get_newsletter_by_fingerprint(fingerprint: str) -> dict | None:
    """Return the newsletter row if one exists with this content_fingerprint, else None."""
    if not fingerprint:
        return None
    conn = get_conn()
    row = conn.execute(
        "SELECT id FROM newsletters WHERE content_fingerprint = ?",
        (fingerprint,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def insert_newsletter(sender_email, sender_name, subject, received_at,
                      raw_html, plain_text, content_fingerprint: str = "",
                      skipped_reason: str | None = None) -> int:
    """Insert a new newsletter row and return its id."""
    conn = get_conn()
    with conn:
        processed = 1 if skipped_reason else 0
        cur = conn.execute(
            """
            INSERT INTO newsletters
                (sender_email, sender_name, subject, received_at, raw_html, plain_text, content_fingerprint, skipped_reason, processed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (sender_email, sender_name, subject, received_at, raw_html, plain_text, content_fingerprint or None, skipped_reason, processed),
        )
        row_id = cur.lastrowid
    conn.close()
    return row_id


def get_unprocessed_newsletters():
    """Return all newsletters that haven't been processed yet."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM newsletters WHERE processed = 0 ORDER BY received_at ASC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_newsletter_processed(newsletter_id: int):
    conn = get_conn()
    with conn:
        conn.execute(
            "UPDATE newsletters SET processed = 1 WHERE id = ?",
            (newsletter_id,),
        )
    conn.close()


def set_newsletter_skipped(newsletter_id: int, reason: str):
    """Mark a newsletter as skipped with a reason; sets processed=1 so it won't be retried."""
    conn = get_conn()
    with conn:
        conn.execute(
            "UPDATE newsletters SET skipped_reason = ?, processed = 1 WHERE id = ?",
            (reason, newsletter_id),
        )
    conn.close()


def get_newsletters_for_date(date_str: str):
    """Return all newsletters received on a given YYYY-MM-DD date."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM newsletters WHERE date(received_at) = ? ORDER BY received_at ASC",
        (date_str,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_junk_filtered_count_for_date(date_str: str) -> int:
    """Return count of newsletters skipped (skipped_reason IS NOT NULL) on a given date."""
    conn = get_conn()
    row = conn.execute(
        "SELECT COUNT(*) FROM newsletters WHERE date(received_at) = ? AND skipped_reason IS NOT NULL",
        (date_str,),
    ).fetchone()
    conn.close()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Phase 2 helpers — articles, takeaways, themes
# ---------------------------------------------------------------------------

def insert_article(newsletter_id: int, url: str, title: str,
                   extracted_text: str, summary: str, fetch_status: str) -> int:
    """
    Insert or update an article row (upsert by URL).
    Returns the article's id.
    """
    conn = get_conn()
    with conn:
        cur = conn.execute(
            """
            INSERT INTO articles (newsletter_id, url, title, extracted_text, summary, fetch_status)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title          = excluded.title,
                extracted_text = excluded.extracted_text,
                summary        = excluded.summary,
                fetch_status   = excluded.fetch_status
            """,
            (newsletter_id, url, title, extracted_text, summary, fetch_status),
        )
        row_id = cur.lastrowid
    conn.close()
    return row_id


def get_articles_for_newsletter(newsletter_id: int) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM articles WHERE newsletter_id = ? ORDER BY id ASC",
        (newsletter_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def clear_extracted_text_for_newsletter(newsletter_id: int) -> None:
    """
    After processing, wipe the raw scraped article bodies.
    The AI-generated summary is preserved — only the source text is cleared.
    """
    conn = get_conn()
    with conn:
        conn.execute(
            "UPDATE articles SET extracted_text = NULL WHERE newsletter_id = ?",
            (newsletter_id,)
        )
    conn.close()


def clear_raw_html_for_newsletter(newsletter_id: int) -> None:
    """
    After processing, wipe the raw HTML body of a newsletter.
    The stripped plain_text is preserved for re-reading.
    """
    conn = get_conn()
    with conn:
        conn.execute(
            "UPDATE newsletters SET raw_html = NULL WHERE id = ?",
            (newsletter_id,)
        )
    conn.close()


def insert_takeaway(newsletter_id: int, content: str) -> int:
    conn = get_conn()
    with conn:
        cur = conn.execute(
            "INSERT INTO takeaways (newsletter_id, content) VALUES (?, ?)",
            (newsletter_id, content),
        )
        row_id = cur.lastrowid
    conn.close()
    return row_id


def get_takeaways_for_newsletter(newsletter_id: int) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM takeaways WHERE newsletter_id = ? ORDER BY id ASC",
        (newsletter_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_takeaways_for_newsletter(newsletter_id: int):
    """Clear takeaways before a reprocess so we don't double-up."""
    conn = get_conn()
    with conn:
        conn.execute("DELETE FROM takeaways WHERE newsletter_id = ?", (newsletter_id,))
    conn.close()


def set_newsletter_category(newsletter_id: int, category: str) -> None:
    conn = get_conn()
    with conn:
        conn.execute("UPDATE newsletters SET category = ? WHERE id = ?", (category, newsletter_id))
    conn.close()


def insert_theme(date: str, tag: str, title: str, summary: str,
                 source_ids: str, confidence: str) -> int:
    conn = get_conn()
    with conn:
        cur = conn.execute(
            """
            INSERT INTO themes (date, tag, title, summary, source_ids, confidence)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (date, tag, title, summary, source_ids, confidence),
        )
        row_id = cur.lastrowid
    conn.close()
    return row_id


def delete_themes_for_date(date: str):
    """Clear themes before re-running synthesis for a given day."""
    conn = get_conn()
    with conn:
        conn.execute("DELETE FROM themes WHERE date = ?", (date,))
    conn.close()


def get_themes_for_date(date: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM themes WHERE date = ? ORDER BY id ASC",
        (date,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Job posting analysis helpers
# ---------------------------------------------------------------------------

def insert_job_analysis(run_date: str, queries: str, locations: str) -> int:
    conn = get_conn()
    with conn:
        cur = conn.execute(
            "INSERT INTO job_analyses (run_date, queries, locations) VALUES (?, ?, ?)",
            (run_date, queries, locations),
        )
        row_id = cur.lastrowid
    conn.close()
    return row_id


def update_job_analysis_count(analysis_id: int, count: int) -> None:
    conn = get_conn()
    with conn:
        conn.execute(
            "UPDATE job_analyses SET postings_analyzed = ? WHERE id = ?",
            (count, analysis_id),
        )
    conn.close()


def insert_job_posting(analysis_id: int, external_id: str, title: str,
                       company: str, location: str, description: str,
                       posted_at: str) -> int:
    conn = get_conn()
    with conn:
        cur = conn.execute(
            """
            INSERT INTO job_postings
                (analysis_id, external_id, title, company, location,
                 description, posted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(external_id) DO NOTHING
            """,
            (analysis_id, external_id, title, company, location,
             description, posted_at),
        )
        row_id = cur.lastrowid
    conn.close()
    return row_id


def insert_job_skill(analysis_id: int, skill: str, category: str,
                     mention_count: int, pct_of_jobs: float, trend: str,
                     prior_pct: float | None, example_companies: str) -> int:
    conn = get_conn()
    with conn:
        cur = conn.execute(
            """
            INSERT INTO job_skills
                (analysis_id, skill, category, mention_count, pct_of_jobs,
                 trend, prior_pct, example_companies)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (analysis_id, skill, category, mention_count, pct_of_jobs,
             trend, prior_pct, example_companies),
        )
        row_id = cur.lastrowid
    conn.close()
    return row_id


def get_latest_job_analysis() -> dict | None:
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM job_analyses ORDER BY run_date DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_job_analysis_for_date(run_date: str) -> dict | None:
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM job_analyses WHERE run_date = ?", (run_date,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_job_skills_for_analysis(analysis_id: int) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT * FROM job_skills
        WHERE analysis_id = ?
        ORDER BY mention_count DESC
        """,
        (analysis_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_prior_week_skills(current_analysis_id: int) -> dict[str, float]:
    """
    Returns a {skill_name: pct_of_jobs} dict from the analysis run
    immediately before the given analysis_id. Used for trend calculation.
    """
    conn = get_conn()
    row = conn.execute(
        """
        SELECT id FROM job_analyses
        WHERE id < ?
        ORDER BY run_date DESC LIMIT 1
        """,
        (current_analysis_id,),
    ).fetchone()
    if not row:
        conn.close()
        return {}
    prior_id = row[0]
    rows = conn.execute(
        "SELECT skill, pct_of_jobs FROM job_skills WHERE analysis_id = ?",
        (prior_id,),
    ).fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


def delete_job_data_older_than(cutoff_date: str) -> dict:
    """Used by the retention purge. Deletes in FK-safe order."""
    conn = get_conn()
    counts = {"job_skills": 0, "job_postings": 0, "job_analyses": 0}
    with conn:
        old_ids = [
            r[0] for r in conn.execute(
                "SELECT id FROM job_analyses WHERE run_date < ?",
                (cutoff_date,)
            ).fetchall()
        ]
        if old_ids:
            ph = ",".join("?" * len(old_ids))
            counts["job_skills"] = conn.execute(
                f"DELETE FROM job_skills WHERE analysis_id IN ({ph})", old_ids
            ).rowcount
            counts["job_postings"] = conn.execute(
                f"DELETE FROM job_postings WHERE analysis_id IN ({ph})", old_ids
            ).rowcount
            counts["job_analyses"] = conn.execute(
                "DELETE FROM job_analyses WHERE run_date < ?", (cutoff_date,)
            ).rowcount
    conn.close()
    return counts


def get_full_digest_for_date(date: str) -> dict:
    """
    Return a single structured dict with everything needed to render
    the dashboard for a given date:
      - themes (cross-newsletter synthesis)
      - newsletters (with their takeaways and article summaries)
    """
    newsletters = get_newsletters_for_date(date)
    themes = get_themes_for_date(date)

    enriched = []
    for n in newsletters:
        n["takeaways"] = get_takeaways_for_newsletter(n["id"])
        n["articles"] = get_articles_for_newsletter(n["id"])
        enriched.append(n)

    return {
        "date": date,
        "themes": themes,
        "newsletters": enriched,
    }


def purge_old_data(retention_days: int = 30) -> dict:
    """
    Delete all newsletters older than retention_days and all their dependent
    rows (takeaways, articles). Also prunes themes and the gmail_ingested
    tracking table.

    Deletion order respects foreign key constraints:
      takeaways → articles → newsletters → themes → gmail_ingested

    Returns a dict of row counts deleted per table.
    """
    cutoff_iso = (
        datetime.now(timezone.utc) - timedelta(days=retention_days)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    cutoff_date = (
        date.today() - timedelta(days=retention_days)
    ).isoformat()

    counts = {
        "newsletters": 0,
        "takeaways": 0,
        "articles": 0,
        "themes": 0,
        "gmail_ingested": 0,
        "job_skills": 0,
        "job_postings": 0,
        "job_analyses": 0,
    }

    job_counts = delete_job_data_older_than(cutoff_date)
    counts["job_skills"] = job_counts["job_skills"]
    counts["job_postings"] = job_counts["job_postings"]
    counts["job_analyses"] = job_counts["job_analyses"]

    conn = get_conn()
    with conn:
        # Fetch IDs of newsletters to be deleted
        old_ids = [
            row[0] for row in conn.execute(
                "SELECT id FROM newsletters WHERE received_at < ?",
                (cutoff_iso,)
            ).fetchall()
        ]

        if old_ids:
            placeholders = ",".join("?" * len(old_ids))

            counts["takeaways"] = conn.execute(
                f"DELETE FROM takeaways WHERE newsletter_id IN ({placeholders})",
                old_ids,
            ).rowcount

            counts["articles"] = conn.execute(
                f"DELETE FROM articles WHERE newsletter_id IN ({placeholders})",
                old_ids,
            ).rowcount

            counts["newsletters"] = conn.execute(
                "DELETE FROM newsletters WHERE received_at < ?",
                (cutoff_iso,),
            ).rowcount

        # Themes use a date string (YYYY-MM-DD), not a timestamp
        counts["themes"] = conn.execute(
            "DELETE FROM themes WHERE date < ?",
            (cutoff_date,),
        ).rowcount

        # gmail_ingested only needs ~30 days to prevent re-ingestion
        # Use the same retention window for consistency
        counts["gmail_ingested"] = conn.execute(
            "DELETE FROM gmail_ingested WHERE ingested_at < ?",
            (cutoff_date,),
        ).rowcount

    conn.close()
    return counts


def vacuum_if_needed() -> bool:
    """
    Run VACUUM if it hasn't been run in the last 7 days.
    Records the last vacuum date in the _meta table.
    Returns True if VACUUM was run, False if skipped.
    """
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT value FROM _meta WHERE key = 'last_vacuum'"
        ).fetchone()
        last_vacuum = row[0] if row else None

        should_vacuum = (
            last_vacuum is None or
            (date.today() - date.fromisoformat(last_vacuum)).days >= 7
        )

        if not should_vacuum:
            return False

        # VACUUM must run outside a transaction
        conn.isolation_level = None
        conn.execute("VACUUM")
        conn.isolation_level = ""

        with conn:
            conn.execute(
                """
                INSERT INTO _meta (key, value) VALUES ('last_vacuum', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (date.today().isoformat(),),
            )
        return True
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
