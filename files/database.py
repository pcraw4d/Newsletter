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
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "briefly.db")


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
        """)
        try:
            conn.execute("ALTER TABLE newsletters ADD COLUMN category TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.close()
    print(f"[db] Database initialised at {DB_PATH}")


# ---------------------------------------------------------------------------
# Helper functions used by the ingest layer
# ---------------------------------------------------------------------------

def insert_newsletter(sender_email, sender_name, subject, received_at,
                      raw_html, plain_text) -> int:
    """Insert a new newsletter row and return its id."""
    conn = get_conn()
    with conn:
        cur = conn.execute(
            """
            INSERT INTO newsletters
                (sender_email, sender_name, subject, received_at, raw_html, plain_text)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (sender_email, sender_name, subject, received_at, raw_html, plain_text),
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


def get_newsletters_for_date(date_str: str):
    """Return all newsletters received on a given YYYY-MM-DD date."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM newsletters WHERE date(received_at) = ? ORDER BY received_at ASC",
        (date_str,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


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


if __name__ == "__main__":
    init_db()
