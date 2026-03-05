"""
gmail_poller.py — Fetch newsletters from Gmail and ingest them into Briefly.

How it works:
  1. Connect to Gmail API using stored OAuth credentials
  2. Search for unread emails in a configured label (default: "Newsletters")
     OR from a list of known sender addresses in .env
  3. Parse each email with the existing email_parser module
  4. Insert into the database (deduplicating by Message-ID)
  5. Mark each Gmail message as read so it won't be re-fetched next time

Configuration (set in .env or Railway env vars):
  GMAIL_LABEL       — Gmail label to watch (default: "Newsletters")
                      Create this label in Gmail and apply it via filters.
  GMAIL_SENDERS     — Comma-separated sender addresses to watch instead of a label
                      e.g. "editor@netinterest.co,brew@morningbrew.com"
                      If both are set, label takes priority.
  GMAIL_DAYS_BACK   — How many days back to look on first run (default: 1)
  GMAIL_MAX_FETCH   — Max emails to fetch per run (default: 50, safety cap)
"""

import base64
import hashlib
import os
import re
import time
from datetime import datetime, timezone, timedelta

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from gmail_auth import get_credentials
from email_parser import parse_raw_email
from database import insert_newsletter, get_conn, get_newsletter_by_fingerprint

# ---------------------------------------------------------------------------
# Junk subject filter
# ---------------------------------------------------------------------------

_JUNK_SUBJECT_RE = re.compile(
    r"(?:"
    r"order\s+confirmation|receipt\s+(?:for|#)|invoice\s+(?:for|#)?|"
    r"shipment\s+confirmation|delivery\s+confirmation|payment\s+confirmation|"
    r"booking\s+confirmation|"
    r"password\s+reset|reset\s+your\s+password|verify\s+your\s+email|"
    r"email\s+verification|account\s+confirmation|"
    r"welcome\s+to\s+|you['']ve\s+been\s+invited|you['']ve\s+been\s+added|"
    r"you['']ve\s+been\s+subscribed|you\s+have\s+been\s+invited|"
    r"\d+%\s*off|\boff\s+\d+%|limited\s+time\s+offer|flash\s+sale|"
    r"thank\s+you\s+for\s+your\s+(?:order|purchase|payment)|"
    r"your\s+\w+(?:\s+\w+)?\s+is\s+(?:ready|confirmed|on\s+its\s+way)|"
    r"unsubscribe|opt[-\s]?out(?:\s+of\s+emails)?"
    r")",
    re.IGNORECASE,
)


def _is_junk_subject(subject: str) -> bool:
    """Return True if the subject matches common transactional/promotional patterns."""
    return bool(_JUNK_SUBJECT_RE.search(subject or ""))


def _content_fingerprint(sender_email: str, subject: str, plain_text: str) -> str:
    """
    Compute a content-based fingerprint for deduplication.
    Normalises inputs and returns the first 16 chars of SHA-256 hex digest.
    """
    sender = (sender_email or "").lower().strip()
    subj = (subject or "").lower().strip()
    text = (plain_text or "").strip()[:500]
    payload = f"{sender}:{subj}:{text}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GMAIL_LABEL     = os.getenv("GMAIL_LABEL", "Newsletters")
GMAIL_SENDERS   = os.getenv("GMAIL_SENDERS", "")       # comma-separated
GMAIL_DAYS_BACK = int(os.getenv("GMAIL_DAYS_BACK", "1"))
GMAIL_MAX_FETCH = int(os.getenv("GMAIL_MAX_FETCH", "50"))

MIN_PLAIN_TEXT_WORDS = 80


# ---------------------------------------------------------------------------
# Deduplication: track which Gmail message IDs we've already ingested
# ---------------------------------------------------------------------------

def _ensure_gmail_ids_table():
    """Create a tracking table for ingested Gmail message IDs if needed."""
    conn = get_conn()
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gmail_ingested (
                gmail_message_id TEXT PRIMARY KEY,
                ingested_at      TEXT DEFAULT (datetime('now'))
            )
        """)
    conn.close()


def _already_ingested(gmail_message_id: str) -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT 1 FROM gmail_ingested WHERE gmail_message_id = ?",
        (gmail_message_id,)
    ).fetchone()
    conn.close()
    return row is not None


def _mark_ingested(gmail_message_id: str):
    conn = get_conn()
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO gmail_ingested (gmail_message_id) VALUES (?)",
            (gmail_message_id,)
        )
    conn.close()


# ---------------------------------------------------------------------------
# Gmail API helpers
# ---------------------------------------------------------------------------

def _build_service():
    creds = get_credentials()
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _build_query() -> str:
    """
    Build the Gmail search query string.
    Prefers label-based search; falls back to sender list.
    """
    # Date filter — only fetch emails newer than GMAIL_DAYS_BACK days
    since = (datetime.now(timezone.utc) - timedelta(days=GMAIL_DAYS_BACK))
    date_filter = f"after:{since.strftime('%Y/%m/%d')}"

    if GMAIL_LABEL:
        # Gmail label query — wrap in quotes if it contains spaces
        label = GMAIL_LABEL.strip()
        label_q = f'label:"{label}"' if ' ' in label else f"label:{label}"
        return f"{label_q} {date_filter}"

    if GMAIL_SENDERS:
        senders = [s.strip() for s in GMAIL_SENDERS.split(",") if s.strip()]
        from_q = " OR ".join(f"from:{s}" for s in senders)
        return f"({from_q}) {date_filter}"

    # Fallback: anything that looks like a newsletter in the inbox
    return f"in:inbox {date_filter} unsubscribe"


def _get_raw_message(service, message_id: str) -> bytes | None:
    """Fetch the full raw RFC-2822 bytes of a Gmail message."""
    try:
        msg = service.users().messages().get(
            userId="me",
            id=message_id,
            format="raw"
        ).execute()
        raw = msg.get("raw", "")
        return base64.urlsafe_b64decode(raw + "==")   # pad for safety
    except HttpError as e:
        print(f"   ⚠️  Could not fetch message {message_id}: {e}")
        return None


def _mark_read(service, message_id: str):
    """Remove the UNREAD label from a Gmail message."""
    try:
        service.users().messages().modify(
            userId="me",
            id=message_id,
            body={"removeLabelIds": ["UNREAD"]}
        ).execute()
    except HttpError as e:
        print(f"   ⚠️  Could not mark {message_id} as read: {e}")


# ---------------------------------------------------------------------------
# Main polling function
# ---------------------------------------------------------------------------

def poll_gmail() -> dict:
    """
    Fetch new newsletter emails from Gmail and insert them into the database.

    Returns a summary dict:
    {
        "fetched":    int,   # emails found in Gmail matching the query
        "ingested":   int,   # new emails added to the database
        "skipped":    int,   # already-ingested duplicates skipped
        "failed":     int,   # emails that failed to parse/insert
        "query":      str,   # the Gmail search query used
    }
    """
    _ensure_gmail_ids_table()

    print(f"\n📬 Gmail Poller starting…")
    print(f"   Label      : {GMAIL_LABEL or '(none)'}")
    print(f"   Senders    : {GMAIL_SENDERS or '(none)'}")
    print(f"   Days back  : {GMAIL_DAYS_BACK}")

    try:
        service = _build_service()
    except Exception as e:
        print(f"   ❌ Could not connect to Gmail API: {e}")
        return {"fetched": 0, "ingested": 0, "skipped": 0, "junk_skipped": 0, "failed": 0, "query": ""}

    query = _build_query()
    print(f"   Query      : {query}")

    # List matching messages
    try:
        result = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=GMAIL_MAX_FETCH
        ).execute()
    except HttpError as e:
        print(f"   ❌ Gmail list error: {e}")
        return {"fetched": 0, "ingested": 0, "skipped": 0, "junk_skipped": 0, "failed": 0, "query": query}

    messages = result.get("messages", [])
    print(f"   Found {len(messages)} message(s) matching query\n")

    ingested = skipped = failed = junk_skipped = 0

    for msg_meta in messages:
        gmail_id = msg_meta["id"]

        # Dedup check
        if _already_ingested(gmail_id):
            skipped += 1
            continue

        # Fetch raw bytes
        raw_bytes = _get_raw_message(service, gmail_id)
        if not raw_bytes:
            failed += 1
            continue

        # Parse with existing email_parser
        try:
            parsed = parse_raw_email(raw_bytes)
        except Exception as e:
            print(f"   ❌ Parse failed for {gmail_id}: {e}")
            failed += 1
            continue

        plain_text = parsed["plain_text"] or ""
        raw_html = parsed["raw_html"] or ""
        fingerprint = _content_fingerprint(
            parsed["sender_email"], parsed["subject"], plain_text
        )

        # Junk subject filter — insert with skipped_reason so it's visible in API
        if _is_junk_subject(parsed["subject"]):
            junk_skipped += 1
            print(f"   ⛔ [junk subject] {parsed['subject'][:60]}")
            try:
                insert_newsletter(
                    sender_email=parsed["sender_email"],
                    sender_name=parsed["sender_name"],
                    subject=parsed["subject"],
                    received_at=parsed["received_at"],
                    raw_html=raw_html,
                    plain_text=plain_text,
                    content_fingerprint=fingerprint,
                    skipped_reason="junk subject",
                )
                _mark_ingested(gmail_id)
                _mark_read(service, gmail_id)
            except Exception as e:
                print(f"   ⚠️  Could not record skipped email: {e}")
            continue

        # Content quality: minimum word count
        word_count = len(plain_text.split())
        if word_count < MIN_PLAIN_TEXT_WORDS:
            junk_skipped += 1
            print(f"   ⛔ [too short — {word_count} words] {parsed['subject'][:60]}")
            try:
                insert_newsletter(
                    sender_email=parsed["sender_email"],
                    sender_name=parsed["sender_name"],
                    subject=parsed["subject"],
                    received_at=parsed["received_at"],
                    raw_html=raw_html,
                    plain_text=plain_text,
                    content_fingerprint=fingerprint,
                    skipped_reason=f"too short ({word_count} words)",
                )
                _mark_ingested(gmail_id)
                _mark_read(service, gmail_id)
            except Exception as e:
                print(f"   ⚠️  Could not record skipped email: {e}")
            continue

        # Content quality: text density (only for HTML-heavy emails)
        if len(raw_html) > 2000:
            ratio = len(plain_text) / len(raw_html)
            if ratio < 0.03:
                junk_skipped += 1
                print(f"   ⛔ [low text density — {ratio:.1%}] {parsed['subject'][:60]}")
                try:
                    insert_newsletter(
                        sender_email=parsed["sender_email"],
                        sender_name=parsed["sender_name"],
                        subject=parsed["subject"],
                        received_at=parsed["received_at"],
                        raw_html=raw_html,
                        plain_text=plain_text,
                        content_fingerprint=fingerprint,
                        skipped_reason=f"low text density ({ratio:.1%})",
                    )
                    _mark_ingested(gmail_id)
                    _mark_read(service, gmail_id)
                except Exception as e:
                    print(f"   ⚠️  Could not record skipped email: {e}")
                continue

        # Content-based deduplication (re-sends, A/B variants with different Gmail IDs)
        existing = get_newsletter_by_fingerprint(fingerprint)
        if existing:
            junk_skipped += 1
            print(f"   ⛔ [duplicate content — id={existing['id']}] {parsed['subject'][:60]}")
            _mark_ingested(gmail_id)
            _mark_read(service, gmail_id)
            continue

        # Insert into DB
        try:
            nl_id = insert_newsletter(
                sender_email=parsed["sender_email"],
                sender_name=parsed["sender_name"],
                subject=parsed["subject"],
                received_at=parsed["received_at"],
                raw_html=parsed["raw_html"],
                plain_text=parsed["plain_text"],
                content_fingerprint=fingerprint,
            )
            _mark_ingested(gmail_id)
            _mark_read(service, gmail_id)

            print(f"   ✅ [{nl_id}] {parsed['sender_name']} — {parsed['subject'][:55]}")
            print(f"         {len(parsed['article_links'])} link(s) found")
            ingested += 1

        except Exception as e:
            print(f"   ❌ DB insert failed for {gmail_id}: {e}")
            failed += 1

        # Small delay to stay well within Gmail API rate limits
        time.sleep(0.3)

    print(f"\n   Poll complete — ingested: {ingested} | skipped: {skipped} | junk: {junk_skipped} | failed: {failed}")

    return {
        "fetched":      len(messages),
        "ingested":     ingested,
        "skipped":      skipped,
        "junk_skipped": junk_skipped,
        "failed":       failed,
        "query":        query,
    }


if __name__ == "__main__":
    from database import init_db
    from dotenv import load_dotenv
    load_dotenv()
    init_db()
    result = poll_gmail()
    print(result)
