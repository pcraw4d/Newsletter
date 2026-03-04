"""
email_parser.py — Utilities for parsing raw MIME emails into structured data.

Handles:
  - Multipart MIME (text/plain + text/html)
  - HTML → clean plaintext stripping
  - Outbound link extraction (for article fetching in Phase 2)
  - Sender / subject / date extraction
"""

import email
import re
from email.header import decode_header
from email.utils import parseaddr, parsedate_to_datetime
from datetime import datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Header helpers
# ---------------------------------------------------------------------------

def _decode_header_value(raw_value: str) -> str:
    """Decode a potentially RFC-2047-encoded header value to a plain string."""
    if not raw_value:
        return ""
    parts = decode_header(raw_value)
    decoded = []
    for chunk, charset in parts:
        if isinstance(chunk, bytes):
            decoded.append(chunk.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(chunk)
    return " ".join(decoded).strip()


def extract_sender(msg) -> tuple[str, str]:
    """Return (name, email_address) from the From header."""
    raw = _decode_header_value(msg.get("From", ""))
    name, addr = parseaddr(raw)
    return name.strip(), addr.strip().lower()


def extract_subject(msg) -> str:
    return _decode_header_value(msg.get("Subject", "(no subject)"))


def extract_date(msg) -> str:
    """Return an ISO-8601 UTC timestamp string from the Date header."""
    raw = msg.get("Date", "")
    try:
        dt = parsedate_to_datetime(raw)
        # Normalise to UTC
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        # Fall back to now if the date header is malformed
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Body extraction
# ---------------------------------------------------------------------------

def _html_to_text(html: str) -> str:
    """Strip HTML tags and return readable plaintext."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove elements that add noise
    for tag in soup(["script", "style", "head", "nav", "footer",
                     "img", "svg", "button", "form"]):
        tag.decompose()

    # Preserve some structure with newlines
    for tag in soup.find_all(["p", "br", "h1", "h2", "h3", "h4", "li"]):
        tag.insert_before("\n")

    text = soup.get_text(separator=" ")
    # Collapse excess whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_bodies(msg) -> tuple[str, str]:
    """
    Walk the MIME tree and return (raw_html, plain_text).
    Prefers text/html for raw_html and derives plaintext from it.
    Falls back to text/plain if no HTML part exists.
    """
    html_body = ""
    plain_body = ""

    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            disp = str(part.get("Content-Disposition", ""))
            if "attachment" in disp:
                continue
            charset = part.get_content_charset() or "utf-8"
            try:
                payload = part.get_payload(decode=True)
                if payload is None:
                    continue
                text = payload.decode(charset, errors="replace")
            except Exception:
                continue

            if ct == "text/html" and not html_body:
                html_body = text
            elif ct == "text/plain" and not plain_body:
                plain_body = text
    else:
        charset = msg.get_content_charset() or "utf-8"
        try:
            payload = msg.get_payload(decode=True)
            text = payload.decode(charset, errors="replace") if payload else ""
        except Exception:
            text = ""

        if msg.get_content_type() == "text/html":
            html_body = text
        else:
            plain_body = text

    # Derive plain text from HTML if we have it
    if html_body and not plain_body:
        plain_body = _html_to_text(html_body)

    return html_body, plain_body


# ---------------------------------------------------------------------------
# Link extraction
# ---------------------------------------------------------------------------

# Domains to skip — tracking pixels, unsubscribe links, social icons, etc.
_SKIP_DOMAINS = {
    "unsubscribe", "mailchimp.com", "list-manage.com", "sendgrid.net",
    "beehiiv.com", "substack.com", "ghost.io", "convertkit.com",
    "klaviyo.com", "constantcontact.com", "twitter.com", "x.com",
    "facebook.com", "linkedin.com", "instagram.com", "youtube.com",
    "t.co", "bit.ly", "ow.ly",
}

_SKIP_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg",
                    ".zip", ".mp4", ".mov"}


def extract_article_links(html: str) -> list[str]:
    """
    Parse all <a href> links from HTML and return a deduplicated list
    of URLs that look like real article links (not tracking/social/media).
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    seen = set()
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()

        # Must look like a full URL
        if not href.startswith(("http://", "https://")):
            continue

        parsed = urlparse(href)
        domain = parsed.netloc.lower().replace("www.", "")

        # Skip known noise domains
        if any(skip in domain for skip in _SKIP_DOMAINS):
            continue

        # Skip non-article file types
        path_lower = parsed.path.lower()
        if any(path_lower.endswith(ext) for ext in _SKIP_EXTENSIONS):
            continue

        # Skip very short paths (likely homepage links)
        if len(parsed.path.strip("/")) < 3:
            continue

        if href not in seen:
            seen.add(href)
            links.append(href)

    return links


# ---------------------------------------------------------------------------
# Main parse entry point
# ---------------------------------------------------------------------------

def parse_raw_email(raw_bytes: bytes) -> dict:
    """
    Parse a raw email (bytes) into a structured dict ready for DB insertion.

    Returns:
    {
        "sender_email": str,
        "sender_name": str,
        "subject": str,
        "received_at": str,     # ISO-8601 UTC
        "raw_html": str,
        "plain_text": str,
        "article_links": [str], # extracted outbound URLs
    }
    """
    msg = email.message_from_bytes(raw_bytes)

    sender_name, sender_email = extract_sender(msg)
    subject = extract_subject(msg)
    received_at = extract_date(msg)
    raw_html, plain_text = extract_bodies(msg)
    article_links = extract_article_links(raw_html)

    return {
        "sender_email": sender_email,
        "sender_name": sender_name,
        "subject": subject,
        "received_at": received_at,
        "raw_html": raw_html,
        "plain_text": plain_text,
        "article_links": article_links,
    }
