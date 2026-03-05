"""
article_fetcher.py — Fetch linked articles from newsletter URLs and extract
readable plaintext for the AI processing pipeline.

Handles:
  - Timeouts and connection errors gracefully
  - Paywalled / bot-blocked pages (returns partial content with a flag)
  - Heavy JS-rendered pages (fetches raw HTML, extracts what it can)
  - Token budget: Gemini Flash 2.0 has a 1M token context window, so we can be generous.
  Truncation is a last-resort safeguard against truly enormous pages, not a routine cap.
"""

import re
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT = 10        # seconds per request
MAX_TEXT_CHARS  = 80_000    # ~20k tokens — generous limit; Gemini's 1M context handles this easily

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Domains known to aggressively block scrapers — we skip fetch and note it
BLOCKED_DOMAINS = {
    "wsj.com", "ft.com", "bloomberg.com", "nytimes.com",
    "washingtonpost.com", "economist.com", "theathletic.com",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_likely_blocked(domain: str) -> bool:
    return any(b in domain for b in BLOCKED_DOMAINS)


def _extract_title(soup: BeautifulSoup) -> str:
    """Try several common title locations."""
    # og:title is usually the cleanest
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()

    # twitter:title
    tw = soup.find("meta", attrs={"name": "twitter:title"})
    if tw and tw.get("content"):
        return tw["content"].strip()

    # <title> tag
    if soup.title and soup.title.string:
        return soup.title.string.strip()

    return ""


def _extract_main_text(soup: BeautifulSoup) -> str:
    """
    Extract the article body text.
    Prefers <article> or <main> tags; falls back to full body stripping.
    """
    # Remove noise elements
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "aside", "form", "button", "svg", "img",
                     "figure", "figcaption", "iframe", "noscript"]):
        tag.decompose()

    # Try to find the article container
    body = (
        soup.find("article")
        or soup.find("main")
        or soup.find(attrs={"role": "main"})
        or soup.find(class_=re.compile(r"(article|post|content|body|entry)", re.I))
        or soup.body
    )

    if not body:
        return ""

    # Add newlines around block elements for readability
    for tag in body.find_all(["p", "h1", "h2", "h3", "h4", "li", "br"]):
        tag.insert_before("\n")

    text = body.get_text(separator=" ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _truncate(text: str, max_chars: int = MAX_TEXT_CHARS) -> str:
    """
    Trim to max_chars at a sentence boundary so we don't cut mid-thought.
    """
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    # Try to end at the last full sentence
    last_period = truncated.rfind(". ")
    if last_period > max_chars * 0.8:
        truncated = truncated[: last_period + 1]
    return truncated + "\n\n[Article truncated — showing first portion only]"


# ---------------------------------------------------------------------------
# Main fetch function
# ---------------------------------------------------------------------------

def fetch_article(url: str) -> dict:
    """
    Fetch a URL and extract its readable text.

    Returns a dict:
    {
        "url":     str,
        "title":   str,
        "text":    str,   # extracted readable text (may be empty on failure)
        "status":  "ok" | "blocked" | "failed",
        "note":    str,   # human-readable explanation on non-ok status
    }
    """
    domain = urlparse(url).netloc.lower().replace("www.", "")

    # Skip known paywalled domains upfront
    if _is_likely_blocked(domain):
        return {
            "url": url,
            "title": "",
            "text": "",
            "status": "blocked",
            "note": f"Skipped — {domain} is behind a paywall",
        }

    try:
        resp = requests.get(
            url,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        resp.raise_for_status()

        # Only process HTML responses
        content_type = resp.headers.get("Content-Type", "")
        if "text/html" not in content_type:
            return {
                "url": url,
                "title": "",
                "text": "",
                "status": "failed",
                "note": f"Non-HTML content type: {content_type}",
            }

        soup = BeautifulSoup(resp.text, "html.parser")
        title = _extract_title(soup)
        raw_text = _extract_main_text(soup)

        if not raw_text or len(raw_text) < 100:
            return {
                "url": url,
                "title": title,
                "text": "",
                "status": "failed",
                "note": "Could not extract meaningful text (possibly JS-rendered)",
            }

        return {
            "url": url,
            "title": title,
            "text": _truncate(raw_text),
            "status": "ok",
            "note": "",
        }

    except requests.exceptions.Timeout:
        return {"url": url, "title": "", "text": "", "status": "failed",
                "note": "Request timed out"}
    except requests.exceptions.TooManyRedirects:
        return {"url": url, "title": "", "text": "", "status": "failed",
                "note": "Too many redirects"}
    except requests.exceptions.HTTPError as e:
        return {"url": url, "title": "", "text": "", "status": "failed",
                "note": f"HTTP {e.response.status_code}"}
    except Exception as e:
        return {"url": url, "title": "", "text": "", "status": "failed",
                "note": str(e)[:120]}


def fetch_articles(urls: list[str]) -> list[dict]:
    """Fetch multiple URLs sequentially. Returns results in the same order."""
    results = []
    for url in urls:
        result = fetch_article(url)
        status_icon = "✅" if result["status"] == "ok" else "⚠️ "
        print(f"  {status_icon} [{result['status']:7}] {url[:80]}")
        if result["note"]:
            print(f"            {result['note']}")
        results.append(result)
    return results
