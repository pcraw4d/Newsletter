"""
job_fetcher.py — Fetch PM job postings from the Adzuna API and insert them
into the database.

Adzuna requires a free API key from api.adzuna.com.

Config is read from environment variables:
    ADZUNA_APP_ID    — from api.adzuna.com (free registration)
    ADZUNA_APP_KEY   — from api.adzuna.com
    ADZUNA_COUNTRY   — default "us"
"""

import hashlib
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SEARCH_QUERIES = [
    "product manager",
    "senior product manager",
    "staff product manager",
]
SEARCH_LOCATIONS = ["New York City", "remote"]
RESULTS_PER_PAGE = 50
MAX_DAYS_OLD = 7  # only fetch postings from the past 7 days

REQUEST_TIMEOUT = 15
RATE_LIMIT_WAIT = 5
RATE_LIMIT_RETRIES = 1

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# ---------------------------------------------------------------------------
# Pre-processing filter config
# ---------------------------------------------------------------------------

# How many days old a posting can be (enforced client-side after Adzuna fetch)
MAX_POSTING_AGE_DAYS = 7

# Minimum description length in characters before we bother with AI extraction
MIN_DESCRIPTION_CHARS = 300

# Maximum postings to keep per company (prevents one employer dominating signal)
MAX_POSTINGS_PER_COMPANY = 2

# Job title must contain one of these patterns to be considered a PM IC role
_TITLE_REQUIRE_RE = re.compile(
    r'\bproduct manager\b',
    re.IGNORECASE,
)

# Job titles matching any of these are dropped regardless of the above
_TITLE_REJECT_RE = re.compile(
    r'\b('
    r'project manager|program manager|product marketing manager|'
    r'product marketing|marketing manager|sales manager|'
    r'account manager|office manager|general manager|'
    r'district manager|property manager|operations manager|'
    r'category manager|brand manager|'
    r'associate product manager|APM|'
    r'junior product manager|intern|coordinator|'
    r'director of product|vp of product|vp product|'
    r'chief product officer|CPO|head of product|'
    r'principal product manager'
    r')\b',
    re.IGNORECASE,
)

# Company names that are staffing/recruiting agencies — their postings hide
# the real employer and inflate duplicate signal
_AGENCY_NAMES = {
    "teksystems", "tek systems",
    "insight global",
    "robert half",
    "hays", "hays plc",
    "kforce",
    "modis",
    "hired",
    "toptal",
    "revature",
    "experis",
    "ciber",
    "apex systems",
    "staffmark",
    "adecco",
    "manpower", "manpowergroup",
    "randstad",
    "kelly services", "kelly",
    "spherion",
    "aerotek",
    "volt information sciences", "volt",
    "judge group",
    "motion recruitment",
    "talener",
    "cybercoders",
    "dice",
    "lancesoft",
    "vivo",
    "epitec",
    "numentica",
    "jobgether",
    "iqtalent", "iq talent",
}

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pre-processing filter functions
# ---------------------------------------------------------------------------

def _filter_by_date(postings: list[dict]) -> tuple[list[dict], int]:
    """
    Drop postings older than MAX_POSTING_AGE_DAYS.
    Returns (kept, dropped_count).
    Falls through (keeps) any posting with an unparseable posted_at date
    so we never silently discard data we can't evaluate.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_POSTING_AGE_DAYS)
    kept, dropped = [], 0
    for p in postings:
        posted_at = p.get("posted_at", "")
        try:
            dt = datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
            if dt < cutoff:
                dropped += 1
                continue
        except (ValueError, AttributeError):
            pass  # unparseable date — keep the posting
        kept.append(p)
    return kept, dropped


def _filter_by_title(postings: list[dict]) -> tuple[list[dict], int]:
    """
    Keep only postings whose title matches _TITLE_REQUIRE_RE
    AND does not match _TITLE_REJECT_RE.
    Returns (kept, dropped_count).
    """
    kept, dropped = [], 0
    for p in postings:
        title = p.get("title", "") or ""
        if _TITLE_REJECT_RE.search(title):
            dropped += 1
            continue
        if not _TITLE_REQUIRE_RE.search(title):
            dropped += 1
            continue
        kept.append(p)
    return kept, dropped


def _filter_by_agency(postings: list[dict]) -> tuple[list[dict], int]:
    """
    Drop postings where the company name matches a known staffing agency.
    Match is case-insensitive, full-string after stripping whitespace.
    Returns (kept, dropped_count).
    """
    kept, dropped = [], 0
    for p in postings:
        company = (p.get("company") or "").lower().strip()
        if company in _AGENCY_NAMES:
            dropped += 1
            continue
        kept.append(p)
    return kept, dropped


def _filter_by_description(postings: list[dict]) -> tuple[list[dict], int]:
    """
    Drop postings whose description is shorter than MIN_DESCRIPTION_CHARS
    characters. Short descriptions contain insufficient signal for skill
    extraction and waste AI calls.
    Returns (kept, dropped_count).
    """
    kept, dropped = [], 0
    for p in postings:
        desc = p.get("description", "") or ""
        if len(desc) < MIN_DESCRIPTION_CHARS:
            dropped += 1
            continue
        kept.append(p)
    return kept, dropped


def _dedupe_by_company(postings: list[dict]) -> tuple[list[dict], int]:
    """
    Cap postings at MAX_POSTINGS_PER_COMPANY per unique company name.
    Also deduplicates by description fingerprint (SHA-256 of first 300 chars)
    to catch the same posting re-listed under slightly different titles.
    Returns (kept, dropped_count).
    """
    company_counts: Counter = Counter()
    seen_fingerprints: set[str] = set()
    kept, dropped = [], 0

    for p in postings:
        # Description fingerprint check first
        desc = (p.get("description") or "")[:300]
        fingerprint = hashlib.sha256(desc.encode("utf-8")).hexdigest()
        if fingerprint in seen_fingerprints:
            dropped += 1
            continue
        seen_fingerprints.add(fingerprint)

        # Per-company cap
        company = (p.get("company") or "unknown").lower().strip()
        if company_counts[company] >= MAX_POSTINGS_PER_COMPANY:
            dropped += 1
            continue
        company_counts[company] += 1
        kept.append(p)

    return kept, dropped


def _apply_preprocess_pipeline(postings: list[dict]) -> list[dict]:
    """
    Run all five pre-processing filters in sequence.
    Prints a per-stage summary so every run shows exactly what was dropped
    and why. Returns the filtered list ready for DB insertion and AI extraction.
    """
    initial = len(postings)

    postings, n_date    = _filter_by_date(postings)
    postings, n_title   = _filter_by_title(postings)
    postings, n_agency  = _filter_by_agency(postings)
    postings, n_desc    = _filter_by_description(postings)
    postings, n_company = _dedupe_by_company(postings)

    final = len(postings)
    total_dropped = initial - final

    print(f"  📋 Pre-processing pipeline: {initial} → {final} postings")
    print(f"     Stale date    : -{n_date}")
    print(f"     Title mismatch: -{n_title}")
    print(f"     Agency posting: -{n_agency}")
    print(f"     Short desc    : -{n_desc}")
    print(f"     Company dedupe: -{n_company}")
    print(f"     Total dropped : -{total_dropped}  ({final} remaining)")

    return postings


def _fetch_adzuna_page(query: str, location: str, page: int = 1) -> list[dict]:
    """
    Fetch one page of results from Adzuna for a given query + location.
    Returns a list of raw result dicts from the API.
    On any HTTP or connection error, logs the error and returns [].
    """
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    country = os.getenv("ADZUNA_COUNTRY", "us")

    if not app_id or not app_key:
        logger.error("ADZUNA_APP_ID and ADZUNA_APP_KEY must be set")
        return []

    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": RESULTS_PER_PAGE,
        "what": query,
        "where": location,
        "max_days_old": MAX_DAYS_OLD,
        "content-type": "application/json",
    }

    for attempt in range(RATE_LIMIT_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 429:
                if attempt < RATE_LIMIT_RETRIES:
                    logger.warning("Adzuna rate limit (429), waiting %ds before retry", RATE_LIMIT_WAIT)
                    time.sleep(RATE_LIMIT_WAIT)
                else:
                    logger.error("Adzuna rate limit (429) after retry")
                    return []
            else:
                resp.raise_for_status()
                data = resp.json()
                return data.get("results", [])
        except requests.exceptions.Timeout:
            logger.error("Adzuna request timed out: %s [%s]", query, location)
            return []
        except requests.exceptions.RequestException as e:
            logger.error("Adzuna request failed: %s", e)
            return []
        except (ValueError, KeyError) as e:
            logger.error("Adzuna response parse error: %s", e)
            return []

    return []


def _normalise_result(raw: dict) -> dict:
    """Convert a raw Adzuna result to our normalised posting format."""
    company = raw.get("company") or {}
    location_obj = raw.get("location") or {}
    if isinstance(company, dict):
        company = company.get("display_name", "") or ""
    else:
        company = str(company)
    if isinstance(location_obj, dict):
        location_str = location_obj.get("display_name", "") or ""
    else:
        location_str = str(location_obj)

    return {
        "external_id": str(raw.get("id", "")),
        "title": str(raw.get("title", "") or ""),
        "company": company,
        "location": location_str,
        "description": str(raw.get("description", "") or ""),
        "posted_at": str(raw.get("created", "") or ""),
    }


def fetch_all_postings() -> list[dict]:
    """
    Iterate over all SEARCH_QUERIES × SEARCH_LOCATIONS combinations.
    Fetch page 1 for each combination (50 results = one page is enough).
    Deduplicate results by external_id across all queries.
    Returns a flat list of normalised posting dicts, each with keys:
        external_id, title, company, location, description, posted_at
    Prints progress: "  🔍 [{query}] [{location}] → {n} results"
    """
    seen_ids: set[str] = set()
    all_postings: list[dict] = []

    for query in SEARCH_QUERIES:
        for location in SEARCH_LOCATIONS:
            raw_results = _fetch_adzuna_page(query, location, page=1)
            normalised = [_normalise_result(r) for r in raw_results]
            new_count = 0
            for p in normalised:
                eid = p.get("external_id", "")
                if eid and eid not in seen_ids:
                    seen_ids.add(eid)
                    all_postings.append(p)
                    new_count += 1
            print(f"  🔍 [{query}] [{location}] → {len(raw_results)} results ({new_count} new after external_id dedupe)")

    # Apply pre-processing pipeline before returning
    all_postings = _apply_preprocess_pipeline(all_postings)
    return all_postings


def run_job_fetch(analysis_id: int) -> int:
    """
    Fetch all postings and insert them into the database under the given
    analysis_id. Skips postings that already exist (db conflict).
    Returns the count of postings successfully inserted.
    Prints a summary line:
        "  ✅ Job fetch complete — {inserted} inserted | {skipped} skipped (db conflict)"
    """
    from database import insert_job_posting

    postings = fetch_all_postings()
    inserted = 0

    for p in postings:
        desc = p.get("description", "") or ""
        row_id = insert_job_posting(
            analysis_id=analysis_id,
            external_id=p["external_id"],
            title=p["title"],
            company=p["company"],
            location=p["location"],
            description=desc,
            posted_at=p["posted_at"],
        )
        if row_id and row_id > 0:
            inserted += 1

    print(f"  ✅ Job fetch complete — {inserted} inserted | {len(postings) - inserted} skipped (db conflict)")
    return inserted


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    from database import init_db
    init_db()
    postings = fetch_all_postings()
    print(f"Total unique postings: {len(postings)}")
