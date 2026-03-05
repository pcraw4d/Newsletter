"""
job_fetcher.py — Fetch PM job postings from the Adzuna API and insert them
into the database.

Adzuna requires a free API key from api.adzuna.com.

Config is read from environment variables:
    ADZUNA_APP_ID    — from api.adzuna.com (free registration)
    ADZUNA_APP_KEY   — from api.adzuna.com
    ADZUNA_COUNTRY   — default "us"
"""

import logging
import os
import time

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

logger = logging.getLogger(__name__)


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
            print(f"  🔍 [{query}] [{location}] → {len(raw_results)} results")

    return all_postings


def run_job_fetch(analysis_id: int) -> int:
    """
    Fetch all postings and insert them into the database under the given
    analysis_id. Skips postings with empty descriptions (< 50 chars).
    Returns the count of postings successfully inserted.
    Prints a summary line:
        "  ✅ Job fetch complete — {inserted} inserted | {skipped} skipped (dupe/empty)"
    """
    from database import insert_job_posting

    postings = fetch_all_postings()
    empty_count = 0
    inserted = 0

    for p in postings:
        desc = p.get("description", "") or ""
        if len(desc) < 50:
            empty_count += 1
            continue
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

    skipped = empty_count + (len(postings) - empty_count - inserted)
    print(f"  ✅ Job fetch complete — {inserted} inserted | {skipped} skipped (dupe/empty)")
    return inserted


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    from database import init_db
    init_db()
    postings = fetch_all_postings()
    print(f"Total unique postings: {len(postings)}")
