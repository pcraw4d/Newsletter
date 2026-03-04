"""
processor.py — AI processing pipeline for Briefly.

Two main jobs:
  1. process_newsletter(id)  — per-newsletter: extract takeaways + summarise articles
  2. run_synthesis(date)     — cross-newsletter: identify themes across all newsletters

Both call Groq's API (Llama 3.3 70B) via the OpenAI-compatible client and store
results back to SQLite.

Switching providers: set MODEL_PROVIDER=together or =openrouter in .env — no code changes needed.
"""

import json
import os
import re
import time
from datetime import date as date_type

from openai import OpenAI
from dotenv import load_dotenv

from database import (
    get_unprocessed_newsletters,
    get_newsletters_for_date,
    get_takeaways_for_newsletter,
    insert_article,
    insert_takeaway,
    delete_takeaways_for_newsletter,
    insert_theme,
    delete_themes_for_date,
    mark_newsletter_processed,
)
from email_parser import extract_article_links
from article_fetcher import fetch_articles

load_dotenv()

# ---------------------------------------------------------------------------
# Model client — swap provider by changing env vars only
# ---------------------------------------------------------------------------

PROVIDER = os.getenv("MODEL_PROVIDER", "groq").lower()

_PROVIDERS = {
    "groq": {
        "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY",
        "model": "llama-3.3-70b-versatile",
    },
    "together": {
        "base_url": "https://api.together.xyz/v1",
        "api_key_env": "TOGETHER_API_KEY",
        "model": "meta-llama/Llama-3.3-70B-Instruct-Turbo",
    },
    "openrouter": {
        "base_url": "https://openrouter.ai/api/v1",
        "api_key_env": "OPENROUTER_API_KEY",
        "model": "meta-llama/llama-3.3-70b-instruct",
    },
}

_cfg = _PROVIDERS.get(PROVIDER, _PROVIDERS["groq"])
MODEL = os.getenv("MODEL_NAME", _cfg["model"])

client = OpenAI(
    api_key=os.getenv(_cfg["api_key_env"], "missing-key"),
    base_url=_cfg["base_url"],
)

# ---------------------------------------------------------------------------
# JSON-safe LLM call with retry
#
# Open source models occasionally wrap JSON in markdown fences or add a brief
# preamble. This wrapper strips common formatting noise and retries up to 3x
# with an escalating instruction to return clean JSON.
# ---------------------------------------------------------------------------

_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)

def _clean_json(raw: str) -> str:
    """Strip markdown fences and leading/trailing prose."""
    # Try to extract fenced block first
    match = _FENCE_RE.search(raw)
    if match:
        return match.group(1).strip()
    # Find the first { or [ and slice from there
    for start_char, end_char in [('{', '}'), ('[', ']')]:
        idx = raw.find(start_char)
        if idx != -1:
            last = raw.rfind(end_char)
            if last > idx:
                return raw[idx:last + 1].strip()
    return raw.strip()


def _llm_call(prompt: str, expect: str = "object", retries: int = 3) -> dict | list:
    """
    Call the model and return parsed JSON.
    expect: 'object' or 'array' — used to craft retry instructions.
    Raises ValueError after all retries exhausted.
    """
    messages = [{"role": "user", "content": prompt}]
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                max_tokens=2000,
                temperature=0.2,          # lower temp = more consistent JSON
                messages=messages,
            )
            raw = resp.choices[0].message.content or ""
            cleaned = _clean_json(raw)
            parsed = json.loads(cleaned)

            # Validate top-level type matches expectation
            if expect == "array" and not isinstance(parsed, list):
                raise ValueError(f"Expected JSON array, got {type(parsed).__name__}")
            if expect == "object" and not isinstance(parsed, dict):
                raise ValueError(f"Expected JSON object, got {type(parsed).__name__}")

            return parsed

        except (json.JSONDecodeError, ValueError) as e:
            last_error = e
            print(f"   ⚠️  Attempt {attempt}/{retries} — JSON parse failed: {e}")
            if attempt < retries:
                # Add assistant turn showing the bad response + a correction nudge
                retry_instruction = (
                    f"Your previous response could not be parsed as valid JSON. "
                    f"Return ONLY a valid JSON {'array' if expect == 'array' else 'object'}. "
                    f"No markdown, no explanation, no preamble."
                )
                messages = [
                    {"role": "user", "content": prompt},
                    {"role": "assistant", "content": raw},
                    {"role": "user", "content": retry_instruction},
                ]
                time.sleep(1)

        except Exception as e:
            print(f"   ❌ API error on attempt {attempt}: {e}")
            last_error = e
            if attempt < retries:
                time.sleep(2 * attempt)

    raise ValueError(f"LLM call failed after {retries} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _build_newsletter_prompt(newsletter: dict, fetched_articles: list[dict]) -> str:
    articles_section = ""
    for i, a in enumerate(fetched_articles, 1):
        if a["status"] == "ok" and a["text"]:
            articles_section += f"""
--- LINKED ARTICLE {i} ---
Title: {a['title']}
URL: {a['url']}
Content:
{a['text'][:8000]}
"""

    return f"""You are an expert analyst processing a newsletter for a busy professional.

NEWSLETTER DETAILS:
Sender: {newsletter['sender_name']} <{newsletter['sender_email']}>
Subject: {newsletter['subject']}

NEWSLETTER BODY:
{newsletter['plain_text'][:6000]}
{articles_section}

Analyse the newsletter body AND any linked articles above, then return a JSON object with this EXACT structure:

{{
  "category": "one of: Fintech & Markets | Product | Venture & Tech | AI & ML | Macro & Policy | Compliance & Risk | Other",
  "takeaways": [
    "Concise, insight-rich bullet. Lead with the finding. Be specific — include numbers, names, decisions where relevant. 1-2 sentences max.",
    "3-5 bullets total. Synthesise across newsletter body AND linked articles."
  ],
  "articles": [
    {{
      "url": "exact URL from the linked articles above",
      "title": "article title",
      "summary": "2-3 sentences on what the article argues or reveals. Be specific."
    }}
  ]
}}

IMPORTANT: Return ONLY the raw JSON object. No markdown fences, no preamble, no explanation.
Do not invent information not present in the source material.
If no articles were fetched, return an empty array for articles."""


def _build_synthesis_prompt(newsletters: list[dict]) -> str:
    content_block = ""
    for n in newsletters:
        bullets = "\n".join(
            f"  • {t['content']}" for t in n.get("takeaways", [])
        )
        content_block += f"\n=== {n['sender_name']} — {n['subject']} ===\n{bullets}\n"

    return f"""You are a senior analyst synthesising intelligence across multiple newsletters received today.

TODAY'S NEWSLETTER TAKEAWAYS:
{content_block}

Identify 2-4 meaningful cross-cutting themes — signals appearing across multiple newsletters OR a single significant standalone development.

Return a JSON array with this EXACT structure:

[
  {{
    "tag": "one of: MACRO SIGNAL | PRODUCT TREND | MARKET MOVE | REGULATORY SHIFT | EMERGING | CONSENSUS VIEW",
    "title": "Single crisp thesis sentence, max 15 words.",
    "summary": "2-4 sentences. Name which newsletters contributed. Include concrete details.",
    "source_names": ["Newsletter Name 1", "Newsletter Name 2"],
    "confidence": "HIGH (3+ sources align) | MEDIUM (2 sources or one strong signal) | LOW (speculative)"
  }}
]

IMPORTANT: Return ONLY the raw JSON array. No markdown fences, no preamble, no explanation.
Order by confidence: HIGH first. Only surface themes the data genuinely supports."""


# ---------------------------------------------------------------------------
# Per-newsletter processing
# ---------------------------------------------------------------------------

def process_newsletter(newsletter: dict) -> bool:
    """
    Full pipeline for one newsletter:
      1. Extract article links from raw HTML
      2. Fetch each article
      3. Call LLM → structured JSON
      4. Store takeaways + article summaries
      5. Mark processed
    """
    nid = newsletter["id"]
    print(f"\n📰 Processing id={nid}: '{newsletter['subject'][:60]}'")
    print(f"   From: {newsletter['sender_name']} <{newsletter['sender_email']}>")

    # 1. Extract links
    links = extract_article_links(newsletter.get("raw_html") or "")
    print(f"   Found {len(links)} article link(s)")

    # 2. Fetch articles
    fetched = []
    if links:
        print("   Fetching linked articles...")
        fetched = fetch_articles(links)

    # 3. Call LLM
    print(f"   Calling {PROVIDER} ({MODEL})...")
    prompt = _build_newsletter_prompt(newsletter, fetched)

    try:
        result = _llm_call(prompt, expect="object")
    except ValueError as e:
        print(f"   ❌ Failed: {e}")
        return False

    # 4a. Store takeaways
    delete_takeaways_for_newsletter(nid)
    takeaways = [t.strip() for t in result.get("takeaways", []) if str(t).strip()]
    for bullet in takeaways:
        insert_takeaway(nid, bullet)
    print(f"   ✅ {len(takeaways)} takeaways stored")

    # 4b. Store article summaries
    fetched_map = {a["url"]: a for a in fetched}
    articles = result.get("articles", [])
    for art in articles:
        url = art.get("url", "")
        if not url:
            continue
        meta = fetched_map.get(url, {})
        insert_article(
            newsletter_id=nid,
            url=url,
            title=art.get("title") or meta.get("title", ""),
            extracted_text=meta.get("text", ""),
            summary=art.get("summary", ""),
            fetch_status=meta.get("status", "ok"),
        )
    print(f"   ✅ {len(articles)} article summaries stored")

    # 5. Mark done
    mark_newsletter_processed(nid)
    return True


# ---------------------------------------------------------------------------
# Cross-newsletter synthesis
# ---------------------------------------------------------------------------

def run_synthesis(target_date: str) -> bool:
    """Identify cross-cutting themes across all processed newsletters for a date."""
    print(f"\n🔬 Running synthesis for {target_date}...")

    all_newsletters = get_newsletters_for_date(target_date)
    processed = [n for n in all_newsletters if n.get("processed")]

    if not processed:
        print("   No processed newsletters found — skipping")
        return False

    print(f"   Synthesising across {len(processed)} newsletter(s)...")

    for n in processed:
        n["takeaways"] = get_takeaways_for_newsletter(n["id"])

    try:
        themes = _llm_call(_build_synthesis_prompt(processed), expect="array")
    except ValueError as e:
        print(f"   ❌ Synthesis failed: {e}")
        return False

    name_to_id = {n["sender_name"]: n["id"] for n in processed}
    delete_themes_for_date(target_date)

    for theme in themes:
        source_names = theme.get("source_names", [])
        source_ids = json.dumps([name_to_id[n] for n in source_names if n in name_to_id])
        insert_theme(
            date=target_date,
            tag=theme.get("tag", "EMERGING"),
            title=theme.get("title", ""),
            summary=theme.get("summary", ""),
            source_ids=source_ids,
            confidence=theme.get("confidence", "MEDIUM"),
        )

    print(f"   ✅ {len(themes)} theme(s) stored")
    return True


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(target_date: str | None = None) -> dict:
    """Process all queued newsletters then run synthesis. Entry point for run.py."""
    if target_date is None:
        target_date = date_type.today().isoformat()

    print(f"\n{'='*60}")
    print(f"  Briefly Pipeline — {target_date}  [{PROVIDER} / {MODEL}]")
    print(f"{'='*60}")

    unprocessed = get_unprocessed_newsletters()
    print(f"\n📥 {len(unprocessed)} newsletter(s) queued")

    ok, failed = 0, 0
    for newsletter in unprocessed:
        if process_newsletter(newsletter):
            ok += 1
        else:
            failed += 1

    synthesis_ok = run_synthesis(target_date)

    summary = {
        "date": target_date,
        "newsletters_processed": ok,
        "newsletters_failed": failed,
        "synthesis_run": synthesis_ok,
    }

    print(f"\n{'='*60}")
    print(f"  Done — Processed: {ok} | Failed: {failed} | Synthesis: {'✅' if synthesis_ok else '❌'}")
    print(f"{'='*60}\n")

    return summary
