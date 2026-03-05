"""
processor.py — AI processing pipeline for Briefly.

Two main jobs:
  1. process_newsletter(id)  — per-newsletter: extract takeaways + summarise articles
  2. run_synthesis(date)     — cross-newsletter: identify themes across all newsletters

Model: Google Gemini 2.5 Flash
  - Free tier: 15 requests/min, 1,500 requests/day — more than enough for a personal digest
  - Context window: 1,000,000 tokens — no more token limit errors
  - API: OpenAI-compatible endpoint, so the client is identical to before
  - Get a free key: aistudio.google.com → Get API Key (no credit card)

Fallback providers still supported via MODEL_PROVIDER env var:
  MODEL_PROVIDER=groq       → Groq + Llama 3.3 70B
  MODEL_PROVIDER=together   → Together AI + Llama 3.3 70B
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
    set_newsletter_category,
    insert_theme,
    delete_themes_for_date,
    mark_newsletter_processed,
)
from email_parser import extract_article_links
from article_fetcher import fetch_articles

load_dotenv()

# ---------------------------------------------------------------------------
# Model client
#
# Default: Gemini Flash 2.0 via Google's OpenAI-compatible endpoint.
# Override by setting MODEL_PROVIDER in .env.
# ---------------------------------------------------------------------------

PROVIDER = os.getenv("MODEL_PROVIDER", "gemini").lower()

_PROVIDERS = {
    "gemini": {
        "base_url":    "https://generativelanguage.googleapis.com/v1beta/openai/",
        "api_key_env": "GEMINI_API_KEY",
        "model":       "gemini-2.5-flash",
    },
    "groq": {
        "base_url":    "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY",
        "model":       "llama-3.3-70b-versatile",
    },
    "together": {
        "base_url":    "https://api.together.xyz/v1",
        "api_key_env": "TOGETHER_API_KEY",
        "model":       "meta-llama/Llama-3.3-70B-Instruct-Turbo",
    },
    "openrouter": {
        "base_url":    "https://openrouter.ai/api/v1",
        "api_key_env": "OPENROUTER_API_KEY",
        "model":       "google/gemini-flash-1.5",
    },
}

_cfg = _PROVIDERS.get(PROVIDER, _PROVIDERS["gemini"])
MODEL = os.getenv("MODEL_NAME", _cfg["model"])

client = OpenAI(
    api_key=os.getenv(_cfg["api_key_env"], "missing-key"),
    base_url=_cfg["base_url"],
)

print(f"[processor] provider={PROVIDER}  model={MODEL}")

# ---------------------------------------------------------------------------
# JSON schemas for structured output (Gemini only)
# ---------------------------------------------------------------------------

NEWSLETTER_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {"type": "string"},
        "takeaways": {
            "type": "array",
            "items": {"type": "string"},
        },
        "articles": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "url": {"type": "string"},
                    "title": {"type": "string"},
                    "summary": {"type": "string"},
                },
                "required": ["url", "title", "summary"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["category", "takeaways", "articles"],
    "additionalProperties": False,
}

SYNTHESIS_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "tag": {"type": "string"},
            "title": {"type": "string"},
            "summary": {"type": "string"},
            "source_names": {
                "type": "array",
                "items": {"type": "string"},
            },
            "confidence": {"type": "string"},
        },
        "required": ["tag", "title", "summary", "source_names", "confidence"],
        "additionalProperties": False,
    },
}


def _get_response_format(expect: str):
    """Return response_format dict for Gemini structured output, or None for other providers."""
    if PROVIDER != "gemini":
        return None
    schema = NEWSLETTER_SCHEMA if expect == "object" else SYNTHESIS_SCHEMA
    name = "newsletter_response" if expect == "object" else "synthesis_response"
    return {
        "type": "json_schema",
        "json_schema": {
            "name": name,
            "schema": schema,
            "strict": True,
        },
    }


def _log_llm_debug(resp, raw: str, error: str | None = None):
    """Log raw API response details for debugging empty or parse failures."""
    lines = ["   [DEBUG] LLM response details:"]
    try:
        if resp.choices:
            choice = resp.choices[0]
            lines.append(f"   [DEBUG]   finish_reason: {getattr(choice, 'finish_reason', 'N/A')}")
            lines.append(f"   [DEBUG]   content length: {len(raw)} chars")
            if len(raw) <= 200:
                lines.append(f"   [DEBUG]   content: {repr(raw)}")
            else:
                lines.append(f"   [DEBUG]   content (first 200): {repr(raw[:200])}...")
        else:
            lines.append(f"   [DEBUG]   choices: empty")
        if hasattr(resp, "usage") and resp.usage:
            u = resp.usage
            lines.append(f"   [DEBUG]   usage: prompt_tokens={getattr(u, 'prompt_tokens', 'N/A')} "
                        f"completion_tokens={getattr(u, 'completion_tokens', 'N/A')} "
                        f"total_tokens={getattr(u, 'total_tokens', 'N/A')}")
    except Exception as e:
        lines.append(f"   [DEBUG]   (could not extract: {e})")
    if error:
        lines.append(f"   [DEBUG]   error: {error}")
    print("\n".join(lines))


# ---------------------------------------------------------------------------
# JSON-safe LLM caller with retry
#
# Gemini is very reliable with JSON but we keep the retry wrapper for safety.
# It strips markdown fences, finds JSON boundaries, and on failure sends the
# model its own bad response back with a correction nudge — up to 3 attempts.
# ---------------------------------------------------------------------------

_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def _extract_first_json(raw: str, expect: str) -> str | None:
    """Find first complete JSON object or array by bracket matching."""
    start_char = "{" if expect == "object" else "["
    idx = raw.find(start_char)
    if idx == -1:
        return None

    stack = [start_char]
    i = idx + 1
    in_string = False
    escape = False
    quote_char = None

    while i < len(raw):
        c = raw[i]
        if escape:
            escape = False
            i += 1
            continue
        if in_string:
            if c == quote_char:
                in_string = False
            i += 1
            continue
        if c == "\\":
            escape = True
            i += 1
            continue
        if c == '"':
            in_string = True
            quote_char = '"'
            i += 1
            continue
        if c == "{":
            stack.append("{")
        elif c == "[":
            stack.append("[")
        elif c == "}":
            if stack and stack[-1] == "{":
                stack.pop()
                if not stack:
                    return raw[idx : i + 1].strip()
        elif c == "]":
            if stack and stack[-1] == "[":
                stack.pop()
                if not stack:
                    return raw[idx : i + 1].strip()
        i += 1
    return None


def _clean_json(raw: str, expect: str = "object") -> str:
    """Strip markdown fences and extract first complete JSON value."""
    match = _FENCE_RE.search(raw)
    if match:
        return match.group(1).strip()
    extracted = _extract_first_json(raw, expect)
    if extracted:
        return extracted
    return raw.strip()


def _repair_truncated_json(raw: str, expect: str) -> str | None:
    """
    Attempt to repair JSON truncated mid-string (e.g. finish_reason: length).
    Returns repaired string or None if repair fails.
    """
    s = raw.strip()
    if not s or s[0] not in "{[":
        return None
    in_string = False
    escape = False
    stack = []
    for i, c in enumerate(s):
        if escape:
            escape = False
            continue
        if in_string:
            if c == "\\":
                escape = True
            elif c == '"':
                in_string = False
            continue
        if c == '"':
            in_string = True
            continue
        if c == "{":
            stack.append("}")
        elif c == "[":
            stack.append("]")
        elif c in "}]" and stack and stack[-1] == c:
            stack.pop()
    # Close unclosed string if we're inside one
    if in_string:
        s = s.rstrip()
        if s.endswith("\\"):
            s = s[:-1]
        s += '"'
    s += "".join(reversed(stack))
    try:
        json.loads(s)
        return s
    except json.JSONDecodeError:
        return None


def _llm_call(prompt: str, expect: str = "object", retries: int = 3) -> dict | list:
    """
    Call the model and return parsed JSON.
    expect: 'object' or 'array'
    Raises ValueError after all retries exhausted.
    """
    messages = [{"role": "user", "content": prompt}]
    last_error = None

    for attempt in range(1, retries + 1):
        raw = ""
        resp = None
        try:
            kwargs = {
                "model": MODEL,
                "max_tokens": 8192,
                "temperature": 0.2,
                "messages": messages,
            }
            response_format = _get_response_format(expect)
            if response_format:
                kwargs["response_format"] = response_format

            resp = client.chat.completions.create(**kwargs)
            raw = resp.choices[0].message.content or ""

            if not raw.strip():
                _log_llm_debug(resp, raw, "Empty response from model")
                raise ValueError("Empty response from model")

            cleaned = _clean_json(raw, expect)
            try:
                parsed = json.loads(cleaned)
            except json.JSONDecodeError as parse_err:
                # If truncated (finish_reason: length), try to repair
                finish = getattr(resp.choices[0], "finish_reason", None) if resp.choices else None
                if finish == "length":
                    repaired = _repair_truncated_json(cleaned, expect)
                    if repaired:
                        parsed = json.loads(repaired)
                    else:
                        raise parse_err
                else:
                    raise parse_err

            if expect == "array" and not isinstance(parsed, list):
                raise ValueError(f"Expected JSON array, got {type(parsed).__name__}")
            if expect == "object" and not isinstance(parsed, dict):
                raise ValueError(f"Expected JSON object, got {type(parsed).__name__}")

            return parsed

        except (json.JSONDecodeError, ValueError) as e:
            last_error = e
            if resp:
                _log_llm_debug(resp, raw, str(e))
            err_msg = "empty response" if "Empty response" in str(e) else f"JSON parse error: {e}"
            print(f"   ⚠️  Attempt {attempt}/{retries} — {err_msg}")
            if attempt < retries:
                if raw.strip():
                    messages = [
                        {"role": "user", "content": prompt},
                        {"role": "assistant", "content": raw},
                        {"role": "user", "content": (
                            "Your response could not be parsed as valid JSON. "
                            f"Return ONLY a valid JSON {'array' if expect == 'array' else 'object'}. "
                            "No markdown fences, no explanation, no preamble."
                        )},
                    ]
                else:
                    messages = [
                        {"role": "user", "content": prompt},
                        {"role": "user", "content": (
                            "You returned an empty response. "
                            f"Return a valid JSON {'array' if expect == 'array' else 'object'}. "
                            "If you cannot process this content, return a minimal valid structure "
                            f"(e.g. empty {'[]' if expect == 'array' else '{}'} or with placeholder fields)."
                        )},
                    ]
                time.sleep(1)

        except Exception as e:
            last_error = e
            if resp:
                _log_llm_debug(resp, raw, str(e))
            print(f"   ❌ API error on attempt {attempt}: {e}")
            if attempt < retries:
                time.sleep(2 * attempt)

    raise ValueError(f"LLM call failed after {retries} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Two-pass article processing: summarise first, then build newsletter prompt
# ---------------------------------------------------------------------------

def _summarize_article(article: dict) -> str:
    """
    Summarise a single article in 3–4 sentences. Returns empty string on any exception.
    Uses global client and MODEL.
    """
    try:
        text = (article.get("text") or "").strip()
        if not text:
            return ""
        text = text[:8000]  # Truncate to 8,000 chars before sending
        title = article.get("title") or "Untitled"
        url = article.get("url") or ""

        prompt = f"""Summarise this article in 3–4 sentences. Focus on:
- The core argument or thesis
- Key data points, numbers, and names
- Implications or significance

Article title: {title}
URL: {url}

Article text:
{text}"""

        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.1,
        )
        raw = resp.choices[0].message.content or ""
        return raw.strip()
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

MAX_ARTICLES_IN_PROMPT = 25  # Limit articles passed to model to keep prompt lean


def _build_newsletter_prompt(newsletter: dict, fetched_articles: list[dict]) -> str:
    """
    Build prompt with newsletter body and linked article summaries (pre_summary).
    Newsletter body capped at 4,000 chars. Articles use pre_summary, not full text.
    """
    ok_articles = [a for a in fetched_articles if a["status"] == "ok" and a.get("text")]
    articles_for_prompt = ok_articles[:MAX_ARTICLES_IN_PROMPT]
    articles_section = ""
    for i, a in enumerate(articles_for_prompt, 1):
        pre_summary = a.get("pre_summary", "")
        articles_section += f"""
--- LINKED ARTICLE {i} ---
Title: {a['title']}
URL:   {a['url']}
Summary:
{pre_summary}
"""

    plain_text = newsletter.get("plain_text") or ""
    if len(plain_text) > 4000:
        plain_text = plain_text[:4000] + "\n\n[Truncated — newsletter body exceeded 4000 chars]"

    return f"""You are an expert analyst processing a newsletter for a busy professional.

NEWSLETTER DETAILS:
Sender:  {newsletter['sender_name']} <{newsletter['sender_email']}>
Subject: {newsletter['subject']}

NEWSLETTER BODY:
{plain_text}
{articles_section}

Analyse the newsletter body AND all linked articles provided above. Return a JSON object with this EXACT structure:

{{
  "category": "one of: Fintech & Markets | Product | Venture & Tech | AI & ML | Macro & Policy | Compliance & Risk | Other",
  "takeaways": [
    "Concise, insight-rich bullet. Lead with the finding — be specific, include numbers/names/decisions where present.",
    "Each bullet is 1–2 sentences, self-contained.",
    "3–5 bullets total. Synthesise across newsletter body AND linked articles."
  ],
  "articles": [
    {{
      "url": "exact URL from the linked articles above",
      "title": "article title",
      "summary": "2–3 sentences on what the article specifically argues or reveals."
    }}
  ]
}}

Return ONLY the raw JSON object. No markdown fences, no preamble, no explanation.
Do not invent information not in the source material.
If no articles were fetched, return an empty array for articles."""


def _build_synthesis_prompt(newsletters: list[dict]) -> str:
    content_block = ""
    for n in newsletters:
        bullets = "\n".join(f"  • {t['content']}" for t in n.get("takeaways", []))
        content_block += f"\n=== {n['sender_name']} — {n['subject']} ===\n{bullets}\n"

    return f"""You are a senior analyst synthesising intelligence across multiple newsletters received today.

TODAY'S NEWSLETTER TAKEAWAYS:
{content_block}

Identify 2–4 meaningful cross-cutting themes — signals appearing across multiple newsletters, or a single significant standalone development worth flagging.

Return a JSON array with this EXACT structure:

[
  {{
    "tag": "one of: MACRO SIGNAL | PRODUCT TREND | MARKET MOVE | REGULATORY SHIFT | EMERGING | CONSENSUS VIEW",
    "title": "A single crisp thesis sentence, max 15 words.",
    "summary": "2–4 sentences. Name specific newsletters that contributed. Include concrete details.",
    "source_names": ["Newsletter Name 1", "Newsletter Name 2"],
    "confidence": "HIGH (3+ sources align) | MEDIUM (2 sources or one strong signal) | LOW (speculative)"
  }}
]

Return ONLY the raw JSON array. No markdown fences, no preamble.
Order by confidence: HIGH first. Only surface themes the data genuinely supports."""


# ---------------------------------------------------------------------------
# Per-newsletter processing
# ---------------------------------------------------------------------------

def process_newsletter(newsletter: dict) -> bool:
    """
    Full pipeline for one newsletter:
      1. Extract article links from raw HTML
      2. Fetch each article
      3. Call Gemini → structured JSON
      4. Store takeaways + article summaries
      5. Mark processed
    Returns True on success, False on error.
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

    ok_count = sum(1 for a in fetched if a["status"] == "ok")
    ok_articles = [a for a in fetched if a["status"] == "ok" and a.get("text")]
    n = len(ok_articles)
    for i, a in enumerate(ok_articles, 1):
        print(f"   Summarising article {i}/{n}...")
        a["pre_summary"] = _summarize_article(a)
    cap_note = f" (capped at {MAX_ARTICLES_IN_PROMPT})" if n > MAX_ARTICLES_IN_PROMPT else ""
    print(f"   {ok_count}/{len(fetched)} articles fetched{cap_note}")

    # 3. Call Gemini
    print(f"   Calling {PROVIDER} ({MODEL})...")
    prompt = _build_newsletter_prompt(newsletter, fetched)

    try:
        result = _llm_call(prompt, expect="object")
    except ValueError as e:
        print(f"   ❌ Failed after retries: {e}")
        return False

    # 4a. Store takeaways
    delete_takeaways_for_newsletter(nid)
    set_newsletter_category(nid, result.get("category", ""))
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
        if not art.get("summary", "") and meta.get("status", "") != "ok":
            continue
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

    print(f"   All newsletters for {target_date}: {len(all_newsletters)}")
    print(f"   Processed: {len(processed)} | Unprocessed: {len(all_newsletters) - len(processed)}")

    if not processed:
        print("   No processed newsletters found — skipping")
        return False

    print(f"   Synthesising across {len(processed)} newsletter(s)...")
    for n in processed:
        n["takeaways"] = get_takeaways_for_newsletter(n["id"])

    print(f"   Takeaway counts: { {n['sender_name']: len(n['takeaways']) for n in processed} }")

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
