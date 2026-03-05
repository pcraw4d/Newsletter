"""
job_processor.py — Two-pass AI analysis of fetched job postings.

Pass 1: Extract skills from each individual job description.
Pass 2: Aggregate frequencies and synthesise trends across all postings.

Uses the same OpenAI client setup as processor.py.
"""

import json
import re
import time
from datetime import date as date_type

from processor import client, MODEL
from database import (
    get_conn,
    insert_job_skill,
    get_job_skills_for_analysis,
    get_prior_week_skills,
    update_job_analysis_count,
    get_job_analysis_for_date,
    insert_job_analysis,
)

_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def _extract_first_json_array(raw: str) -> str | None:
    """Find first complete JSON array by bracket matching."""
    idx = raw.find("[")
    if idx == -1:
        return None
    stack = ["["]
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
        if c == "[":
            stack.append("[")
        elif c == "]":
            if stack and stack[-1] == "[":
                stack.pop()
                if not stack:
                    return raw[idx : i + 1].strip()
        i += 1
    return None


def _clean_json_array(raw: str) -> str:
    """Strip markdown fences and extract first complete JSON array."""
    match = _FENCE_RE.search(raw)
    if match:
        return match.group(1).strip()
    extracted = _extract_first_json_array(raw)
    if extracted:
        return extracted
    return raw.strip()


# ---------------------------------------------------------------------------
# PASS 1: Per-posting skill extraction
# ---------------------------------------------------------------------------

def extract_skills_from_posting(posting: dict) -> list[dict]:
    """
    Call the AI to extract skills from a single job posting.
    Returns a list of dicts with keys: skill, category.
    On any failure, returns [].
    """
    title = posting.get("title") or ""
    company = posting.get("company") or ""
    description = (posting.get("description") or "")[:3000]

    prompt = f"""Extract all skills and requirements from this PM job posting.
Return a JSON array of objects. Each object must have:
  skill    — the skill name, normalised (e.g. 'SQL' not 'sql', 'A/B Testing'
             not 'ab testing', 'Stakeholder Management' not 'stakeholder mgmt')
  category — one of: Technical | Tool | Domain | Soft Skill | Credential

Only extract skills explicitly stated. Do not infer or add skills not mentioned.
Ignore generic phrases like 'strong communication' unless they are a specific
requirement pattern. Include tools (Jira, Figma, Amplitude), technical skills
(SQL, Python, API design), domain knowledge (fintech, healthcare), soft skills
(executive communication, cross-functional leadership), and credentials (MBA,
PMP).

Job title: {title}
Company: {company}
Description: {description}"""

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=4096,
            temperature=0.2,
            timeout=60,
        )
        raw = resp.choices[0].message.content or ""
        if not raw.strip():
            return []
        cleaned = _clean_json_array(raw)
        parsed = json.loads(cleaned)
        if not isinstance(parsed, list):
            return []
        result = []
        for item in parsed:
            if isinstance(item, dict) and item.get("skill"):
                result.append({
                    "skill": str(item["skill"]).strip(),
                    "category": str(item.get("category", "")).strip() or "Technical",
                })
        return result
    except Exception:
        return []


def extract_all_skills(analysis_id: int) -> dict[str, list]:
    """
    Read all job_postings for the given analysis_id, extract skills from each.
    Returns a dict mapping external_id -> list of extracted skill dicts.
    """
    conn = get_conn()
    rows = conn.execute(
        "SELECT external_id, title, company, description FROM job_postings WHERE analysis_id = ?",
        (analysis_id,),
    ).fetchall()
    conn.close()

    postings = [dict(r) for r in rows]
    total = len(postings)
    result: dict[str, list] = {}

    for i, row in enumerate(postings):
        external_id = row.get("external_id") or ""
        skills = extract_skills_from_posting(row)
        result[external_id] = skills
        if (i + 1) % 10 == 0:
            print(f"   Extracted {i + 1}/{total}...")
        time.sleep(0.5)

    return result


# ---------------------------------------------------------------------------
# PASS 2: Aggregation and synthesis
# ---------------------------------------------------------------------------

def aggregate_skills(
    all_skills: dict[str, list],
    total_postings: int,
    external_id_to_company: dict[str, str] | None = None,
) -> list[dict]:
    """
    Flatten extracted skills, count mentions per skill (case-insensitive, title case),
    compute pct_of_jobs, track up to 3 example companies per skill.
    Returns list sorted by mention_count DESC.
    """
    id_to_company = external_id_to_company or {}
    skill_data: dict[str, dict] = {}

    for external_id, skills in all_skills.items():
        company = id_to_company.get(external_id, "")
        for s in skills:
            skill_name = (s.get("skill") or "").strip()
            if not skill_name:
                continue
            key = skill_name.title()
            cat = (s.get("category") or "Technical").strip()
            if key not in skill_data:
                skill_data[key] = {
                    "mention_count": 0,
                    "category": cat,
                    "companies": set(),
                }
            skill_data[key]["mention_count"] += 1
            if company and len(skill_data[key]["companies"]) < 3:
                skill_data[key]["companies"].add(company)

    out = []
    for key, data in skill_data.items():
        pct = data["mention_count"] / total_postings if total_postings else 0.0
        out.append({
            "skill": key,
            "category": data["category"],
            "mention_count": data["mention_count"],
            "pct_of_jobs": pct,
            "example_companies": json.dumps(list(data["companies"])[:3]),
        })

    out.sort(key=lambda x: x["mention_count"], reverse=True)
    return out


def classify_trends(
    aggregated: list[dict],
    prior_week: dict[str, float],
) -> list[dict]:
    """
    For each skill, compare pct_of_jobs to prior week and set trend.
    Adds "trend" and "prior_pct" keys to each skill dict.
    """
    prior_normalized = {k.title(): v for k, v in prior_week.items()}

    for item in aggregated:
        skill = item.get("skill", "")
        current = item.get("pct_of_jobs", 0.0)
        prior = prior_normalized.get(skill.title() if skill else "")

        item["prior_pct"] = prior
        if prior is None:
            item["trend"] = "new"
        elif current - prior > 0.08:
            item["trend"] = "rising"
        elif current - prior < -0.08:
            item["trend"] = "declining"
        else:
            item["trend"] = "stable"

    return aggregated


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def _get_external_id_to_company(analysis_id: int) -> dict[str, str]:
    """Read job_postings for analysis_id and return external_id -> company mapping."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT external_id, company FROM job_postings WHERE analysis_id = ?",
        (analysis_id,),
    ).fetchall()
    conn.close()
    return {str(r[0] or ""): str(r[1] or "") for r in rows}


def run_job_analysis(run_date: str | None = None) -> dict:
    """
    Full job analysis pipeline:
      1. Create a job_analysis row in the DB
      2. Fetch postings via job_fetcher.run_job_fetch()
      3. Extract skills from each posting (Pass 1)
      4. Aggregate skill frequencies (Pass 2)
      5. Classify trends vs prior week
      6. Store all job_skills rows
      7. Return a summary dict

    run_date defaults to today's date (YYYY-MM-DD).
    """
    if run_date is None:
        run_date = date_type.today().isoformat()

    print(f"\n{'='*60}")
    print(f"  Briefly Job Analysis — {run_date}")
    print(f"{'='*60}")

    status = "ok"
    analysis_id = 0
    try:
        import job_fetcher

        # 1. Create job_analysis row
        queries = json.dumps(job_fetcher.SEARCH_QUERIES)
        locations = json.dumps(job_fetcher.SEARCH_LOCATIONS)
        analysis_id = insert_job_analysis(run_date, queries, locations)
        print(f"\n  Created analysis id={analysis_id}")

        # 2. Fetch postings
        print("\n  Fetching job postings...")
        postings_count = job_fetcher.run_job_fetch(analysis_id)

        # 3. Extract skills (Pass 1)
        print("\n  Pass 1 — Extracting skills from each posting...")
        all_skills = extract_all_skills(analysis_id)
        id_to_company = _get_external_id_to_company(analysis_id)

        # 4. Aggregate (Pass 2)
        total_postings = len(all_skills)
        print(f"\n  Pass 2 — Aggregating {total_postings} postings...")
        aggregated = aggregate_skills(all_skills, total_postings, id_to_company)

        # 5. Classify trends
        prior_week = get_prior_week_skills(analysis_id)
        classified = classify_trends(aggregated, prior_week)

        # 6. Store job_skills
        for item in classified:
            insert_job_skill(
                analysis_id=analysis_id,
                skill=item["skill"],
                category=item["category"],
                mention_count=item["mention_count"],
                pct_of_jobs=item["pct_of_jobs"],
                trend=item["trend"],
                prior_pct=item.get("prior_pct"),
                example_companies=item.get("example_companies", "[]"),
            )

        # 7. Update analysis count
        update_job_analysis_count(analysis_id, total_postings)

        skills_count = len(classified)
        print(f"\n  Done — {total_postings} postings | {skills_count} unique skills identified")
        print(f"{'='*60}\n")

        return {
            "run_date": run_date,
            "analysis_id": analysis_id,
            "postings_analyzed": total_postings,
            "skills_identified": skills_count,
            "status": status,
        }

    except Exception as e:
        status = "failed"
        print(f"\n  ❌ Failed: {e}")
        print(f"{'='*60}\n")
        return {
            "run_date": run_date,
            "analysis_id": 0,
            "postings_analyzed": 0,
            "skills_identified": 0,
            "status": status,
        }


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    from database import init_db
    init_db()
    result = run_job_analysis()
    print(result)
