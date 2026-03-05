"""
job_processor.py — Three-pass AI analysis of fetched job postings.

Pass 1: Extract skills from each individual job description.
Pass 2: Aggregate frequencies and synthesise trends across all postings.
Pass 3: Generate actionable insight report from top skills.

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

_AI_TAXONOMY = """
AI SKILL TAXONOMY — When the job description references AI/ML in any way,
you MUST map it to the most specific applicable leaf node from this taxonomy.
If context is insufficient to choose a leaf, discard the skill entirely.

AI Foundations & Engineering:
  - Prompt Engineering
  - RAG (Retrieval-Augmented Generation)
  - LLM Fine-tuning & Model Customisation
  - LLM Evaluation & Quality Assurance
  - Vector Databases & Embeddings
  - AI Agent Design & Orchestration
  - Multimodal AI (vision, audio, documents)
  - AI Infrastructure & Latency Optimisation

AI Product Skills:
  - AI Feature Scoping & Tradeoffs
  - Human-in-the-Loop Design
  - AI Metrics & KPI Definition
  - Model Output QA Design
  - Responsible AI / AI Governance
  - AI Product Safety & Red-teaming
  - AI Roadmapping & Prioritisation

AI Tooling (only if named in JD):
  - OpenAI API / Azure OpenAI
  - Anthropic Claude API
  - LangChain / LlamaIndex
  - Hugging Face
  - Weights & Biases (W&B)
  - Pinecone / Weaviate / pgvector
  - Google Vertex AI
  - AWS Bedrock
"""

_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def _extract_first_json_object(raw: str) -> str | None:
    """Find first complete JSON object by brace matching."""
    idx = raw.find("{")
    if idx == -1:
        return None
    stack = ["{"]
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
        elif c == "}":
            if stack and stack[-1] == "{":
                stack.pop()
                if not stack:
                    return raw[idx : i + 1].strip()
        i += 1
    return None


def _parse_json_object(raw: str) -> dict | None:
    """
    Parse a JSON object string, with fallback to json-repair for malformed LLM output.
    Returns dict or None on failure.
    """
    stripped = raw.strip()
    # Strip markdown fences if present
    fence_match = _FENCE_RE.search(stripped)
    if fence_match:
        stripped = fence_match.group(1).strip()
    for candidate in (stripped, _extract_first_json_object(stripped) or stripped):
        if not candidate:
            continue
        try:
            obj = json.loads(candidate)
            return obj if isinstance(obj, dict) else None
        except json.JSONDecodeError:
            pass
    try:
        from json_repair import repair_json
        repaired = repair_json(raw.strip())
        obj = json.loads(repaired)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


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

    if not description.strip():
        return []

    # ----------------------------------------------------------------
    # Stage A — Literal extraction: pull every skill-like phrase stated
    # ----------------------------------------------------------------
    stage_a_prompt = f"""Extract every skill, tool, technology, and domain mentioned
in this job posting. Be literal — extract only what is explicitly stated.
Do not apply quality filters yet.

Job title: {title}
Company: {company}
Description: {description}

Return a JSON array of strings. Example: ["SQL", "AI", "Stakeholder Management", "Figma"]
Raw JSON only, no markdown, no explanation."""

    try:
        resp_a = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": stage_a_prompt}],
            max_tokens=1024,
            temperature=0.1,
            timeout=30,
        )
        raw_a = resp_a.choices[0].message.content or ""
        if not raw_a.strip():
            return []
        cleaned_a = _clean_json_array(raw_a)
        stage_a_skills = json.loads(cleaned_a)
        if not isinstance(stage_a_skills, list) or not stage_a_skills:
            return []
    except Exception:
        return []

    # ----------------------------------------------------------------
    # Stage B — Refinement: apply blocklist, specificity floor, taxonomy
    # ----------------------------------------------------------------
    stage_b_prompt = f"""You are refining a raw skill list extracted from a PM job posting.
Apply the following rules to produce a final, high-quality skill list.

RULES:

1. BLOCKLIST — Remove entirely:
   Product Management, Product Thinking, Product Sense, Leadership, Management,
   Innovation, Collaboration, Communication, Strategy, Teamwork, Vision,
   Ownership, Problem Solving, Critical Thinking, Creativity, Passion, Drive,
   Execution, Influence, Accountability, Transparency, Curiosity, Empathy,
   Integrity, Self-starter, Results-oriented, Bias for Action

2. SPECIFICITY — For each remaining skill, either:
   a. Keep it if already specific (e.g. "Amplitude", "SQL", "A/B Testing")
   b. Sharpen it using context from the job description below
   c. Discard it if it cannot be made specific enough to Google for a tutorial

3. AI/ML — Map any AI-related skill to the most specific concept the job
   description supports, using this taxonomy:
{_AI_TAXONOMY}
   If context is insufficient for any leaf node, discard the skill.

4. CATEGORISE each kept skill as one of:
   Technical | Tool | Domain | Soft Skill | Credential

Raw skill list to refine: {json.dumps(stage_a_skills)}

Job description context (use this to sharpen vague skills):
{description}

Return a JSON array. Each element: {{ "skill": "...", "category": "..." }}
Raw JSON only, no markdown, no explanation."""

    try:
        resp_b = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": stage_b_prompt}],
            max_tokens=2048,
            temperature=0.1,
            timeout=45,
        )
        raw_b = resp_b.choices[0].message.content or ""
        if not raw_b.strip():
            return []
        cleaned_b = _clean_json_array(raw_b)
        parsed = json.loads(cleaned_b)
        if not isinstance(parsed, list):
            return []
        result = []
        for item in parsed:
            if isinstance(item, dict) and item.get("skill"):
                result.append({
                    "skill": str(item["skill"]).strip(),
                    "category": str(item.get("category", "Technical")).strip() or "Technical",
                })
        return result
    except Exception:
        return []


def _get_external_id_to_company(analysis_id: int) -> dict[str, str]:
    """Read job_postings for analysis_id and return external_id -> company mapping."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT external_id, company FROM job_postings WHERE analysis_id = ?",
        (analysis_id,),
    ).fetchall()
    conn.close()
    return {str(r[0] or ""): str(r[1] or "") for r in rows}


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
        time.sleep(1.0)

    return result


# ---------------------------------------------------------------------------
# PASS 2: Aggregation and synthesis
# ---------------------------------------------------------------------------

def cluster_skills(raw_skills: list[str]) -> dict[str, str]:
    """
    Takes a flat list of unique skill strings and returns a mapping:
        { "original_skill": "canonical_skill" }

    Uses a single AI call to:
      1. Merge near-duplicates into one canonical name
      2. Standardise naming conventions
      3. Map DISCARD for anything that passed extraction but should be removed
      4. Preserve specificity — never merge distinct skills just because
         they're in the same category

    Returns a dict. Callers should filter out entries where value == "DISCARD".
    """
    if not raw_skills:
        return {}

    # Deduplicate input before sending
    unique = list(dict.fromkeys(raw_skills))

    prompt = f"""You are consolidating a skill list extracted from PM job postings.
Apply these operations and return a JSON mapping of original → canonical name.

OPERATIONS:

1. MERGE near-duplicates into one canonical name (use the most specific/common form):
   - "Agile" + "Agile Methodology" + "Scrum/Agile" → "Agile Methodology"
   - "LLM APIs" + "OpenAI API" + "Claude API" → "LLM APIs"
   - "Data Analysis" + "Data Analytics" → "Data Analysis"
   - "Stakeholder Mgmt" + "Stakeholder Management" → "Stakeholder Management"
   - "SQL" + "SQL Queries" + "SQL (Structured Query Language)" → "SQL"

2. STANDARDISE naming (consistent title case, spell out abbreviations where helpful):
   - "AB Testing" → "A/B Testing"
   - "rag" → "RAG (Retrieval-Augmented Generation)"
   - "llm eval" → "LLM Evaluation & Quality"
   - "GTM" → "Go-to-Market Strategy"

3. PRESERVE distinct skills — do NOT merge skills that are meaningfully different:
   - "Prompt Engineering" and "LLM Fine-tuning" are different — keep both
   - "Amplitude" and "Mixpanel" are different tools — keep both
   - "Fintech" and "B2B SaaS" are different domains — keep both

4. DISCARD anything that slipped through and is still too generic:
   Map to the string "DISCARD" for: "AI", "Technology", "Data", "Analytics",
   "Machine Learning" (without further specificity), "Software", "Leadership",
   "Management", "Communication", "Collaboration", "Innovation", "Strategy"

Skill list to process:
{json.dumps(unique, indent=2)}

Return a JSON object mapping each original skill to its canonical name or "DISCARD".
Every skill in the input list must appear as a key in the output.
Raw JSON only, no markdown, no explanation."""

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=4096,
            temperature=0.1,
            timeout=60,
        )
        raw = resp.choices[0].message.content or ""
        if not raw.strip():
            return {s: s for s in unique}
        mapping = _parse_json_object(raw)
        if not mapping:
            return {s: s for s in unique}
        # Ensure every input skill has a mapping (default to itself)
        return {s: mapping.get(s, s) for s in unique}
    except Exception:
        # On any failure, return identity mapping (no clustering)
        return {s: s for s in unique}


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
    # skill_key (title case) -> {mention_count, category, companies: set}
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

    # --- Semantic clustering pass ---
    raw_skill_names = list(skill_data.keys())
    print(f"   Clustering {len(raw_skill_names)} raw skills...")
    canonical_map = cluster_skills(raw_skill_names)

    # Re-aggregate under canonical names, discarding DISCARD entries
    clustered: dict[str, dict] = {}
    for original_key, data in skill_data.items():
        canonical = canonical_map.get(original_key, original_key)
        if canonical == "DISCARD":
            continue
        if canonical not in clustered:
            clustered[canonical] = {
                "mention_count": 0,
                "category": data["category"],
                "companies": set(),
            }
        clustered[canonical]["mention_count"] += data["mention_count"]
        clustered[canonical]["companies"].update(data["companies"])

    # Build output from clustered data
    out = []
    for key, data in clustered.items():
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

def run_insight_synthesis(
    analysis_id: int,
    top_skills: list[dict],
    total_postings: int,
    run_date: str,
) -> dict:
    """
    Pass 3 — Generate actionable insight report from aggregated skill data.

    Takes the top 30 skills by frequency and produces:
      - "rising": skills that are new/rising and worth investing in
      - "table_stakes": skills in >50% of postings (hygiene, not differentiators)
      - "differentiators": skills in 15-45% of postings concentrated at senior level
      - "learning_paths": concrete resources for the top rising skills
      - "summary": 2-3 sentence executive summary

    Returns a dict with those keys, or an empty dict on failure.
    Stores the result in the _meta table under key f"insights_{analysis_id}".
    """
    top_30 = top_skills[:30]
    if not top_30:
        return {}

    # Format skill data for the prompt
    skill_lines = []
    for s in top_30:
        pct = round(s.get("pct_of_jobs", 0) * 100)
        trend = s.get("trend", "stable")
        prior = s.get("prior_pct")
        prior_str = f", prior week: {round(prior*100)}%" if prior is not None else ", first appearance"
        skill_lines.append(
            f"  - {s['skill']} ({s['category']}): {pct}% of jobs, trend: {trend}{prior_str}"
        )
    skill_block = "\n".join(skill_lines)

    prompt = f"""You are advising a senior Product Manager in NYC who wants to stay
competitive and identify exactly what to learn next. You have just analysed
{total_postings} PM job postings (product manager, senior PM, staff PM roles)
in NYC and remote for the week of {run_date}.

Here are the top skills by frequency:
{skill_block}

Generate a structured insight report with these exact sections:

1. RISING (3-5 skills): Skills that are "new" or "rising" in trend and will
   likely become standard requirements within 6 months. For each:
   - Why it is rising (1 sentence, be specific about market forces)
   - How urgent it is to learn (High / Medium)

2. TABLE_STAKES (skills with >50% frequency): These are now baseline requirements
   for any competitive PM application. List them plainly — they are hygiene,
   not differentiators.

3. DIFFERENTIATORS (skills with 15-45% frequency that are Technical, Domain, or
   Credential category): These separate strong candidates from average ones at
   senior/staff level. Explain briefly why each differentiates.

4. LEARNING_PATHS: For the top 3 RISING skills, provide a concrete learning path:
   - One specific course, project, or resource (name it — not "take an online course")
   - One hands-on project idea to build proof of the skill
   - Estimated time to job-ready proficiency

5. SUMMARY: 3 sentences. What is the market collectively asking for this week?
   What is the single most important skill gap to close? What does the trend
   line suggest about where PM skills are heading?

Return as a JSON object with keys:
  "rising", "table_stakes", "differentiators", "learning_paths", "summary"

For "rising", value is an array of objects: {{ "skill", "why_rising", "urgency" }}
For "table_stakes", value is an array of skill name strings.
For "differentiators", value is an array of objects: {{ "skill", "why_differentiates" }}
For "learning_paths", value is an array of objects:
  {{ "skill", "resource", "project_idea", "time_to_proficiency" }}
For "summary", value is a string.

Be specific and direct. Name tools, frameworks, and concepts explicitly.
Not "learn AI" but "build a RAG pipeline using LangChain and Pinecone".
Raw JSON only, no markdown, no preamble."""

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000,
            temperature=0.3,
            timeout=90,
        )
        raw = resp.choices[0].message.content or ""
        if not raw.strip():
            return {}
        insights = _parse_json_object(raw)
        if not insights:
            return {}

        # Store in _meta for retrieval by the API
        conn = get_conn()
        with conn:
            conn.execute(
                """
                INSERT INTO _meta (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (f"insights_{analysis_id}", json.dumps(insights)),
            )
        conn.close()
        print(f"   ✅ Insight synthesis stored for analysis_id={analysis_id}")
        return insights

    except Exception as e:
        print(f"   ⚠️  Insight synthesis failed: {e}")
        return {}


def rerun_insights(analysis_id: int) -> dict:
    """Re-run Pass 3 insight synthesis on an already-processed analysis."""
    skills = get_job_skills_for_analysis(analysis_id)
    conn = get_conn()
    row = conn.execute(
        "SELECT run_date, postings_analyzed FROM job_analyses WHERE id = ?",
        (analysis_id,)
    ).fetchone()
    conn.close()
    if not row:
        print(f"Analysis id={analysis_id} not found")
        return {}
    run_date = row[0]
    total_postings = row[1] or len(skills)
    print(f"Re-running insight synthesis for analysis_id={analysis_id} ({run_date})...")
    return run_insight_synthesis(analysis_id, skills, total_postings, run_date)


def run_job_analysis(run_date: str | None = None) -> dict:
    """
    Full job analysis pipeline:
      1. Create a job_analysis row in the DB
      2. Fetch postings via job_fetcher.run_job_fetch()
      3. Extract skills from each posting (Pass 1)
      4. Aggregate skill frequencies (Pass 2)
      5. Classify trends vs prior week
      6. Insight synthesis (Pass 3)
      7. Store all job_skills rows
      8. Return a summary dict

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
        # 1. Create job_analysis row
        import job_fetcher
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

        # Pass 3 — Insight synthesis
        print("\n  Pass 3 — Generating insight report...")
        insights = run_insight_synthesis(
            analysis_id=analysis_id,
            top_skills=classified,
            total_postings=total_postings,
            run_date=run_date,
        )

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
            "insights": insights,
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
            "insights": {},
            "status": status,
        }


if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv()
    from database import init_db
    init_db()

    parser = argparse.ArgumentParser()
    parser.add_argument("--rerun-insights", type=int, metavar="ANALYSIS_ID",
                        help="Re-run insight synthesis for an existing analysis")
    args = parser.parse_args()

    if args.rerun_insights:
        result = rerun_insights(args.rerun_insights)
        print(json.dumps(result, indent=2))
    else:
        result = run_job_analysis()
        print(result)
