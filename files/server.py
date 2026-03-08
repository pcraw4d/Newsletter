"""
server.py — Flask web server for Briefly.

Endpoints:
  GET  /                     ← dashboard SPA
  GET  /api/digest           ← today's full digest
  GET  /api/digest/<date>    ← digest for YYYY-MM-DD
  GET  /api/status           ← health check + queue depth
  GET  /api/pull/status      ← is a pipeline run in progress?
  POST /api/pull             ← trigger Gmail poll + AI pipeline (UI button)
  GET  /api/newsletters/today
  POST /test/ingest          ← inject a fake email (dev only)
"""

import json
import os
import hmac
import threading
import time
from datetime import date, datetime

from flask import Flask, request, jsonify, abort, send_from_directory
from dotenv import load_dotenv

from database import (
    clear_all_job_data,
    init_db,
    insert_newsletter,
    get_unprocessed_newsletters,
    get_newsletters_for_date,
    get_full_digest_for_date,
    get_junk_filtered_count_for_date,
    get_latest_job_analysis,
    get_job_skills_for_analysis,
    get_job_analysis_for_date,
)
from email_parser import parse_raw_email

load_dotenv()

app = Flask(__name__)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# ---------------------------------------------------------------------------
# Pipeline job state
# Track whether a pipeline run is active so the UI can show progress
# and prevent double-triggers.
# ---------------------------------------------------------------------------

_job = {
    "running":    False,
    "started_at": None,
    "finished_at": None,
    "result":     None,   # summary dict from run_pipeline
    "error":      None,
    "log":        [],     # captured print output
}
_job_lock = threading.Lock()


# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Briefly-Secret"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response

@app.route("/api/<path:path>", methods=["OPTIONS"])
def options_handler(path=""):
    return "", 204


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@app.get("/")
def dashboard():
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "index.html")


# ---------------------------------------------------------------------------
# Pipeline trigger — runs Gmail poll + AI pipeline in a background thread
# ---------------------------------------------------------------------------

def _run_pipeline_job():
    """Worker function executed in a background thread by POST /api/pull."""
    import io
    import sys

    # Capture stdout so the log is available via /api/pull/status
    log_lines = []

    class _Capture:
        def write(self, msg):
            if msg.strip():
                log_lines.append(msg.rstrip())
            sys.__stdout__.write(msg)
        def flush(self):
            sys.__stdout__.flush()

    sys.stdout = _Capture()

    try:
        # Step 1: Gmail poll
        try:
            from gmail_poller import poll_gmail
            poll_result = poll_gmail()
        except RuntimeError as e:
            log_lines.append(f"⚠️  Gmail poll skipped: {e}")
            poll_result = {"ingested": 0, "skipped": 0, "junk_skipped": 0, "failed": 0}
        except Exception as e:
            log_lines.append(f"❌ Gmail poll error: {e}")
            poll_result = {"ingested": 0, "skipped": 0, "junk_skipped": 0, "failed": 0}

        # Step 2: AI processing + synthesis
        from processor import run_pipeline
        pipeline_result = run_pipeline()

        result = {**poll_result, **pipeline_result, "status": "ok"}

        with _job_lock:
            _job["result"]      = result
            _job["error"]       = None
            _job["log"]         = log_lines
            _job["finished_at"] = datetime.utcnow().isoformat() + "Z"
            _job["running"]     = False

    except Exception as e:
        with _job_lock:
            _job["error"]       = str(e)
            _job["log"]         = log_lines
            _job["finished_at"] = datetime.utcnow().isoformat() + "Z"
            _job["running"]     = False
    finally:
        sys.stdout = sys.__stdout__


@app.post("/api/pull")
def trigger_pull():
    """
    Trigger a full Gmail poll + AI pipeline run.
    Returns 202 immediately; poll /api/pull/status to track progress.
    """
    with _job_lock:
        if _job["running"]:
            return jsonify({
                "ok": False,
                "error": "A pipeline run is already in progress",
                "started_at": _job["started_at"],
            }), 409

        _job["running"]     = True
        _job["started_at"]  = datetime.utcnow().isoformat() + "Z"
        _job["finished_at"] = None
        _job["result"]      = None
        _job["error"]       = None
        _job["log"]         = []

    thread = threading.Thread(target=_run_pipeline_job, daemon=True)
    thread.start()

    return jsonify({"ok": True, "message": "Pipeline started"}), 202


@app.get("/api/pull/status")
def pull_status():
    """Return current pipeline job state."""
    with _job_lock:
        return jsonify({
            "running":     _job["running"],
            "started_at":  _job["started_at"],
            "finished_at": _job["finished_at"],
            "result":      _job["result"],
            "error":       _job["error"],
            "log":         _job["log"][-30:],   # last 30 lines
        })


# ---------------------------------------------------------------------------
# Digest API
# ---------------------------------------------------------------------------

@app.get("/api/digest")
def digest_today():
    return jsonify(get_full_digest_for_date(date.today().isoformat()))

@app.get("/api/digest/<date_str>")
def digest_for_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date. Use YYYY-MM-DD"}), 400
    return jsonify(get_full_digest_for_date(date_str))

def _get_db_stats() -> dict:
    from database import get_conn
    conn = get_conn()
    stats = {}
    stats["total_newsletters"] = conn.execute(
        "SELECT COUNT(*) FROM newsletters"
    ).fetchone()[0]
    stats["total_articles"] = conn.execute(
        "SELECT COUNT(*) FROM articles"
    ).fetchone()[0]
    stats["total_themes"] = conn.execute(
        "SELECT COUNT(*) FROM themes"
    ).fetchone()[0]
    row = conn.execute(
        "SELECT MIN(date(received_at)), MAX(date(received_at)) FROM newsletters"
    ).fetchone()
    stats["oldest_newsletter"] = row[0]
    stats["newest_newsletter"] = row[1]
    conn.close()
    return stats


@app.get("/api/status")
def status():
    today = date.today().isoformat()
    digest = get_full_digest_for_date(today)
    with _job_lock:
        pipeline_running = _job["running"]
    return jsonify({
        "status":              "ok",
        "today":               today,
        "unprocessed_count":   len(get_unprocessed_newsletters()),
        "newsletters_today":   len(digest["newsletters"]),
        "themes_today":        len(digest["themes"]),
        "junk_filtered_today": get_junk_filtered_count_for_date(today),
        "pipeline_running":    pipeline_running,
        "retention_days":      int(os.getenv("DATA_RETENTION_DAYS", "30")),
        "db_stats":            _get_db_stats(),
    })

@app.get("/api/newsletters/today")
def newsletters_today():
    today = date.today().isoformat()
    rows = get_newsletters_for_date(today)
    return jsonify({
        "date": today,
        "count": len(rows),
        "newsletters": [
            {
                "id": r["id"],
                "sender_email": r["sender_email"],
                "sender_name": r["sender_name"],
                "subject": r["subject"],
                "received_at": r["received_at"],
                "processed": bool(r["processed"]),
                "skipped_reason": r.get("skipped_reason"),
            }
            for r in rows
        ],
    })


# ---------------------------------------------------------------------------
# Job analysis API
# ---------------------------------------------------------------------------

def _get_job_insights(analysis_id: int) -> dict:
    """Read the stored insight synthesis for a given analysis_id."""
    from database import get_conn
    import json
    conn = get_conn()
    row = conn.execute(
        "SELECT value FROM _meta WHERE key = ?",
        (f"insights_{analysis_id}",)
    ).fetchone()
    conn.close()
    if not row or not row[0]:
        return {}
    try:
        return json.loads(row[0])
    except Exception:
        return {}


@app.get("/api/jobs/latest")
def jobs_latest():
    """Return the most recent job analysis with all skills."""
    analysis = get_latest_job_analysis()
    if not analysis:
        return jsonify({"analysis": None, "skills": []}), 200
    skills = get_job_skills_for_analysis(analysis["id"])
    # Parse example_companies JSON string back to list for each skill
    for s in skills:
        try:
            s["example_companies"] = json.loads(s.get("example_companies") or "[]")
        except Exception:
            s["example_companies"] = []
    insights = _get_job_insights(analysis["id"]) if analysis else {}
    return jsonify({
        "analysis": analysis,
        "skills": skills,
        "insights": insights,
    })


@app.get("/api/jobs/<run_date>")
def jobs_for_date(run_date):
    """Return job analysis for a specific YYYY-MM-DD date."""
    try:
        datetime.strptime(run_date, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date. Use YYYY-MM-DD"}), 400
    analysis = get_job_analysis_for_date(run_date)
    if not analysis:
        return jsonify({"analysis": None, "skills": []}), 200
    skills = get_job_skills_for_analysis(analysis["id"])
    for s in skills:
        try:
            s["example_companies"] = json.loads(s.get("example_companies") or "[]")
        except Exception:
            s["example_companies"] = []
    insights = _get_job_insights(analysis["id"]) if analysis else {}
    return jsonify({"analysis": analysis, "skills": skills, "insights": insights})


@app.delete("/api/jobs/clear")
def clear_job_data():
    """Delete all job analyses, postings, skills, and insight metadata."""
    counts = clear_all_job_data()
    return jsonify({"ok": True, "cleared": counts}), 200


@app.post("/api/jobs/pull")
def trigger_job_pull():
    """
    Trigger a job analysis run in a background thread.
    Returns 202 immediately.
    Uses the same _job lock mechanism as /api/pull to prevent double-triggers.
    """
    with _job_lock:
        if _job["running"]:
            return jsonify({"ok": False, "error": "A pipeline run is already in progress"}), 409
        _job["running"]     = True
        _job["started_at"]  = datetime.utcnow().isoformat() + "Z"
        _job["finished_at"] = None
        _job["result"]      = None
        _job["error"]       = None
        _job["log"]         = []

    def _run():
        import sys, io
        log_lines = []
        class _Cap:
            def write(self, m):
                if m.strip(): log_lines.append(m.rstrip())
                sys.__stdout__.write(m)
            def flush(self): sys.__stdout__.flush()
        sys.stdout = _Cap()
        try:
            from job_processor import run_job_analysis
            result = run_job_analysis()
            with _job_lock:
                _job["result"]      = result
                _job["error"]       = None
                _job["log"]         = log_lines
                _job["finished_at"] = datetime.utcnow().isoformat() + "Z"
                _job["running"]     = False
        except Exception as e:
            with _job_lock:
                _job["error"]       = str(e)
                _job["log"]         = log_lines
                _job["finished_at"] = datetime.utcnow().isoformat() + "Z"
                _job["running"]     = False
        finally:
            sys.stdout = sys.__stdout__

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "message": "Job analysis started"}), 202


# ---------------------------------------------------------------------------
# Test / dev
# ---------------------------------------------------------------------------

@app.post("/test/ingest")
def test_ingest():
    """Inject a fake newsletter. Dev only."""
    data = request.get_json(silent=True) or {}

    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from datetime import timezone

    msg = MIMEMultipart("alternative")
    msg["From"] = f"{data.get('sender_name','Test')} <{data.get('sender_email','test@example.com')}>"
    msg["Subject"] = data.get("subject", "Test Newsletter")
    msg["Date"] = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
    msg.attach(MIMEText(data.get("body", "<p>Test</p>"), "html"))

    parsed = parse_raw_email(msg.as_bytes())
    plain_text = parsed.get("plain_text") or ""
    if len(plain_text.split()) < 10:
        return jsonify({"ok": False, "error": "body too short for testing"}), 400

    nl_id = insert_newsletter(
        sender_email=parsed["sender_email"],
        sender_name=parsed["sender_name"],
        subject=parsed["subject"],
        received_at=parsed["received_at"],
        raw_html=parsed["raw_html"],
        plain_text=plain_text,
    )
    return jsonify({"ok": True, "newsletter_id": nl_id}), 201


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", 5001))
    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    print(f"\n🗞  Briefly server — http://localhost:{port}")
    print(f"   Dashboard  : GET  /")
    print(f"   Pull now   : POST /api/pull")
    print(f"   Digest API : GET  /api/digest\n")
    app.run(host="0.0.0.0", port=port, debug=debug, use_reloader=False)
