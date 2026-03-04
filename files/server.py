"""
server.py — Flask web server for Briefly.

Endpoints:
  POST /inbound              ← SendGrid webhook (receives forwarded newsletters)
  POST /test/ingest          ← inject a fake email for local testing
  GET  /api/digest           ← today's full digest (used by the dashboard)
  GET  /api/digest/<date>    ← digest for a specific YYYY-MM-DD
  GET  /api/status           ← health check + queue depth
  GET  /api/newsletters/today
"""

import os
import hmac
from datetime import date

from flask import Flask, request, jsonify, abort
from dotenv import load_dotenv

from database import (
    init_db,
    insert_newsletter,
    get_unprocessed_newsletters,
    get_newsletters_for_date,
    get_full_digest_for_date,
)
from email_parser import parse_raw_email

load_dotenv()

app = Flask(__name__, static_folder="static", static_url_path="")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# ---------------------------------------------------------------------------
# CORS — allows the React dashboard (on a different port locally) to call the API
# ---------------------------------------------------------------------------

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Briefly-Secret"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response

@app.route("/api/<path:path>", methods=["OPTIONS"])
@app.route("/inbound", methods=["OPTIONS"])
def options_handler(path=""):
    return "", 204


# ---------------------------------------------------------------------------
# Dashboard — serve index.html at root
# ---------------------------------------------------------------------------

@app.get("/")
def dashboard():
    """Serve the dashboard SPA."""
    from flask import send_from_directory
    return send_from_directory(app.static_folder, "index.html")



# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _verify_secret(provided: str) -> bool:
    if not WEBHOOK_SECRET:
        return True   # dev mode — no secret configured
    return hmac.compare_digest(provided or "", WEBHOOK_SECRET)


# ---------------------------------------------------------------------------
# Inbound email webhook (SendGrid Inbound Parse)
#
# SendGrid sends multipart/form-data with a field called 'email' containing
# the full raw MIME message. Our parser handles this format natively.
# ---------------------------------------------------------------------------

@app.post("/inbound")
def inbound_email():
    if not _verify_secret(request.headers.get("X-Briefly-Secret", "")):
        abort(403, "Invalid webhook secret")

    raw_bytes = None
    content_type = request.content_type or ""

    if "message/rfc822" in content_type:
        raw_bytes = request.get_data()
    elif "multipart/form-data" in content_type or "application/x-www-form-urlencoded" in content_type:
        # SendGrid default format
        raw_mime = request.form.get("email") or request.form.get("body-mime") or ""
        if raw_mime:
            raw_bytes = raw_mime.encode("utf-8")
    elif "application/json" in content_type:
        data = request.get_json(silent=True) or {}
        raw_mime = data.get("body-mime") or data.get("raw") or ""
        if raw_mime:
            raw_bytes = raw_mime.encode("utf-8")

    if not raw_bytes:
        return jsonify({"error": "Could not extract email body"}), 400

    try:
        parsed = parse_raw_email(raw_bytes)
    except Exception as exc:
        app.logger.error(f"[inbound] Parse error: {exc}")
        return jsonify({"error": "Email parse failed", "detail": str(exc)}), 500

    try:
        newsletter_id = insert_newsletter(
            sender_email=parsed["sender_email"],
            sender_name=parsed["sender_name"],
            subject=parsed["subject"],
            received_at=parsed["received_at"],
            raw_html=parsed["raw_html"],
            plain_text=parsed["plain_text"],
        )
    except Exception as exc:
        app.logger.error(f"[inbound] DB error: {exc}")
        return jsonify({"error": "Database insert failed", "detail": str(exc)}), 500

    app.logger.info(
        f"[inbound] id={newsletter_id} from='{parsed['sender_email']}' "
        f"links={len(parsed['article_links'])}"
    )

    return jsonify({
        "ok": True,
        "newsletter_id": newsletter_id,
        "sender": parsed["sender_email"],
        "subject": parsed["subject"],
        "article_links_found": len(parsed["article_links"]),
    }), 200


# ---------------------------------------------------------------------------
# Dashboard API endpoints
# ---------------------------------------------------------------------------

@app.get("/api/digest")
def digest_today():
    """Full digest for today — themes + newsletters + takeaways + articles."""
    today = date.today().isoformat()
    return jsonify(get_full_digest_for_date(today))


@app.get("/api/digest/<date_str>")
def digest_for_date(date_str: str):
    """Full digest for a specific date (YYYY-MM-DD)."""
    try:
        # Basic format validation
        from datetime import datetime
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
    return jsonify(get_full_digest_for_date(date_str))


@app.get("/api/status")
def status():
    unprocessed = get_unprocessed_newsletters()
    today = date.today().isoformat()
    digest = get_full_digest_for_date(today)
    return jsonify({
        "status": "ok",
        "today": today,
        "unprocessed_count": len(unprocessed),
        "newsletters_today": len(digest["newsletters"]),
        "themes_today": len(digest["themes"]),
    })


@app.get("/api/newsletters/today")
def newsletters_today():
    today = date.today().isoformat()
    rows = get_newsletters_for_date(today)
    slim = [
        {
            "id": r["id"],
            "sender_email": r["sender_email"],
            "sender_name": r["sender_name"],
            "subject": r["subject"],
            "received_at": r["received_at"],
            "processed": bool(r["processed"]),
        }
        for r in rows
    ]
    return jsonify({"date": today, "count": len(slim), "newsletters": slim})


# ---------------------------------------------------------------------------
# Test / dev endpoint
# ---------------------------------------------------------------------------

@app.post("/test/ingest")
def test_ingest():
    """Inject a fake newsletter without a real email provider. Dev only."""
    data = request.get_json(silent=True) or {}

    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from datetime import datetime, timezone

    sender_email = data.get("sender_email", "test@example.com")
    sender_name = data.get("sender_name", "Test Sender")
    subject = data.get("subject", "Test Newsletter")
    body_html = data.get("body", "<p>Test email body</p>")

    msg = MIMEMultipart("alternative")
    msg["From"] = f"{sender_name} <{sender_email}>"
    msg["Subject"] = subject
    msg["Date"] = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")
    msg.attach(MIMEText(body_html, "html"))

    parsed = parse_raw_email(msg.as_bytes())
    newsletter_id = insert_newsletter(
        sender_email=parsed["sender_email"],
        sender_name=parsed["sender_name"],
        subject=parsed["subject"],
        received_at=parsed["received_at"],
        raw_html=parsed["raw_html"],
        plain_text=parsed["plain_text"],
    )

    return jsonify({
        "ok": True,
        "newsletter_id": newsletter_id,
        "parsed": {
            "sender_email": parsed["sender_email"],
            "subject": parsed["subject"],
            "plain_text_preview": parsed["plain_text"][:200],
            "article_links_found": len(parsed["article_links"]),
            "article_links": parsed["article_links"],
        },
    }), 201


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", 5001))
    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    print(f"\n🗞  Briefly server running on http://localhost:{port}")
    print(f"   POST /inbound              ← SendGrid webhook")
    print(f"   GET  /api/digest           ← today's digest (dashboard)")
    print(f"   GET  /api/status           ← health check\n")
    app.run(host="0.0.0.0", port=port, debug=debug)
