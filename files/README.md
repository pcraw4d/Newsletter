# Briefly — AI Newsletter Digest

Receives forwarded newsletters via SendGrid, extracts key takeaways and linked
articles using Groq (Llama 3.3 70B), synthesises cross-newsletter themes, and
serves everything via a JSON API to the dashboard.

## Stack

| Component    | Choice                          |
|--------------|---------------------------------|
| Hosting      | Railway                         |
| Database     | SQLite on Railway Volume        |
| Model API    | Groq — Llama 3.3 70B (free)    |
| Email        | SendGrid Inbound Parse          |
| Cron         | Railway Cron Service            |

---

## Project structure

```
briefly/
├── server.py          ← Flask server (webhook + dashboard API)
├── processor.py       ← AI pipeline (Groq calls, takeaways, synthesis)
├── article_fetcher.py ← Fetch & extract text from linked article URLs
├── email_parser.py    ← MIME parsing, HTML stripping, link extraction
├── database.py        ← SQLite schema + all query helpers
├── run.py             ← CLI entry point (used by Railway cron)
├── requirements.txt
├── Procfile           ← gunicorn start command for Railway web service
├── railway.toml       ← Railway service + cron configuration
└── env.example        ← copy to .env for local dev
```

---

## Local development

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure
cp env.example .env
# Edit .env:
#   GROQ_API_KEY=gsk_...          ← get from console.groq.com (free)
#   WEBHOOK_SECRET=any_string     ← any random string for local testing
#   DB_PATH=briefly.db            ← stays as default locally

# 3. Start the server
python server.py
# Server runs on http://localhost:5001

# 4. Inject a test newsletter
curl -X POST http://localhost:5001/test/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sender_email": "editor@netinterest.co",
    "sender_name": "Net Interest",
    "subject": "The Credit Cycle Turns",
    "body": "<p>Charge-offs rising across consumer fintechs. <a href=\"https://techcrunch.com/2026/03/ai-compliance\">Read the TC piece</a>.</p>"
  }'

# 5. Run the AI pipeline manually
python run.py

# 6. Check the digest API
curl http://localhost:5001/api/digest | python3 -m json.tool
```

---

## Getting your Groq API key

1. Go to **console.groq.com**
2. Sign up (free, no credit card)
3. Click **API Keys** → **Create API Key**
4. Copy and paste into `.env` as `GROQ_API_KEY`

Free tier limits: 14,400 requests/day, 500,000 tokens/minute — comfortably
enough for a personal newsletter digest running once daily.

---

## SendGrid setup (inbound email)

1. Create a free SendGrid account at **sendgrid.com**
2. Settings → Inbound Parse → **Add Host & URL**
3. Set the webhook URL to: `https://<your-railway-domain>/inbound`
4. Add your domain's MX record pointing to `mx.sendgrid.net`
5. In Gmail, create a filter: matching senders → **Forward to** your digest address

To verify the webhook secret, add a custom header in SendGrid:
  - Header name: `X-Briefly-Secret`
  - Value: same as your `WEBHOOK_SECRET` env var

---

## Railway deployment

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login and initialise
railway login
railway init

# Deploy
railway up
```

### After deploying:

**Add a Volume** (for persistent SQLite):
1. Railway dashboard → your project → **Volumes** → **New Volume**
2. Mount path: `/data`
3. Set `DB_PATH=/data/briefly.db` in Railway environment variables

**Set environment variables** in Railway dashboard → Variables:
```
GROQ_API_KEY=gsk_...
WEBHOOK_SECRET=your_secret
DB_PATH=/data/briefly.db
FLASK_DEBUG=0
```

Railway will automatically create two services from `railway.toml`:
- **web** — always running, handles inbound webhooks + serves API
- **pipeline** — runs `python run.py` daily at 12:00 UTC (7am EST)

---

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/inbound` | SendGrid webhook receiver |
| GET | `/api/digest` | Today's full digest |
| GET | `/api/digest/<YYYY-MM-DD>` | Digest for a specific date |
| GET | `/api/status` | Health check + queue depth |
| GET | `/api/newsletters/today` | Raw newsletter list |
| POST | `/test/ingest` | Inject a test email (dev only) |

---

## CLI reference

```bash
python run.py                    # process queue + run synthesis (today)
python run.py --date 2026-03-01  # process a specific date
python run.py --synthesis-only   # re-run synthesis without reprocessing
python run.py --status           # print queue depth and exit
```

## Switching AI providers

Change `MODEL_PROVIDER` in your `.env` or Railway variables — no code changes:

| Provider | .env value | API key env var |
|----------|-----------|-----------------|
| Groq (default) | `groq` | `GROQ_API_KEY` |
| Together AI | `together` | `TOGETHER_API_KEY` |
| OpenRouter | `openrouter` | `OPENROUTER_API_KEY` |
