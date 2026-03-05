# Briefly — AI Newsletter Digest

Polls newsletters from Gmail via the Gmail API, extracts key takeaways and linked
articles using Google Gemini 2.5 Flash, synthesises cross-newsletter themes, and
serves everything via a JSON API and dashboard.

## Stack

| Component | Choice |
|-----------|--------|
| Hosting | Railway |
| Database | SQLite on Railway Volume (mount at `/data`, set `DB_PATH=/data/briefly.db`) |
| AI Model | Google Gemini 2.5 Flash via OpenAI-compatible endpoint (free tier) |
| Email | Gmail API polling (no SendGrid, no MX records) |
| Cron | Railway Cron Service (defined in `railway.toml`) |
| Job Data | Adzuna Jobs API (free tier) |

---

## Project structure

```
├── README.md
├── files/                    ← All app code (run from here)
│   ├── server.py             ← Flask server (dashboard API + pull trigger)
│   ├── processor.py          ← AI pipeline (Gemini calls, takeaways, synthesis)
│   ├── article_fetcher.py    ← Fetch & extract text from linked article URLs
│   ├── email_parser.py       ← MIME parsing, HTML stripping, link extraction
│   ├── database.py           ← SQLite schema + query helpers
│   ├── run.py                ← CLI entry point (used by Railway cron)
│   ├── job_fetcher.py        ← Fetch PM job postings from Adzuna API
│   ├── job_processor.py      ← AI skill extraction + weekly trend synthesis
│   ├── gmail_auth.py         ← OAuth flow to generate gmail_token.json
│   ├── gmail_poller.py       ← Poll Gmail for newsletters in label
│   ├── requirements.txt
│   ├── Procfile              ← gunicorn start command for Railway web service
│   ├── railway.toml          ← Railway service + cron configuration
│   ├── env.example           ← copy to .env for local dev
│   └── index.html            ← Dashboard SPA
├── newsletter-digest.jsx     ← Figma design (optional)
└── .gitignore
```

---

## Local development

All commands below run from the `files/` directory.

### Setup order

1. **Install dependencies**
   ```bash
   cd files
   pip install -r requirements.txt
   ```

2. **Get Gemini API key** — [aistudio.google.com](https://aistudio.google.com) → Get API Key (free, no credit card)

3. **Set up Gmail OAuth**
   - [console.cloud.google.com](https://console.cloud.google.com) → create project → enable Gmail API
   - Create **Desktop** OAuth credentials
   - Save as `gmail_credentials.json` in the `files/` directory
   - Run `python gmail_auth.py` to generate `gmail_token.json`

4. **Create Gmail label** — Create a label called `Newsletters` and apply it to newsletter senders via Gmail filters. Or set `GMAIL_SENDERS` to a comma-separated list of sender addresses.

5. **Configure environment**
   ```bash
   cp env.example .env
   ```
   Fill in `GEMINI_API_KEY` and `GMAIL_TOKEN_JSON` (paste full contents of `gmail_token.json`)

6. **Start server**
   ```bash
   python server.py
   ```
   Open http://localhost:5001

7. **Run pipeline** — Press "Pull Now" in the UI, or run `python run.py` from CLI

### Test ingest (optional)

```bash
curl -X POST http://localhost:5001/test/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sender_email": "editor@netinterest.co",
    "sender_name": "Net Interest",
    "subject": "The Credit Cycle Turns",
    "body": "<p>Charge-offs rising across consumer fintechs. <a href=\"https://example.com/article\">Read more</a>.</p>"
  }'
```

---

## Job analysis setup

1. Register at [developer.adzuna.com](https://developer.adzuna.com) → My Apps → Create App
2. Copy App ID and App Key into `.env`
3. Run manually: `python run.py --jobs`
4. Railway runs this automatically every Monday at 9am UTC via the `job-analysis` cron service defined in `railway.toml`

---

## Railway deployment

```bash
railway login
railway init
railway up
```

**Note:** If your app code is in `files/`, set **Root Directory** to `files` in the Railway dashboard (Settings → General) so `server.py` and `run.py` are found.

### After deploying

1. **Add Volume** — Railway dashboard → Volumes → New Volume → mount at `/data`

2. **Set environment variables** (Railway dashboard → Variables):

   | Variable | Value |
   |----------|-------|
   | `GEMINI_API_KEY` | From aistudio.google.com |
   | `GMAIL_TOKEN_JSON` | Full JSON from `gmail_token.json` |
   | `GMAIL_LABEL` | `Newsletters` (or your label name) |
   | `DB_PATH` | `/data/briefly.db` |
   | `FLASK_DEBUG` | `0` |
   | `DATA_RETENTION_DAYS` | `30` (optional, purges old data) |

3. Railway auto-creates from `railway.toml`:
   - **web** — Flask server (inbound API + dashboard)
   - **pipeline** — Cron job (runs `python run.py` daily at 12:00 UTC)
   - **job-analysis** — Cron job (runs `python run.py --jobs` Mondays at 9:00 UTC)

---

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Dashboard SPA |
| GET | `/api/digest` | Today's full digest |
| GET | `/api/digest/<YYYY-MM-DD>` | Digest for a specific date |
| GET | `/api/status` | Health check + queue depth |
| GET | `/api/pull/status` | Pipeline job state (running, result, log) |
| POST | `/api/pull` | Trigger Gmail poll + AI pipeline (returns 202) |
| GET | `/api/jobs/latest` | Most recent job skill analysis |
| GET | `/api/jobs/<YYYY-MM-DD>` | Job analysis for a specific week |
| POST | `/api/jobs/pull` | Trigger a job analysis run (returns 202) |
| GET | `/api/newsletters/today` | Raw newsletter list |
| POST | `/test/ingest` | Inject a test email (dev only) |

---

## CLI reference

| Command | Description |
|---------|-------------|
| `python run.py` | Full run: poll Gmail + process + synthesise |
| `python run.py --no-poll` | Skip Gmail fetch, process queued only |
| `python run.py --poll-only` | Fetch from Gmail only, no processing |
| `python run.py --synthesis-only` | Re-run synthesis without reprocessing |
| `python run.py --date 2026-03-04` | Target a specific date |
| `python run.py --status` | Print queue depth and exit |
| `python run.py --skip-cleanup` | Skip data retention purge and vacuum |
| `python run.py --jobs` | Run job market analysis only |
| `python run.py --jobs-also` | Run after newsletter pipeline |

---

## Switching AI providers

Set `MODEL_PROVIDER` in `.env` or Railway variables — no code changes. Optionally set `MODEL_NAME` to override the default model.

| Provider | .env value | API key env var |
|----------|------------|-----------------|
| Gemini (default) | `gemini` | `GEMINI_API_KEY` |
| Groq | `groq` | `GROQ_API_KEY` |
| Together AI | `together` | `TOGETHER_API_KEY` |
| OpenRouter | `openrouter` | `OPENROUTER_API_KEY` |
