"""
run.py — CLI entry point for the Briefly pipeline.

Full run order:
  1. Poll Gmail for new newsletters  (gmail_poller)
  2. Process each unprocessed newsletter through the AI pipeline  (processor)
  3. Run cross-newsletter synthesis  (processor)

Usage:
  python run.py                     # full run: poll + process + synthesise
  python run.py --no-poll           # skip Gmail fetch, process what's already queued
  python run.py --poll-only         # fetch from Gmail only, don't process
  python run.py --synthesis-only    # re-run synthesis without reprocessing
  python run.py --date 2026-03-04   # target a specific date for processing/synthesis
  python run.py --status            # print queue depth and exit

Railway cron (runs daily at 12:00 UTC / 7am EST):
  Configured in railway.toml — no manual crontab needed.
"""

import argparse
import os
import sys
from datetime import date

from dotenv import load_dotenv
load_dotenv()

JOB_ANALYSIS_ENABLED = os.getenv("JOB_ANALYSIS_ENABLED", "1") == "1"
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "30"))

from database import init_db, get_unprocessed_newsletters, get_full_digest_for_date, get_junk_filtered_count_for_date, purge_old_data, vacuum_if_needed
from processor import run_pipeline, run_synthesis


def cmd_status():
    queue = get_unprocessed_newsletters()
    today = date.today().isoformat()
    digest = get_full_digest_for_date(today)
    print(f"\n📊 Briefly Status — {today}")
    print(f"   Unprocessed in queue    : {len(queue)}")
    print(f"   Newsletters today       : {len(digest['newsletters'])}")
    print(f"   Themes today            : {len(digest['themes'])}")
    print(f"   Junk filtered today     : {get_junk_filtered_count_for_date(today)}")
    if queue:
        print(f"\n   Pending:")
        for n in queue:
            print(f"   • [{n['id']}] {n['sender_name']} — {n['subject'][:50]}")
    print()


def cmd_jobs() -> dict:
    print("\n💼 Starting job market analysis...")
    try:
        from job_processor import run_job_analysis
        return run_job_analysis()
    except Exception as e:
        print(f"\n❌ Job analysis error: {e}\n")
        return {"status": "failed", "error": str(e)}


def cmd_cleanup(retention_days: int) -> None:
    print(f"\n🗑  Running data retention purge (>{retention_days} days old)...")
    counts = purge_old_data(retention_days)
    print(f"   Deleted — newsletters: {counts['newsletters']} "
          f"| takeaways: {counts['takeaways']} "
          f"| articles: {counts['articles']} "
          f"| themes: {counts['themes']} "
          f"| gmail_ingested: {counts['gmail_ingested']}")

    print(f"\n🔧 Checking if VACUUM needed...")
    ran = vacuum_if_needed()
    print(f"   VACUUM: {'ran ✅' if ran else 'skipped (ran recently)'}")


def cmd_poll() -> dict:
    """Fetch new emails from Gmail. Returns poll summary."""
    try:
        from gmail_poller import poll_gmail
        return poll_gmail()
    except RuntimeError as e:
        # Auth not set up yet — surface a clear message
        print(f"\n⚠️  Gmail auth not configured: {e}")
        print("   Run 'python gmail_auth.py' to set up access.\n")
        return {"fetched": 0, "ingested": 0, "skipped": 0, "failed": 0}
    except Exception as e:
        print(f"\n❌ Gmail poll error: {e}\n")
        return {"fetched": 0, "ingested": 0, "skipped": 0, "failed": 0}


def main():
    parser = argparse.ArgumentParser(description="Briefly — newsletter digest pipeline")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--no-poll",        action="store_true", help="Skip Gmail fetch")
    parser.add_argument("--poll-only",      action="store_true", help="Only fetch from Gmail")
    parser.add_argument("--synthesis-only", action="store_true", help="Re-run synthesis only")
    parser.add_argument("--status",         action="store_true", help="Print status and exit")
    parser.add_argument("--skip-cleanup",   action="store_true",
                        help="Skip data retention purge and vacuum")
    parser.add_argument("--jobs", action="store_true",
                        help="Run job market analysis pipeline only")
    parser.add_argument("--jobs-also", action="store_true",
                        help="Run job analysis after the newsletter pipeline")
    args = parser.parse_args()

    init_db()

    if args.jobs:
        result = cmd_jobs()
        sys.exit(0 if result.get("status") == "ok" else 1)

    if args.status:
        cmd_status()
        return

    if args.synthesis_only:
        ok = run_synthesis(args.date)
        sys.exit(0 if ok else 1)

    # Gmail poll step
    if not args.no_poll:
        cmd_poll()

    if args.poll_only:
        return

    # AI processing + synthesis
    summary = run_pipeline(args.date)
    if not args.skip_cleanup:
        cmd_cleanup(DATA_RETENTION_DAYS)
    if args.jobs_also and JOB_ANALYSIS_ENABLED:
        cmd_jobs()
    sys.exit(0 if summary["newsletters_failed"] == 0 else 1)


if __name__ == "__main__":
    main()
