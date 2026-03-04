"""
run.py — CLI entry point for the Briefly AI processing pipeline.

Usage:
  python run.py                  # process today's queue + run synthesis
  python run.py --date 2026-03-04  # process + synthesise a specific date
  python run.py --synthesis-only   # skip per-newsletter processing, re-run synthesis
  python run.py --status           # print queue depth and exit

Cron (runs every morning at 7am):
  Add to crontab with: crontab -e
  0 7 * * * cd /path/to/briefly && python run.py >> logs/pipeline.log 2>&1
"""

import argparse
import sys
from datetime import date

from dotenv import load_dotenv
load_dotenv()

from database import init_db, get_unprocessed_newsletters, get_full_digest_for_date
from processor import run_pipeline, run_synthesis


def cmd_status():
    queue = get_unprocessed_newsletters()
    today = date.today().isoformat()
    digest = get_full_digest_for_date(today)

    print(f"\n📊 Briefly Status — {today}")
    print(f"   Unprocessed newsletters in queue : {len(queue)}")
    print(f"   Newsletters processed today      : {len(digest['newsletters'])}")
    print(f"   Themes synthesised today         : {len(digest['themes'])}")

    if queue:
        print(f"\n   Pending:")
        for n in queue:
            print(f"   • [{n['id']}] {n['sender_name']} — {n['subject'][:50]}")
    print()


def cmd_run(target_date: str):
    summary = run_pipeline(target_date)
    sys.exit(0 if summary["newsletters_failed"] == 0 else 1)


def cmd_synthesis_only(target_date: str):
    ok = run_synthesis(target_date)
    sys.exit(0 if ok else 1)


def main():
    parser = argparse.ArgumentParser(
        description="Briefly — AI newsletter digest pipeline"
    )
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Target date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--synthesis-only",
        action="store_true",
        help="Skip per-newsletter processing; re-run synthesis only.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print queue depth and today's digest stats, then exit.",
    )
    args = parser.parse_args()

    # Ensure DB and schema exist
    init_db()

    if args.status:
        cmd_status()
    elif args.synthesis_only:
        cmd_synthesis_only(args.date)
    else:
        cmd_run(args.date)


if __name__ == "__main__":
    main()
