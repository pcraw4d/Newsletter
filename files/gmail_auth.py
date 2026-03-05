"""
gmail_auth.py — One-time OAuth2 setup for Gmail access.

Run this ONCE locally to authorise Briefly to read your Gmail:

  python gmail_auth.py

It will open a browser, ask you to sign in with Google and grant access,
then save a token file (gmail_token.json) that the poller uses from then on.
The token auto-refreshes — you should never need to run this again unless
you revoke access in your Google account settings.

Prerequisites:
  1. Go to console.cloud.google.com
  2. Create a project (e.g. "Briefly")
  3. Enable the Gmail API  (APIs & Services → Library → Gmail API)
  4. Create OAuth credentials (APIs & Services → Credentials → Create →
     OAuth client ID → Desktop app)
  5. Download the JSON and save as 'gmail_credentials.json' in this folder
  6. Run: python gmail_auth.py

On Railway:
  After running locally, copy the contents of gmail_token.json and set it
  as the GMAIL_TOKEN_JSON environment variable in Railway dashboard.
  The poller reads from env if the file isn't present.
"""

import json
import os
import sys
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# We only need read access to Gmail messages
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

CREDS_FILE  = Path("gmail_credentials.json")
TOKEN_FILE  = Path("gmail_token.json")


def run_auth_flow():
    if not CREDS_FILE.exists():
        print(f"""
❌  gmail_credentials.json not found.

To set up Gmail access:

  1. Go to console.cloud.google.com
  2. Create a project called "Briefly" (or any name)
  3. APIs & Services → Library → search "Gmail API" → Enable
  4. APIs & Services → Credentials → Create Credentials → OAuth client ID
     → Application type: Desktop app → Create
  5. Download the JSON file and save it as:
       {CREDS_FILE.absolute()}
  6. Run this script again: python gmail_auth.py
""")
        sys.exit(1)

    flow = InstalledAppFlow.from_client_secrets_file(str(CREDS_FILE), SCOPES)
    creds = flow.run_local_server(port=0, open_browser=True)

    TOKEN_FILE.write_text(creds.to_json())
    print(f"""
✅  Authorisation complete!
    Token saved to: {TOKEN_FILE.absolute()}

For Railway deployment, set this environment variable:
  GMAIL_TOKEN_JSON = {creds.to_json()!r}

You're ready to run: python run.py
""")


def get_credentials() -> Credentials:
    """
    Load credentials from token file or GMAIL_TOKEN_JSON env var.
    Auto-refreshes the access token when expired.
    Called by gmail_poller.py — not by end users.
    """
    creds = None

    # 1. Try env var first (Railway / production)
    token_json = os.getenv("GMAIL_TOKEN_JSON")
    if token_json:
        try:
            creds = Credentials.from_authorized_user_info(
                json.loads(token_json), SCOPES
            )
        except Exception as e:
            raise RuntimeError(f"Invalid GMAIL_TOKEN_JSON: {e}")

    # 2. Fall back to local file (dev)
    elif TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    else:
        raise RuntimeError(
            "No Gmail credentials found. "
            "Run 'python gmail_auth.py' to authorise, or set GMAIL_TOKEN_JSON."
        )

    # Refresh if expired
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            # Persist refreshed token back to file if running locally
            if TOKEN_FILE.exists():
                TOKEN_FILE.write_text(creds.to_json())
        else:
            raise RuntimeError(
                "Gmail token is invalid and cannot be refreshed. "
                "Re-run 'python gmail_auth.py' to re-authorise."
            )

    return creds


if __name__ == "__main__":
    run_auth_flow()
