#!/usr/bin/env python3
"""One-time Google OAuth2 setup. Run this first to authorize access to Google Tasks.

Creates a token file (.google_token.json) that producer.py uses for subsequent runs.

Prerequisites:
  1. Download OAuth client JSON from Google Cloud Console → Credentials
  2. Save it as client_secret.json in the project root
"""

import json
import os
from lib.config import load_config
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/tasks"]
CLIENT_SECRET_FILE = "client_secret.json"


def main():
    config = load_config()
    token_path = config["google_oauth"]["token_path"]

    if not os.path.exists(CLIENT_SECRET_FILE):
        print(f"Missing {CLIENT_SECRET_FILE}!")
        print("Download it from Google Cloud Console → Credentials → your OAuth client → Download JSON")
        return

    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)

    print("Copy the URL below into your browser to authorize:\n")

    creds = flow.run_local_server(port=0)

    with open(token_path, "w") as f:
        json.dump({
            "token": creds.token,
            "refresh_token": creds.refresh_token,
            "token_uri": creds.token_uri,
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
            "scopes": list(creds.scopes),
        }, f, indent=2)

    print(f"\nToken saved to {token_path}")
    print("You can now run producer.py to start publishing notes.")


if __name__ == "__main__":
    main()
