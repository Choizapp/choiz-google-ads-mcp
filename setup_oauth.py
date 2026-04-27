"""One-time helper to mint a Google Ads OAuth refresh token.

Usage:
    export GOOGLE_ADS_OAUTH_CLIENT_ID="<from GCP project: APIs & Services > Credentials>"
    export GOOGLE_ADS_OAUTH_CLIENT_SECRET="<same>"
    export GOOGLE_ADS_OAUTH_LOGIN_HINT="hola@choiz.com.mx"  # optional
    python setup_oauth.py

A browser window opens; sign in as the bot account that has access to the
Google Ads MCC and approve the consent screen. The script writes
credentials.json next to itself with the resulting refresh_token.

The refresh_token is what we copy into the EC2 .env as
GOOGLE_ADS_OAUTH_REFRESH_TOKEN. credentials.json is .gitignored.
"""
import json
import os
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

CLIENT_ID = os.environ.get("GOOGLE_ADS_OAUTH_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GOOGLE_ADS_OAUTH_CLIENT_SECRET")
LOGIN_HINT = os.environ.get("GOOGLE_ADS_OAUTH_LOGIN_HINT", "")

if not CLIENT_ID or not CLIENT_SECRET:
    sys.exit(
        "Set GOOGLE_ADS_OAUTH_CLIENT_ID and GOOGLE_ADS_OAUTH_CLIENT_SECRET "
        "from the OAuth client in your GCP project (APIs & Services > Credentials)."
    )

CLIENT_SECRETS = {
    "installed": {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uris": ["http://localhost"],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
}

SCOPES = ["https://www.googleapis.com/auth/adwords"]

flow = InstalledAppFlow.from_client_config(CLIENT_SECRETS, SCOPES)
kwargs = {"port": 0, "access_type": "offline", "prompt": "consent"}
if LOGIN_HINT:
    kwargs["login_hint"] = LOGIN_HINT

creds = flow.run_local_server(**kwargs)

with open("credentials.json", "w") as f:
    json.dump(
        {
            "token": creds.token,
            "refresh_token": creds.refresh_token,
            "token_uri": creds.token_uri,
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
            "scopes": list(creds.scopes),
            "universe_domain": "googleapis.com",
            "account": LOGIN_HINT or "(unspecified)",
        },
        f,
        indent=2,
    )

print("credentials.json generated.")
print("Account hint:", LOGIN_HINT or "(none)")
print("refresh_token (copy to EC2 .env as GOOGLE_ADS_OAUTH_REFRESH_TOKEN):")
print(creds.refresh_token)
