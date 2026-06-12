import json
import os
import time

import requests

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
STATES_URL = "https://opensky-network.org/api/states/all"


def retry_delay(attempt, headers=None):
    headers = headers or {}
    for name in ("X-Rate-Limit-Retry-After-Seconds", "Retry-After"):
        try:
            return max(1, float(headers[name]))
        except (KeyError, ValueError, TypeError):
            pass
    return min(900, 5 * 2 ** min(attempt, 8))


class OpenSky:
    def __init__(self, settings):
        self.settings = settings
        self.session = requests.Session()
        self.token = None
        self.expires_at = 0

    def authenticate(self):
        cid = os.getenv("OPENSKY_CLIENT_ID")
        secret = os.getenv("OPENSKY_CLIENT_SECRET")
        if not cid or not secret:
            with open(self.settings.credentials_file, encoding="utf-8-sig") as f:
                credentials = json.load(f)
            cid, secret = credentials["clientId"], credentials["clientSecret"]
        response = self.session.post(TOKEN_URL, data={"grant_type": "client_credentials",
            "client_id": cid, "client_secret": secret}, timeout=30)
        response.raise_for_status()
        token = response.json()
        self.token = token["access_token"]
        self.expires_at = time.monotonic() + max(1, token.get("expires_in", 300) - 30)

    def poll(self):
        if time.monotonic() >= self.expires_at:
            self.authenticate()
        response = self.session.get(STATES_URL, params=self.settings.bbox,
            headers={"Authorization": "Bearer " + self.token}, timeout=30)
        if response.status_code == 401:
            self.expires_at = 0
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("time"), int):
            raise ValueError("Invalid API envelope")
        if payload.get("states") is not None and not isinstance(payload["states"], list):
            raise ValueError("Invalid states collection")
        return payload, response.headers
