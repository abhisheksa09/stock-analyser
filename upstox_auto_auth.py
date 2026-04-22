"""
upstox_auto_auth.py — Fully automated Upstox token renewal via TOTP

Performs the same steps a user does manually each morning (mobile login + TOTP),
but entirely server-side using Upstox's internal login API.

Required env vars (add in Render dashboard):
  UPSTOX_MOBILE       — 10-digit mobile number registered with Upstox (no +91)
  UPSTOX_PIN          — 6-digit login PIN
  UPSTOX_TOTP_SECRET  — Base32 TOTP secret from your authenticator app
                        (Profile → Security → 2FA → "Can't scan QR?" shows secret)
  UPSTOX_API_KEY      — already set for OAuth
  UPSTOX_API_SECRET   — already set for OAuth

Already in requirements.txt:
  requests, pyotp
"""

import os
import uuid
import logging
import urllib.parse

import requests
import pyotp

log = logging.getLogger("upstox_auto_auth")

_UPSTOX_BASE   = "https://api.upstox.com"
_REDIRECT_URI  = os.environ.get("RENDER_BASE_URL", "https://nse-proxy-mojx.onrender.com").rstrip("/") + "/auth/callback"

_SESSION_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 11; Pixel 5) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":     "application/json, text/plain, */*",
    "Origin":     "https://api.upstox.com",
    "Referer":    "https://api.upstox.com/",
}


def is_configured() -> bool:
    """Return True when all 5 required env vars are present."""
    return all(
        os.environ.get(k, "").strip()
        for k in ("UPSTOX_MOBILE", "UPSTOX_PIN", "UPSTOX_TOTP_SECRET",
                  "UPSTOX_API_KEY", "UPSTOX_API_SECRET")
    )


def run_auto_auth() -> tuple:
    """
    Programmatic Upstox OAuth2 login (no browser).

    Returns (True, access_token) on success, (False, error_message) on failure.

    Flow:
      1. GET  auth dialog URL       — establishes session / cookies
      2. POST mobile + PIN          — → request_key
      3. POST TOTP code             — → auth_code
      4. POST auth_code             — → access_token  (same endpoint as /auth/callback)
    """
    mobile      = os.environ.get("UPSTOX_MOBILE", "").strip()
    pin         = os.environ.get("UPSTOX_PIN", "").strip()
    totp_secret = os.environ.get("UPSTOX_TOTP_SECRET", "").strip()
    api_key     = os.environ.get("UPSTOX_API_KEY", "").strip()
    api_secret  = os.environ.get("UPSTOX_API_SECRET", "").strip()

    if not all([mobile, pin, totp_secret, api_key, api_secret]):
        return False, "Missing env vars — need UPSTOX_MOBILE, UPSTOX_PIN, UPSTOX_TOTP_SECRET"

    redirect_uri = _REDIRECT_URI
    device_id    = str(uuid.uuid4())
    session      = requests.Session()
    session.headers.update(_SESSION_HEADERS)

    # ── Step 1: Initiate OAuth session ─────────────────────────────────────────
    auth_dialog_url = (
        f"{_UPSTOX_BASE}/v2/login/authorization/dialog"
        f"?response_type=code"
        f"&client_id={urllib.parse.quote(api_key)}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        f"&state=auto_auth"
    )
    try:
        log.info("Auto-auth step 1: initiating OAuth session")
        session.get(auth_dialog_url, allow_redirects=True, timeout=15)
    except Exception as e:
        log.warning("Step 1 warning (non-fatal): %s", e)

    # ── Step 2: POST mobile + PIN ───────────────────────────────────────────────
    try:
        log.info("Auto-auth step 2: submitting credentials for mobile %s", mobile[-4:].rjust(10, "*"))
        login_resp = session.post(
            f"{_UPSTOX_BASE}/openapi/login/v3/login",
            json={
                "mobile_num": mobile,
                "pin":        pin,
                "source":     "WEB",
                "device_id":  device_id,
            },
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        login_resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        body = e.response.text[:300] if e.response is not None else ""
        return False, f"Step 2 (login) HTTP {e.response.status_code if e.response is not None else '?'}: {body}"
    except Exception as e:
        return False, f"Step 2 (login) error: {e}"

    login_data  = login_resp.json()
    request_key = (login_data.get("data") or {}).get("request_key", "")
    if not request_key:
        return False, f"Step 2: no request_key in response — {str(login_data)[:200]}"
    log.info("Auto-auth step 2 OK — got request_key")

    # ── Step 3: POST TOTP ───────────────────────────────────────────────────────
    try:
        totp_code = pyotp.TOTP(totp_secret).now()
        log.info("Auto-auth step 3: submitting TOTP")
        totp_resp = session.post(
            f"{_UPSTOX_BASE}/openapi/login/v3/verify-totp",
            json={
                "totp":        totp_code,
                "request_key": request_key,
                "device_id":   device_id,
            },
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        totp_resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        body = e.response.text[:300] if e.response is not None else ""
        return False, f"Step 3 (TOTP) HTTP {e.response.status_code if e.response is not None else '?'}: {body}"
    except Exception as e:
        return False, f"Step 3 (TOTP) error: {e}"

    totp_data = totp_resp.json()

    # Auth code may come directly in the JSON body or in a redirect Location header
    auth_code = (totp_data.get("data") or {}).get("code", "")
    if not auth_code:
        redirect_url = (
            (totp_data.get("data") or {}).get("redirect_url", "") or
            totp_resp.headers.get("Location", "")
        )
        if redirect_url:
            parsed    = urllib.parse.urlparse(redirect_url)
            auth_code = urllib.parse.parse_qs(parsed.query).get("code", [""])[0]

    if not auth_code:
        return False, f"Step 3: no auth_code — response: {str(totp_data)[:300]}"
    log.info("Auto-auth step 3 OK — got auth_code")

    # ── Step 4: Exchange auth_code for access_token ─────────────────────────────
    try:
        log.info("Auto-auth step 4: exchanging auth_code for access_token")
        token_resp = session.post(
            f"{_UPSTOX_BASE}/v2/login/authorization/token",
            data={
                "code":          auth_code,
                "client_id":     api_key,
                "client_secret": api_secret,
                "redirect_uri":  redirect_uri,
                "grant_type":    "authorization_code",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept":       "application/json",
                "Api-Version":  "2.0",
            },
            timeout=15,
        )
        token_resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        body = e.response.text[:300] if e.response is not None else ""
        return False, f"Step 4 (token exchange) HTTP {e.response.status_code if e.response is not None else '?'}: {body}"
    except Exception as e:
        return False, f"Step 4 (token exchange) error: {e}"

    token_data   = token_resp.json()
    access_token = token_data.get("access_token", "")
    if not access_token:
        return False, f"Step 4: no access_token — keys received: {list(token_data.keys())}"

    log.info("Auto-auth complete — access_token obtained (length=%d)", len(access_token))
    return True, access_token
