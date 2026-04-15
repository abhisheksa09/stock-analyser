"""
auto_login.py — Headless Upstox token refresh using TOTP

Automates the daily Upstox OAuth flow without any browser interaction.
Uses the Upstox login API directly: mobile → PIN → TOTP → auth code → token.

Required env vars (set once in Render dashboard, never change daily):
  UPSTOX_MOBILE       — Registered mobile number, digits only  e.g. 9876543210
  UPSTOX_PIN          — Your 6-digit Upstox login PIN
  UPSTOX_TOTP_SECRET  — Base-32 TOTP secret from Upstox app
                        (Settings → My Profile → Enable TOTP → "Can't scan?" shows the key)

Called automatically by APScheduler at 08:30 IST every day.
Also callable on-demand via  POST /auth/trigger-auto-login  (admin only).
"""

import os
import logging
import urllib.parse
from datetime import datetime, timezone, timedelta

import requests
import pyotp

log = logging.getLogger("auto_login")

IST            = timezone(timedelta(hours=5, minutes=30))
_AUTH_BASE     = "https://api.upstox.com/v2/login/authorization"
_COMMON_HDRS   = {
    "Accept":       "application/json",
    "Content-Type": "application/json",
    "User-Agent":   (
        "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 "
        "Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
}


# ─── Core headless login ──────────────────────────────────────────────────────

def _headless_login(api_key: str, api_secret: str, redirect_uri: str,
                    mobile: str, pin: str, totp_secret: str) -> str:
    """
    Perform headless Upstox OAuth login.
    Returns the access_token string.  Raises Exception on any failure.
    """
    s = requests.Session()
    s.headers.update(_COMMON_HDRS)

    # ── Step 1: Load auth dialog (sets cookies / CSRF state) ─────────────────
    auth_url = (
        f"{_AUTH_BASE}/dialog"
        f"?response_type=code"
        f"&client_id={urllib.parse.quote(api_key)}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
    )
    r = s.get(auth_url, timeout=15, allow_redirects=True)
    log.info("Step 1 — auth dialog: HTTP %d", r.status_code)

    # ── Step 2: Submit mobile number ──────────────────────────────────────────
    # POST to the same URL with query params so Upstox knows the OAuth context.
    # Upstox's internal API uses "mobile_number", not "mobile".
    # Add Origin + Referer so the request looks like it came from a browser.
    r = s.post(
        auth_url,
        json={"mobile_number": mobile},
        headers={
            **_COMMON_HDRS,
            "Origin":  "https://api.upstox.com",
            "Referer": auth_url,
        },
        timeout=15,
    )
    if not r.ok:
        raise Exception(f"Mobile step HTTP {r.status_code}: {r.text[:400]}")
    body = r.json()
    if body.get("status") != "success":
        raise Exception(f"Mobile step failed: {body}")
    otp_session_id = body.get("data", {}).get("otp_session_id", "")
    if not otp_session_id:
        raise Exception(f"otp_session_id missing. Keys: {list(body.get('data', {}).keys())}")
    log.info("Step 2 — mobile accepted, otp_session_id obtained")

    # ── Step 3: Submit PIN ─────────────────────────────────────────────────────
    r = s.post(
        f"{_AUTH_BASE}/dialog/pin-validation",
        json={
            "mobile_number":  mobile,
            "client_id":      api_key,
            "pin":            pin,
            "otp_session_id": otp_session_id,
        },
        headers={
            **_COMMON_HDRS,
            "Origin":  "https://api.upstox.com",
            "Referer": auth_url,
        },
        timeout=15,
    )
    if not r.ok:
        raise Exception(f"PIN step HTTP {r.status_code}: {r.text[:400]}")
    body = r.json()
    if body.get("status") != "success":
        raise Exception(f"PIN step failed: {body}")
    pin_token = body.get("data", {}).get("token", "")
    if not pin_token:
        raise Exception(f"pin token missing. Keys: {list(body.get('data', {}).keys())}")
    log.info("Step 3 — PIN validated")

    # ── Step 4: Submit TOTP ───────────────────────────────────────────────────
    totp_code = pyotp.TOTP(totp_secret).now()
    log.info("Step 4 — submitting TOTP")
    r = s.post(
        f"{_AUTH_BASE}/dialog/totp-validation",
        json={
            "mobile_number": mobile,
            "otp":           totp_code,
            "token":         pin_token,
        },
        headers={
            **_COMMON_HDRS,
            "Origin":  "https://api.upstox.com",
            "Referer": auth_url,
        },
        timeout=15,
        allow_redirects=False,   # must NOT follow — we need the Location header
    )

    # Location header carries  redirect_uri?code=xxxxx
    location = r.headers.get("Location", "")
    if not location and r.status_code == 200:
        # Some API versions return the redirect URL in the JSON body
        try:
            bj = r.json()
            location = (
                bj.get("data", {}).get("redirect_uri", "") or
                bj.get("redirect_uri", "")
            )
        except Exception:
            pass

    if not location:
        raise Exception(
            f"TOTP step did not return a redirect. "
            f"HTTP {r.status_code}, body: {r.text[:300]}"
        )

    parsed = urllib.parse.urlparse(location)
    code   = urllib.parse.parse_qs(parsed.query).get("code", [""])[0]
    if not code:
        raise Exception(f"Auth code not found in redirect URL: {location[:200]}")
    log.info("Step 4 — TOTP accepted, auth code obtained")

    # ── Step 5: Exchange auth code for access token ───────────────────────────
    token_r = s.post(
        f"{_AUTH_BASE}/token",
        data=urllib.parse.urlencode({
            "code":          code,
            "client_id":     api_key,
            "client_secret": api_secret,
            "redirect_uri":  redirect_uri,
            "grant_type":    "authorization_code",
        }).encode("utf-8"),
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept":       "application/json",
            "Api-Version":  "2.0",
        },
        timeout=15,
    )
    token_r.raise_for_status()
    access_token = token_r.json().get("access_token", "")
    if not access_token:
        keys = list(token_r.json().keys()) if isinstance(token_r.json(), dict) else "non-dict"
        raise Exception(f"access_token missing in token response. Keys present: {keys}")

    log.info("Step 5 — access token obtained (length=%d)", len(access_token))
    return access_token


# ─── Public helper ────────────────────────────────────────────────────────────

def try_auto_login(api_key: str, api_secret: str, redirect_uri: str) -> tuple:
    """
    Attempt headless login using credentials from env vars.
    Returns (success: bool, token_or_error_msg: str).
    """
    mobile      = os.environ.get("UPSTOX_MOBILE",      "").strip()
    pin         = os.environ.get("UPSTOX_PIN",          "").strip()
    totp_secret = os.environ.get("UPSTOX_TOTP_SECRET",  "").strip()

    missing = [
        name for name, val in [
            ("UPSTOX_MOBILE",      mobile),
            ("UPSTOX_PIN",         pin),
            ("UPSTOX_TOTP_SECRET", totp_secret),
        ]
        if not val
    ]
    if missing:
        msg = f"Auto-login not configured — missing env vars: {', '.join(missing)}"
        log.warning(msg)
        return False, msg

    try:
        token = _headless_login(api_key, api_secret, redirect_uri, mobile, pin, totp_secret)
        return True, token
    except Exception as exc:
        log.error("Auto-login failed: %s", exc)
        return False, str(exc)


def is_configured() -> bool:
    """Return True if all three credential env vars are set."""
    return all(
        os.environ.get(k, "").strip()
        for k in ("UPSTOX_MOBILE", "UPSTOX_PIN", "UPSTOX_TOTP_SECRET")
    )
