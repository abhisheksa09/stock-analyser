"""
email_alerts.py — Email notifications for NSE Scanner events

Free option: Gmail SMTP with an App Password (built-in Python stdlib, no new deps).

Setup (one-time):
  1. Enable 2-Step Verification on your Google account.
  2. Go to https://myaccount.google.com/apppasswords
  3. Create an App Password (select "Mail" / "Other").
  4. Set these env vars on Render:
       EMAIL_TO   — recipient address (e.g. you@example.com)
       SMTP_USER  — your Gmail address (e.g. yourname@gmail.com)
       SMTP_PASS  — the 16-char App Password (spaces optional, they are stripped)

Optional overrides (defaults work for Gmail):
  SMTP_HOST  — default: smtp.gmail.com
  SMTP_PORT  — default: 587

Fires for all 5 event types:
  • green_ready       — confidence ≥ 75%, first BUY/SELL signal of session
  • conf_crossed      — confidence just crossed the threshold
  • reversal          — signal flipped direction
  • token_expiry      — Upstox token invalid / auto-renewal failed
  • eod_settlement    — daily paper-trade P&L digest at 15:35 IST
  Also sends for:
  • token_reminder    — midnight reminder to set next day's token
  • login_reminder    — 08:30 morning tap-to-login reminder
"""

import os
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta

log = logging.getLogger("email_alerts")

IST = timezone(timedelta(hours=5, minutes=30))


# ── Configuration check ───────────────────────────────────────────────────────

def is_configured() -> bool:
    """True when all three required env vars are set."""
    return bool(
        os.environ.get("EMAIL_TO",   "").strip()
        and os.environ.get("SMTP_USER", "").strip()
        and os.environ.get("SMTP_PASS", "").strip()
    )


# ── Core send ─────────────────────────────────────────────────────────────────

def send_email(subject: str, html_body: str, to_override: str = None) -> bool:
    """Send an HTML email via SMTP. Returns True on success.
    to_override lets callers send to a one-off address without changing EMAIL_TO."""
    if not is_configured():
        log.debug("Email not configured — skipping (set EMAIL_TO, SMTP_USER, SMTP_PASS)")
        return False, "Email not configured — set EMAIL_TO, SMTP_USER, SMTP_PASS"

    to_addr   = (to_override or os.environ.get("EMAIL_TO", "")).strip()
    smtp_user = os.environ.get("SMTP_USER",  "").strip()
    smtp_pass = os.environ.get("SMTP_PASS",  "").strip().replace(" ", "")
    smtp_host = os.environ.get("SMTP_HOST",  "smtp.gmail.com").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"NSE Scanner <{smtp_user}>"
    msg["To"]      = to_addr
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, [to_addr], msg.as_string())
        log.info("Email sent: %s", subject)
        return True, ""
    except smtplib.SMTPAuthenticationError as e:
        detail = f"Authentication failed — wrong SMTP_USER or SMTP_PASS (Gmail App Password required, not your account password). SMTP said: {e.smtp_error.decode(errors='replace') if hasattr(e, 'smtp_error') else e}"
        log.error("Email auth error: %s", detail)
        return False, detail
    except Exception as e:
        detail = str(e)
        log.error("Email send failed: %s", detail)
        return False, detail


# ── HTML building blocks ──────────────────────────────────────────────────────

_HEADER_COLORS = {
    "green_ready":    "#16a34a",
    "conf_crossed":   "#0369a1",
    "reversal":       "#c2410c",
    "token_expiry":   "#b91c1c",
    "token_reminder": "#374151",
    "login_reminder": "#374151",
    "eod_settlement": "#1e3a5f",
}


def _wrap(title: str, subtitle: str, body_html: str, kind: str) -> str:
    color = _HEADER_COLORS.get(kind, "#374151")
    ts    = datetime.now(IST).strftime("%d %b %Y, %H:%M IST")
    return (
        '<!DOCTYPE html><html>'
        '<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>'
        '<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,Helvetica,sans-serif;">'
        '<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;padding:24px 0;">'
        '<tr><td align="center">'
        '<table width="560" cellpadding="0" cellspacing="0" '
        'style="background:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">'
        f'<tr><td style="background:{color};padding:20px 24px;">'
        '<p style="margin:0;color:#ffffff;font-size:11px;letter-spacing:1px;text-transform:uppercase;opacity:0.85;">'
        'NSE STOCK SCANNER</p>'
        f'<h1 style="margin:4px 0 0;color:#ffffff;font-size:20px;font-weight:700;">{title}</h1>'
        f'<p style="margin:4px 0 0;color:#ffffff;font-size:13px;opacity:0.85;">{subtitle}</p>'
        '</td></tr>'
        f'<tr><td style="padding:24px;">{body_html}</td></tr>'
        '<tr><td style="background:#f9fafb;padding:12px 24px;border-top:1px solid #e5e7eb;text-align:center;">'
        f'<p style="margin:0;color:#6b7280;font-size:11px;">NSE Scanner &middot; {ts} &middot; Automated alert</p>'
        '</td></tr>'
        '</table></td></tr></table>'
        '</body></html>'
    )


def _sig_badge(sig: str) -> str:
    color = "#16a34a" if sig == "BUY" else "#dc2626"
    return (
        f'<span style="background:{color};color:#fff;padding:2px 10px;'
        f'border-radius:12px;font-size:13px;font-weight:700;">{sig}</span>'
    )


def _row(label: str, value: str) -> str:
    return (
        '<tr>'
        f'<td style="padding:7px 14px 7px 0;color:#6b7280;font-size:13px;white-space:nowrap;">{label}</td>'
        f'<td style="padding:7px 0;color:#111827;font-size:13px;">{value}</td>'
        '</tr>'
    )


def _table(*rows: str) -> str:
    return (
        '<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;">'
        + "".join(rows)
        + '</table>'
    )


def _btn(text: str, url: str, color: str = "#374151") -> str:
    return (
        f'<p style="margin:18px 0 0;">'
        f'<a href="{url}" style="background:{color};color:#fff;padding:10px 22px;'
        f'border-radius:6px;text-decoration:none;font-size:14px;font-weight:600;">'
        f'{text}</a></p>'
    )


# ── Per-event formatters ──────────────────────────────────────────────────────

def format_green_ready(s: dict) -> tuple:
    """Returns (subject, html_body) for a Green Ready alert."""
    sig   = s["sig"]
    gain  = round(abs(s["tg"] - s["en"]), 2)
    risk  = round(abs(s["sl"] - s["en"]), 2)
    ctx   = s.get("market_ctx") or {}
    warns = s.get("ctx_warnings") or []

    nifty_str = (
        f"Nifty {ctx['nifty_chg']:+.1f}% &nbsp;|&nbsp; Sector {ctx['sector_chg']:+.1f}%"
        if ctx.get("nifty_chg") is not None else "&mdash;"
    )
    warn_html = (
        f'<p style="margin:14px 0 0;color:#c2410c;font-size:12px;">&#9888; '
        + " &nbsp;|&nbsp; ".join(warns) + "</p>"
    ) if warns else ""

    body = (
        f'<p style="margin:0 0 16px;font-size:15px;font-weight:700;">'
        f'{s["sym"]} &nbsp;'
        f'<span style="color:#6b7280;font-size:13px;font-weight:400;">{s["sec"]}</span>'
        f'&nbsp; {_sig_badge(sig)}</p>'
        + _table(
            _row("Confidence", f'<b>{s["conf"]}%</b>'),
            _row("Entry",      f'Rs&nbsp;{s["en"]}'),
            _row("Target",     f'Rs&nbsp;{s["tg"]} &nbsp;<span style="color:#16a34a">+Rs&nbsp;{gain}</span>'),
            _row("Stop Loss",  f'Rs&nbsp;{s["sl"]} &nbsp;<span style="color:#dc2626">-Rs&nbsp;{risk}</span>'),
            _row("R:R",        f'{s["rr"]}:1'),
            _row("LTP",        f'Rs&nbsp;{s["ltp"]} ({s["chg"]:+.2f}%)'),
            _row("Market",     nifty_str),
            _row("Setup",      s.get("reason", "")),
        )
        + warn_html
    )
    subject = f'{"BUY" if sig == "BUY" else "SELL"} {s["sym"]} — Green Ready ({s["conf"]}% conf) | NSE Scanner'
    return subject, _wrap("Ready to Trade", f'{s["sym"]} · {sig} signal', body, "green_ready")


def format_conf_crossed(s: dict, prev_conf: int, threshold: int) -> tuple:
    """Returns (subject, html_body) for a Confidence Crossed alert."""
    body = (
        f'<p style="margin:0 0 16px;font-size:15px;font-weight:700;">'
        f'{s["sym"]} &nbsp;'
        f'<span style="color:#6b7280;font-size:13px;font-weight:400;">{s["sec"]}</span>'
        f'&nbsp; {_sig_badge(s["sig"])}</p>'
        + _table(
            _row("Confidence",
                 f'<b>{s["conf"]}%</b> '
                 f'<span style="color:#6b7280;font-size:12px;">(was {prev_conf}%)</span>'),
            _row("Entry",     f'Rs&nbsp;{s["en"]}'),
            _row("Stop Loss", f'Rs&nbsp;{s["sl"]}'),
            _row("LTP",       f'Rs&nbsp;{s["ltp"]} ({s["chg"]:+.2f}%)'),
        )
    )
    subject = f'{s["sym"]} crossed {threshold}% confidence | NSE Scanner'
    return subject, _wrap(
        f"Confidence Crossed {threshold}%",
        f'{s["sym"]} · now {s["conf"]}%',
        body, "conf_crossed",
    )


def format_reversal(s: dict, locked_sig: str) -> tuple:
    """Returns (subject, html_body) for a Signal Reversal alert."""
    body = (
        f'<p style="margin:0 0 16px;font-size:15px;font-weight:700;">'
        f'{s["sym"]} &nbsp;'
        f'<span style="color:#6b7280;font-size:13px;font-weight:400;">{s["sec"]}</span></p>'
        + _table(
            _row("Was",  _sig_badge(locked_sig)),
            _row("Now",  _sig_badge(s["sig"])),
            _row("LTP",  f'Rs&nbsp;{s["ltp"]} ({s["chg"]:+.2f}%)'),
        )
        + '<p style="margin:16px 0 0;color:#c2410c;font-weight:700;font-size:13px;">'
          '&#9888; Conflicting signals — skip this trade today</p>'
    )
    subject = f'{s["sym"]} signal reversed ({locked_sig} → {s["sig"]}) | NSE Scanner'
    return subject, _wrap(
        "Signal Reversal",
        f'{s["sym"]} · {locked_sig} → {s["sig"]}',
        body, "reversal",
    )


def format_token_expiry(http_code, login_url: str) -> tuple:
    """Returns (subject, html_body) for a token-expired operational alert."""
    body = (
        f'<p style="margin:0 0 12px;color:#111827;font-size:14px;">'
        f'The Upstox token is invalid (HTTP&nbsp;{http_code or "error"}). '
        f'Auto-renewal failed or was not available.</p>'
        f'<p style="color:#6b7280;font-size:13px;">The scanner is paused until a valid '
        f'token is set. Please log in to Upstox to get a fresh token.</p>'
        + (_btn("Renew Token", login_url, "#b91c1c") if login_url else
           '<p style="margin:16px 0 0;color:#6b7280;font-size:13px;">Go to your Render app &rarr; /auth/login</p>')
    )
    subject = "⚠️ NSE Scanner: Upstox token expired — action required"
    return subject, _wrap("Token Invalid", "Scanner paused — login required", body, "token_expiry")


def format_token_reminder(login_url: str) -> tuple:
    """Returns (subject, html_body) for the midnight token-reminder job."""
    body = (
        '<p style="margin:0 0 12px;color:#111827;font-size:14px;">'
        'Market opens in ~9&frac12; hours. Please log in to Upstox now so '
        "tomorrow's scan runs automatically.</p>"
        '<p style="color:#6b7280;font-size:13px;">After login the token is saved '
        'automatically &mdash; nothing else needed.</p>'
        + (_btn("Login to Upstox", login_url) if login_url else
           '<p style="margin:16px 0 0;color:#6b7280;font-size:13px;">Go to your Render app &rarr; /auth/login</p>')
    )
    subject = "⏰ NSE Scanner: Set tomorrow's Upstox token"
    return subject, _wrap("Set Tomorrow's Token", "Log in before market open", body, "token_reminder")


def format_login_reminder(login_url: str) -> tuple:
    """Returns (subject, html_body) for the 08:30 morning login reminder."""
    body = (
        '<p style="margin:0 0 12px;color:#111827;font-size:14px;">'
        'Good morning! Market opens in ~45 minutes.</p>'
        '<p style="color:#6b7280;font-size:13px;">Log in to Upstox to activate '
        "today's scanner. Scanning starts automatically at 09:15 IST once you log in.</p>"
        + (_btn("Activate Scanner", login_url) if login_url else
           '<p style="margin:16px 0 0;color:#6b7280;font-size:13px;">Go to your Render app &rarr; /auth/login</p>')
    )
    subject = "☀️ NSE Scanner: Good morning — activate scanner"
    return subject, _wrap("Morning Reminder", "Market opens at 09:15 IST", body, "login_reminder")


def format_eod_settlement(trades: list, settled: int, skipped: int, errors: list) -> tuple:
    """Returns (subject, html_body) for the EOD paper-trade settlement digest."""
    date_str  = datetime.now(IST).strftime("%d %b %Y")
    won       = [t for t in trades if t.get("outcome") in ("won",  "partial_win")]
    lost      = [t for t in trades if t.get("outcome") in ("lost", "partial_loss")]
    still_open = [t for t in trades if t.get("outcome") == "open"]
    pnl_list  = [float(t["pnl_pts"]) for t in trades if t.get("pnl_pts") is not None]
    total_pnl = round(sum(pnl_list), 2) if pnl_list else 0
    win_rate  = round(len(won) / (len(won) + len(lost)) * 100) if (won or lost) else 0
    pnl_color = "#16a34a" if total_pnl >= 0 else "#dc2626"

    summary = _table(
        _row("Settled",  str(settled)),
        _row("Won",   f'<span style="color:#16a34a;font-weight:700;">{len(won)}</span>'),
        _row("Lost",  f'<span style="color:#dc2626;font-weight:700;">{len(lost)}</span>'),
        _row("Win rate", f'<b>{win_rate}%</b>' if (won or lost) else "&mdash;"),
        _row("Net P&amp;L",
             f'<span style="color:{pnl_color};font-weight:700;">{total_pnl:+.2f}&nbsp;pts</span>'
             if pnl_list else "&mdash;"),
        *([_row("Still open", str(len(still_open)))] if still_open else []),
        *([_row("Errors",     str(skipped))]         if skipped    else []),
    )

    # Per-trade rows table
    trade_table_html = ""
    if trades:
        rows_html = ""
        for t in sorted(trades, key=lambda x: x.get("signal_time", "")):
            outcome = t.get("outcome", "open")
            pnl     = t.get("pnl_pts")
            if outcome in ("won", "partial_win"):
                oc, ot = "#16a34a", outcome.replace("_", " ").title()
            elif outcome in ("lost", "partial_loss"):
                oc, ot = "#dc2626", outcome.replace("_", " ").title()
            else:
                oc, ot = "#6b7280", "Open"
            pnl_str = f"{float(pnl):+.2f}" if pnl is not None else "&mdash;"
            sig_icon = "&#9650;" if t.get("sig") == "BUY" else "&#9660;"
            rows_html += (
                '<tr style="border-bottom:1px solid #f3f4f6;">'
                f'<td style="padding:8px 6px;font-size:12px;font-weight:600;">{t.get("sym","")}</td>'
                f'<td style="padding:8px 6px;font-size:12px;">{sig_icon} {t.get("sig","")}</td>'
                f'<td style="padding:8px 6px;font-size:12px;text-align:right;">Rs&nbsp;{t.get("entry","")}</td>'
                f'<td style="padding:8px 6px;font-size:12px;text-align:right;">'
                f'Rs&nbsp;{t.get("close_price") or "&mdash;"}</td>'
                f'<td style="padding:8px 6px;font-size:12px;text-align:right;color:{oc};font-weight:600;">{ot}</td>'
                f'<td style="padding:8px 6px;font-size:12px;text-align:right;'
                f'color:{"#16a34a" if pnl and float(pnl) >= 0 else "#dc2626"};">{pnl_str}</td>'
                '</tr>'
            )
        trade_table_html = (
            '<h3 style="margin:20px 0 8px;font-size:12px;color:#374151;font-weight:600;'
            'text-transform:uppercase;letter-spacing:0.5px;">Trade Details</h3>'
            '<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;">'
            '<tr style="border-bottom:2px solid #e5e7eb;">'
            '<th style="padding:6px;text-align:left;color:#6b7280;font-size:11px;font-weight:600;">Symbol</th>'
            '<th style="padding:6px;text-align:left;color:#6b7280;font-size:11px;font-weight:600;">Signal</th>'
            '<th style="padding:6px;text-align:right;color:#6b7280;font-size:11px;font-weight:600;">Entry</th>'
            '<th style="padding:6px;text-align:right;color:#6b7280;font-size:11px;font-weight:600;">Close</th>'
            '<th style="padding:6px;text-align:right;color:#6b7280;font-size:11px;font-weight:600;">Result</th>'
            '<th style="padding:6px;text-align:right;color:#6b7280;font-size:11px;font-weight:600;">P&amp;L&nbsp;pts</th>'
            '</tr>'
            + rows_html
            + '</table>'
        )

    error_html = (
        f'<p style="margin:12px 0 0;color:#c2410c;font-size:12px;">Errors: '
        + ", ".join(errors[:5]) + "</p>"
    ) if errors else ""

    body = (
        f'<h3 style="margin:0 0 12px;font-size:12px;color:#374151;font-weight:600;'
        f'text-transform:uppercase;letter-spacing:0.5px;">Summary — {date_str}</h3>'
        + summary
        + trade_table_html
        + error_html
    )
    pnl_sign = f"{total_pnl:+.2f}" if pnl_list else "0.00"
    subject  = (
        f"NSE Scanner: EOD — {settled} trades settled, "
        f"{win_rate}% win rate, {pnl_sign} pts P&L"
    )
    return subject, _wrap(
        "EOD Settlement",
        f"{date_str} · {settled} trades settled",
        body, "eod_settlement",
    )
