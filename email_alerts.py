"""
email_alerts.py — Email notifications for NSE Scanner events

Uses Resend (https://resend.com) — free tier: 3,000 emails/month.
Sends over HTTPS so it works on Render (SMTP port 587 is blocked there).

Setup (one-time):
  1. Sign up at https://resend.com (free, no credit card)
  2. Go to API Keys → Create API Key → copy it
  3. Set these env vars on Render:
       RESEND_API_KEY  — your Resend API key (re_xxxxxxxxxxxx)
       EMAIL_TO        — must be the email address you signed up to Resend with
                         (free tier restriction — to send to any address, verify a
                          domain at resend.com/domains and set EMAIL_FROM to use it)

Optional:
  EMAIL_FROM  — sender shown in inbox
                default: "NSE Scanner <onboarding@resend.dev>"

Fires for all 5 event types:
  • green_ready       — confidence ≥ 75%, first BUY/SELL signal of session
  • conf_crossed      — confidence just crossed the threshold
  • reversal          — signal flipped direction
  • token_expiry      — Upstox token invalid / auto-renewal failed
  • eod_settlement    — daily paper-trade P&L digest at 15:35 IST
  Also sends for:
  • login_reminder    — 08:30 morning tap-to-login reminder
"""

import os
import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

log = logging.getLogger("email_alerts")

IST = timezone(timedelta(hours=5, minutes=30))

RESEND_API = "https://api.resend.com/emails"


# ── Configuration check ───────────────────────────────────────────────────────

def is_configured() -> bool:
    """True when the required env vars are set."""
    return bool(
        os.environ.get("RESEND_API_KEY", "").strip()
        and os.environ.get("EMAIL_TO",    "").strip()
    )


# ── Core send ─────────────────────────────────────────────────────────────────

def send_email(subject: str, html_body: str, to_override: str = None) -> tuple:
    """Send an HTML email via Resend API. Returns (success: bool, detail: str).
    to_override sends to a one-off address without changing EMAIL_TO env var."""
    if not is_configured():
        log.debug("Email not configured — skipping (set RESEND_API_KEY and EMAIL_TO)")
        return False, "Email not configured — set RESEND_API_KEY and EMAIL_TO on Render"

    api_key   = os.environ.get("RESEND_API_KEY", "").strip()
    to_addr   = (to_override or os.environ.get("EMAIL_TO", "")).strip()
    from_addr = os.environ.get("EMAIL_FROM", "NSE Scanner <onboarding@resend.dev>").strip()

    payload = json.dumps({
        "from":    from_addr,
        "to":      [to_addr],
        "subject": subject,
        "html":    html_body,
    }).encode("utf-8")

    req = urllib.request.Request(
        RESEND_API,
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
            "User-Agent":    "NSEScanner/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            json.loads(r.read())
        log.info("Email sent: %s", subject)
        return True, ""
    except urllib.error.HTTPError as e:
        body   = e.read().decode(errors="replace")
        detail = f"Resend API error {e.code}: {body[:300]}"
        log.error("Email send failed: %s", detail)
        return False, detail
    except Exception as e:
        detail = str(e)
        log.error("Email send failed: %s", detail)
        return False, detail


# ── HTML building blocks ──────────────────────────────────────────────────────

_HEADER_COLORS = {
    "green_ready":          "#16a34a",
    "conf_crossed":         "#0369a1",
    "reversal":             "#c2410c",
    "token_expiry":         "#b91c1c",
    "login_reminder":       "#374151",
    "eod_settlement":       "#1e3a5f",
    "evening_picks":        "#7c3aed",
    "real_trade_candidate": "#b45309",
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


def _analysis_row_html(t: dict) -> str:
    """Return a colspan-6 sub-row with pick analysis for one settled trade."""
    reason      = t.get("reason") or "Signal conditions met"
    conf        = t.get("conf")
    rr          = t.get("rr")
    rsi         = t.get("rsi")
    sec         = t.get("sec") or ""
    sig         = t.get("sig", "")
    signal_time = t.get("signal_time") or ""
    entry       = t.get("entry")
    target      = t.get("target")
    sl          = t.get("stop_loss")
    day_high    = t.get("day_high")
    day_low     = t.get("day_low")
    target_hit  = t.get("target_hit")
    sl_hit      = t.get("sl_hit")

    def _pill(text, bg, fg):
        return (
            f'<span style="background:{bg};color:{fg};padding:2px 8px;border-radius:10px;'
            f'font-size:10px;font-weight:700;margin-right:4px;display:inline-block;">{text}</span>'
        )

    pills = []

    # Confidence pill
    if conf is not None:
        ci = int(conf)
        if ci >= 75:
            cc, cl = "#15803d", "HIGH"
        elif ci >= 55:
            cc, cl = "#b45309", "MED"
        else:
            cc, cl = "#6b7280", "LOW"
        pills.append(_pill(f"Conf {ci}% ● {cl}", f"{cc}18", cc))

    # R:R pill
    if rr is not None:
        rf = float(rr)
        rc = "#15803d" if rf >= 2.0 else ("#b45309" if rf >= 1.5 else "#b91c1c")
        pills.append(_pill(f"R:R {rf:.1f}:1", f"{rc}18", rc))

    # RSI pill
    if rsi is not None:
        rv = float(rsi)
        if rv < 40:
            rl, rc2 = "Oversold", "#15803d" if sig == "SELL" else "#b45309"
        elif rv > 60:
            rl, rc2 = "Overbought", "#b91c1c" if sig == "BUY" else "#15803d"
        else:
            rl, rc2 = "Neutral", "#4b5563"
        pills.append(_pill(f"RSI {rv:.0f} – {rl}", f"{rc2}18", rc2))

    # Signal timing pill
    if signal_time:
        try:
            h, m  = int(signal_time[:2]), int(signal_time[3:5])
            prime = 585 <= h * 60 + m <= 660  # 09:45–11:00 prime window
            tc    = "#0369a1" if prime else "#6b7280"
            tlbl  = f"{signal_time} ✓ Prime" if prime else signal_time
            pills.append(_pill(tlbl, f"{tc}18", tc))
        except Exception:
            pass

    # Sector pill
    if sec:
        pills.append(_pill(sec, "#f3f4f6", "#374151"))

    # Trade geometry line
    geo_parts = []
    if entry is not None and target is not None and sl is not None:
        ef, tf, sf = float(entry), float(target), float(sl)
        geo_parts.append(f"Target &#177;{abs(tf - ef):.1f} pts")
        geo_parts.append(f"SL &#177;{abs(ef - sf):.1f} pts")
    if day_high is not None and day_low is not None:
        geo_parts.append(f"Day range &#8377;{float(day_low):.0f}–{float(day_high):.0f}")

    # Outcome insight line
    outcome = t.get("outcome", "open")
    if target_hit:
        insight = ('<span style="color:#15803d;font-size:10px;font-weight:600;">'
                   '✓ Target reached — setup fully confirmed</span>')
    elif sl_hit:
        insight = ('<span style="color:#b91c1c;font-size:10px;font-weight:600;">'
                   '✕ Stop loss triggered — setup invalidated at SL</span>')
    elif outcome == "partial_win":
        insight = ('<span style="color:#b45309;font-size:10px;">'
                   'Partial win — moved in direction but target not reached by close</span>')
    elif outcome == "partial_loss":
        insight = ('<span style="color:#b91c1c;font-size:10px;">'
                   'Partial loss — closed against entry; SL not triggered intraday</span>')
    else:
        insight = ""

    pills_html  = "".join(pills)
    geo_html    = (
        f'<p style="margin:4px 0 0;font-size:10px;color:#6b7280;">'
        + " &nbsp;|&nbsp; ".join(geo_parts) + "</p>"
    ) if geo_parts else ""
    insight_html = f'<p style="margin:4px 0 0;">{insight}</p>' if insight else ""

    return (
        '<tr style="background:#f9fafb;">'
        '<td colspan="6" style="padding:5px 8px 10px 16px;border-bottom:2px solid #e5e7eb;">'
        f'<p style="margin:0 0 5px;font-size:11px;color:#374151;font-style:italic;">'
        f'\U0001f50d {reason}</p>'
        f'<div style="line-height:1.8;">{pills_html}</div>'
        + geo_html
        + insight_html
        + '</td></tr>'
    )


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
                '<tr>'
                f'<td style="padding:8px 6px;font-size:12px;font-weight:600;">{t.get("sym","")}</td>'
                f'<td style="padding:8px 6px;font-size:12px;">{sig_icon} {t.get("sig","")}</td>'
                f'<td style="padding:8px 6px;font-size:12px;text-align:right;">Rs&nbsp;{t.get("entry","")}</td>'
                f'<td style="padding:8px 6px;font-size:12px;text-align:right;">'
                f'Rs&nbsp;{t.get("close_price") or "&mdash;"}</td>'
                f'<td style="padding:8px 6px;font-size:12px;text-align:right;color:{oc};font-weight:600;">{ot}</td>'
                f'<td style="padding:8px 6px;font-size:12px;text-align:right;'
                f'color:{"#16a34a" if pnl and float(pnl) >= 0 else "#dc2626"};">{pnl_str}</td>'
                '</tr>'
                + _analysis_row_html(t)
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


def format_evening_picks(picks: list) -> tuple:
    """Returns (subject, html_body) for the evening watchlist email."""
    date_str = datetime.now(IST).strftime("%d %b %Y")
    rows = []
    for i, p in enumerate(picks, 1):
        gain = round(abs(p["tg"] - p["en"]), 2)
        risk = round(abs(p["sl"] - p["en"]), 2)
        rows.append(
            f'<tr style="border-bottom:1px solid #e5e7eb;">'
            f'<td style="padding:10px 8px;font-weight:700;font-size:14px;">{i}. {p["sym"]}</td>'
            f'<td style="padding:10px 8px;">{_sig_badge(p["sig"])}</td>'
            f'<td style="padding:10px 8px;color:#374151;font-size:13px;">{p["sec"]}</td>'
            f'<td style="padding:10px 8px;font-weight:700;">{p["conf"]}%</td>'
            f'<td style="padding:10px 8px;font-size:13px;">Rs&nbsp;{p["en"]}</td>'
            f'<td style="padding:10px 8px;font-size:13px;color:#16a34a;">Rs&nbsp;{p["tg"]}&nbsp;(+{gain})</td>'
            f'<td style="padding:10px 8px;font-size:13px;color:#dc2626;">Rs&nbsp;{p["sl"]}&nbsp;(-{risk})</td>'
            f'<td style="padding:10px 8px;font-size:13px;">{p["rr"]}:1</td>'
            f'</tr>'
        )
    table_html = (
        '<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;">'
        '<tr style="background:#f3f4f6;">'
        '<th style="padding:8px;text-align:left;font-size:12px;color:#6b7280;">#&nbsp;Stock</th>'
        '<th style="padding:8px;text-align:left;font-size:12px;color:#6b7280;">Signal</th>'
        '<th style="padding:8px;text-align:left;font-size:12px;color:#6b7280;">Sector</th>'
        '<th style="padding:8px;text-align:left;font-size:12px;color:#6b7280;">Conf</th>'
        '<th style="padding:8px;text-align:left;font-size:12px;color:#6b7280;">Entry</th>'
        '<th style="padding:8px;text-align:left;font-size:12px;color:#6b7280;">Target</th>'
        '<th style="padding:8px;text-align:left;font-size:12px;color:#6b7280;">Stop&nbsp;Loss</th>'
        '<th style="padding:8px;text-align:left;font-size:12px;color:#6b7280;">R:R</th>'
        '</tr>'
        + "".join(rows)
        + '</table>'
    )
    note = (
        '<p style="margin:16px 0 0;font-size:13px;color:#6b7280;">'
        '&#9432; If any of these stocks appear in tomorrow\'s 9:45&nbsp;AM scan, '
        'a <b>Real Trade Candidate</b> alert will fire automatically.</p>'
    )
    body = (
        f'<p style="margin:0 0 16px;font-size:14px;color:#374151;">'
        f'Top {len(picks)} stock{"s" if len(picks) != 1 else ""} with strong signals at market close. '
        f'Watch these tomorrow morning.</p>'
        + table_html + note
    )
    subject = f"NSE Scanner: Evening Watchlist — {len(picks)} picks for {date_str}"
    return subject, _wrap(
        "Evening Watchlist",
        f"{date_str} · {len(picks)} strong picks for tomorrow",
        body, "evening_picks",
    )


def format_real_trade_candidate(s: dict, evening_pick: dict) -> tuple:
    """Returns (subject, html_body) for a Real Trade Candidate alert."""
    date_str = datetime.now(IST).strftime("%d %b %Y")
    gain = round(abs(s["tg"] - s["en"]), 2)
    risk = round(abs(s["sl"] - s["en"]), 2)
    body = (
        '<div style="background:#fef3c7;border-left:4px solid #d97706;'
        'padding:12px 16px;margin-bottom:16px;border-radius:4px;">'
        '<p style="margin:0;font-size:13px;font-weight:700;color:#92400e;">'
        '&#9889; Confirmed in both last night\'s evening watchlist and this morning\'s scan</p>'
        '</div>'
        f'<p style="margin:0 0 16px;font-size:15px;font-weight:700;">'
        f'{s["sym"]} &nbsp;'
        f'<span style="color:#6b7280;font-size:13px;font-weight:400;">{s["sec"]}</span>'
        f'&nbsp; {_sig_badge(s["sig"])}</p>'
        + _table(
            _row("Morning Confidence", f'<b>{s["conf"]}%</b>'),
            _row("Evening Confidence", f'{evening_pick["conf"]}%'),
            _row("Entry",      f'Rs&nbsp;{s["en"]}'),
            _row("Target",     f'Rs&nbsp;{s["tg"]} &nbsp;<span style="color:#16a34a">+Rs&nbsp;{gain}</span>'),
            _row("Stop Loss",  f'Rs&nbsp;{s["sl"]} &nbsp;<span style="color:#dc2626">-Rs&nbsp;{risk}</span>'),
            _row("R:R",        f'{s["rr"]}:1'),
            _row("LTP",        f'Rs&nbsp;{s["ltp"]} ({s["chg"]:+.2f}%)'),
            _row("Setup",      s.get("reason", "")),
        )
        + '<p style="margin:16px 0 0;font-size:13px;color:#374151;">'
          '<b>Action:</b> Place a limit order near Rs&nbsp;' + str(s["en"]) +
          '. Set stop-loss at Rs&nbsp;' + str(s["sl"]) + ' immediately after entry.</p>'
    )
    subject = f'⚡ REAL TRADE: {s["sig"]} {s["sym"]} confirmed — {s["conf"]}% conf | NSE Scanner'
    return subject, _wrap(
        "Real Trade Candidate",
        f'{s["sym"]} · {s["sig"]} · confirmed morning + evening',
        body, "real_trade_candidate",
    )
