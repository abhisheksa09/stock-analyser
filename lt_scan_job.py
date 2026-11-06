"""
lt_scan_job.py — Weekly long-term scan, run from GitHub Actions (.github/workflows/lt-scan.yml).

Render's IP is refused by Yahoo quoteSummary and Screener.in, so every scan there ended
with no fundamentals (the picks went stale after 2026-08-04). GitHub runners reach both.
This runs fundamentals.run_lt_scan with Screener as the fundamentals source, saves picks
to DATABASE_URL (the same table the app reads), emails the digest, then refreshes every
past pick's performance against the Nifty (the Long Term tab's track record).

The US Top 50 ("us") is a separate run with its own digest: SEC EDGAR fundamentals,
Alpaca/Yahoo prices, SPY as the benchmark (us_fundamentals.py).

Usage: python lt_scan_job.py [large|mid|small|us] [--dry-run | --perf-only]
       --dry-run   prints the picks and saves/emails nothing (no DATABASE_URL needed).
       --perf-only skips the scan and only refreshes past picks' performance.
Env:   DATABASE_URL (required), RESEND_API_KEY + EMAIL_TO (digest), NEWS_API_KEY +
       ANTHROPIC_API_KEY (optional per-stock sentiment); for "us": SEC_USER_AGENT
       ("Name email@domain" — SEC refuses requests without a contact email),
       ALPACA_API_KEY + ALPACA_API_SECRET (optional, else Yahoo prices)
Exit:  non-zero if no picks were saved or a segment was skipped, so the workflow run
       shows red instead of silently leaving stale picks behind.
"""

import logging
import os
import sys

os.environ.setdefault("LT_FUNDAMENTALS_SOURCE", "screener")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("lt_scan_job")

import fundamentals as lt     # noqa: E402 — must follow the env default above
import email_alerts as email   # noqa: E402


def update_performance() -> int | None:
    """Refresh past picks' performance. Rows updated, or None if it couldn't run."""
    conn = lt._open_lt_db_conn()
    if not conn:
        log.error("Performance update: no DB connection")
        return None
    try:
        return lt.update_lt_performance(conn)
    except Exception as e:
        log.warning("Performance update failed: %s", e)
        return None
    finally:
        conn.close()


def main() -> int:
    args    = [a.strip() for a in sys.argv[1:] if a.strip()]
    dry_run   = "--dry-run" in args
    perf_only = "--perf-only" in args
    segment   = next((a for a in args if not a.startswith("--")), None)
    if segment not in (None, "large", "mid", "small", "us"):
        log.error("segment must be large, mid, small or us (got %r)", segment)
        return 2
    if dry_run:
        os.environ.pop("DATABASE_URL", None)   # run_lt_scan saves only with a DB connection
    elif not os.environ.get("DATABASE_URL"):
        log.error("DATABASE_URL is not set — picks would not be saved")
        return 2
    if perf_only:
        return 0 if update_performance() is not None else 1

    result  = lt.run_lt_scan(segment)
    picks   = result.get("picks", [])
    summary = result.get("summary", {})
    log.info("LT scan summary: %s", summary)

    if dry_run:
        for p in picks:
            log.info(
                "%-6s %-12s score=%5.1f %-10s cmp=%s pe=%s roe=%s de=%s eps_g=%s%% rev_g=%s%% "
                "promoter=%s sector=%s target=%s–%s",
                p["segment"], p["symbol"], p["score"], p["signal"], p["cmp"], p["pe"], p["roe"],
                p["debt_equity"], p["eps_growth"], p["rev_growth"], p["promoter_pct"],
                p["sector"], p["target_low"], p["target_high"],
            )
    elif picks:
        try:
            ok, info = email.send_email(*email.format_weekly_lt_picks(picks))
            log.info("Digest email: %s %s", "sent" if ok else "not sent", info)
        except Exception as e:
            log.warning("Digest email failed: %s", e)

    if not dry_run:
        update_performance()

    failed = {seg: s["error"] for seg, s in summary.items() if s.get("error")}
    if failed:
        log.error("Segments skipped: %s", failed)
        return 1
    if not picks:
        log.error("Scan finished with no picks")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
