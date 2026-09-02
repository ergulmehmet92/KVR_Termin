#!/usr/bin/env python3
"""
watch.py — the always-on half of the KVR appointment watcher.

WHY THIS FILE EXISTS
    kvr_sniper.py is a long-running daemon on a MacBook. A MacBook sleeps when
    the lid closes: `caffeinate -i` only blocks IDLE sleep, and lid-close sleep
    is governed by a kernel property no user-space assertion can override. So
    every closed lid is a blind spot. This script is the same watch, reduced to
    a single stateless poll that a scheduler somewhere else (GitHub Actions, a
    cron on any box) can run forever without a human present.

WHY A SEPARATE SCRIPT AND NOT kvr_sniper.py IN CI
    kvr_sniper.py's only alerting path is `watch`, an infinite loop; its `check`
    command deliberately SUPPRESSES alerts because it resets the dedupe baseline
    to whatever it just saw. Neither is usable one-shot in CI. It also carries
    macOS-only notifiers (osascript, afplay, say), a cookie jar, a file lock and
    a reserve/hold code path that has no business existing on a public runner.
    This file is ~1 endpoint, 1 GET, 0 mutating calls, and no macOS anything —
    small enough to audit line by line, which matters when it runs unattended
    for six weeks and is the only thing standing between the user and a missed
    cancellation.

WHAT IT DOES, EXACTLY ONCE PER INVOCATION
    1. Reads state (a secret GitHub Gist, or a local file) — FAIL-OPEN.
    2. ONE GET to available-calendar. No retries: a retry is just the next
       invocation 90 seconds later, and one GET per run is a promise that is
       easy to verify by reading the code.
    3. Any non-200, any unexpected body shape, any unparseable field is a
       FAILURE. It is never "no appointments available". Reporting an empty
       calendar because the API broke is the single worst thing this can do.
    4. Alerts on any bookable date STRICTLY EARLIER than KVR_TARGET_BEFORE.
    5. On repeated failure, sends a DIFFERENT Telegram push saying the watcher
       has gone blind. A watcher that looks healthy while seeing nothing is
       worse than no watcher.
    6. Writes state back — AFTER the alert has been attempted, never before.

FAIL-OPEN IS THE WHOLE SAFETY ARGUMENT
    Every state-store failure — gist 5xx, revoked token, corrupt blob, two runs
    overlapping — degrades to a DUPLICATE alert, never to silence. Unknown
    state is treated as empty state, and empty state means "everything looks
    new", which means it shouts. That ordering is in the control flow, not in a
    comment.

ENVIRONMENT
    Required
      TG_BOT_TOKEN        Telegram bot token          (secret)
      TG_CHAT_ID          Telegram chat id            (secret)
      KVR_TARGET_BEFORE   YYYY-MM-DD. Alert only on dates strictly earlier.
    Recommended
      KVR_FALLNUMMER      "CASE-123 / Muster" — pasted into the alert so the
                          booking can be finished on a phone. Personal data:
                          keep it a SECRET, never a plaintext repo value.
      GIST_ID             32-hex id of a secret gist holding kvr-state.json.
      GIST_TOKEN          PAT with the `gist` scope, and nothing else.
    Optional
      KVR_STATE_FILE      local state path used when GIST_ID is unset
                          (default: ./state.cloud.json next to this file)
      KVR_FAIL_THRESHOLD  consecutive failures before the blind-watcher push (3)
      KVR_DAILY_HEARTBEAT 1 = one "still alive" push per day (default 1)
      KVR_HEARTBEAT_HOUR  Berlin hour for that push (default 9)
      KVR_RETIRE_AFTER    stop polling after this date (default TARGET_BEFORE)
      KVR_STATE_MIN_WRITE seconds between state writes when nothing changed (600)
      KVR_HORIZON_DAYS    how far ahead to ask, in days (180; hard cap 350)
      KVR_EMPTY_DATE_RANGE 1 = send startDate=&endDate= empty. Do NOT: verified
                          live on 2026-09-01, the server answers HTTP 400
                          invalidStartDate. Kept only as an escape hatch.
      KVR_USER_AGENT      override the User-Agent
      KVR_TIMEOUT         HTTP timeout in seconds (20)

USAGE
    python3 cloud/watch.py              one poll, alerts, writes state
    python3 cloud/watch.py --dry-run    live GET, prints, sends and writes NOTHING
    python3 cloud/watch.py --self-test  offline; exercises the whole pipeline
    python3 cloud/watch.py --heartbeat-check
                                        no KVR call; reads state and shouts if
                                        the watcher has stopped polling. Run
                                        this from the laptop as a dead-man's
                                        switch against the cloud job dying.

EXIT CODES
    0 ok (or nothing to do)   1 poll failed (after attempting to notify)
    2 configuration problem
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date as _date, datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:                                        # pragma: no cover
    print("FATAL: this needs python3.9+ with zoneinfo", file=sys.stderr)
    raise SystemExit(2)

# --------------------------------------------------------------------------- #
# Constants — every one of these is verified against the live API.
# --------------------------------------------------------------------------- #

VERSION = "1.0.0"

CAL_URL = "https://www48.muenchen.de/buergeransicht/api/citizen/available-calendar/"
OFFICE_ID = 10471          # KVR Einbuergerungsbehoerde
SERVICE_ID = 1071907       # Einbuergerung / Urkundenuebergabe
SERVICE_COUNT = 1

# Verified against the page's own hash router. This is what the user taps.
DEEP_LINK = ("https://stadt.muenchen.de/buergerservice/terminvereinbarung.html"
             "#/services/1071907/locations/10471")

TELEGRAM_API = "https://api.telegram.org"
GITHUB_API = "https://api.github.com"
GIST_FILENAME = "kvr-state.json"

# Honest. This is a personal watcher making one request per poll; pretending to
# be Chrome would be a lie told to a municipal server, and the rate here (about
# 0.6% of the advertised 120 req/60s budget) needs no camouflage.
DEFAULT_UA = f"kvr-watch/{VERSION} (personal single-appointment watcher; 1 GET per poll)"

MAX_HORIZON_DAYS = 350     # the API answers HTTP 400 to spans over ~12 months
DEFAULT_TIMEOUT = 20.0
STATE_SCHEMA = 1

# An appointment epoch outside this window means the API changed units or
# semantics (seconds -> milliseconds is the classic). That is a FAILURE.
EPOCH_MIN = 1_600_000_000          # 2020-09-13
EPOCH_MAX = 2_200_000_000          # 2039-09-13

WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

try:
    BERLIN = ZoneInfo("Europe/Berlin")
except Exception as exc:                                   # pragma: no cover
    print(f"FATAL: no Europe/Berlin tz database available ({exc}). "
          f"Install tzdata on this host.", file=sys.stderr)
    raise SystemExit(2)

# --------------------------------------------------------------------------- #
# Secret hygiene
#
# Public-repo Actions logs are world-readable forever, and GitHub's redaction is
# exact-match only — a URL-encoded token (the colon becomes %3A) sails straight
# through it. So nothing this script prints ever goes out unscrubbed, and the
# Telegram token is only ever assembled inside the request call.
# --------------------------------------------------------------------------- #

_SECRETS: list[str] = []


def register_secret(value: str) -> None:
    v = (value or "").strip()
    if len(v) >= 6:
        _SECRETS.append(v)
        _SECRETS.append(urllib.parse.quote(v, safe=""))


def scrub(text: object) -> str:
    out = str(text)
    for secret in _SECRETS:
        if secret and secret in out:
            out = out.replace(secret, "***REDACTED***")
    # Belt and braces: a bot token is <digits>:<35 url-safe chars>. Kill the
    # shape itself, in case one arrives from somewhere we did not register.
    out = re.sub(r"\b\d{6,12}:[A-Za-z0-9_-]{30,}\b", "***REDACTED***", out)
    return out


_QUIET = False


def log(msg: str) -> None:
    if _QUIET:
        return
    stamp = datetime.now(BERLIN).strftime("%Y-%m-%d %H:%M:%S%z")
    print(f"{stamp} {scrub(msg)}", file=sys.stderr, flush=True)


def say(msg: str) -> None:
    """Human-facing stdout, for --dry-run and --self-test."""
    print(scrub(msg), flush=True)


# --------------------------------------------------------------------------- #
# Failure taxonomy. Everything that is not a clean, well-shaped 200 lands here.
# --------------------------------------------------------------------------- #

class PollFailure(Exception):
    def __init__(self, kind: str, detail: str, *, status: int = 0):
        super().__init__(detail)
        self.kind = kind          # network | http | shape | ratelimited | blacklisted
        self.detail = detail
        self.status = status

    def __str__(self) -> str:
        return f"[{self.kind}] {self.detail}"


class ConfigError(Exception):
    pass


# --------------------------------------------------------------------------- #
# Transport. One class, injectable, so --self-test can run the entire pipeline
# with zero packets on the wire.
# --------------------------------------------------------------------------- #

class Transport:
    def __init__(self, timeout: float = DEFAULT_TIMEOUT, user_agent: str = DEFAULT_UA):
        self.timeout = timeout
        self.user_agent = user_agent

    def request(self, method: str, url: str, *, headers: dict | None = None,
                data: bytes | None = None) -> tuple[int, dict, bytes]:
        """Return (status, lowercased headers, body). Never raises for 4xx/5xx.

        Raises PollFailure('network') for anything below HTTP — and the message
        is built from the exception TYPE plus its reason, never from a repr that
        might carry the full URL (which, for Telegram, carries the token).
        """
        hdrs = {"User-Agent": self.user_agent, "Accept": "application/json"}
        hdrs.update(headers or {})
        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
        except urllib.error.HTTPError as exc:
            try:
                body = exc.read()
            except Exception:
                body = b""
            return exc.code, {k.lower(): v for k, v in (exc.headers or {}).items()}, body
        except urllib.error.URLError as exc:
            raise PollFailure("network", f"{type(exc).__name__}: {exc.reason}") from None
        except Exception as exc:
            raise PollFailure("network", f"{type(exc).__name__}: {exc}") from None


# --------------------------------------------------------------------------- #
# Time helpers. Locale-proof on purpose: a CI runner's locale is not ours to
# assume, and "Di 06 Okt" in a German locale would still be correct but the
# test asserting "Tue" would not be.
# --------------------------------------------------------------------------- #

def berlin_now() -> datetime:
    return datetime.now(BERLIN)


def parse_iso_date(text: object) -> _date:
    if not isinstance(text, str) or len(text) != 10:
        raise ValueError(f"not a YYYY-MM-DD date: {text!r}")
    return _date(int(text[0:4]), int(text[5:7]), int(text[8:10]))


def pretty_date(iso: str) -> str:
    d = parse_iso_date(iso)
    return f"{WEEKDAYS[d.weekday()]} {d.day:02d} {MONTHS[d.month - 1]} {d.year}"


def slot_time(epoch: int) -> str:
    """Epoch seconds -> HH:MM in Europe/Berlin.

    This is why zoneinfo is non-negotiable: 2026-10-06 is CEST (+02:00) and
    2026-11-03 is CET (+01:00). Fixed-offset arithmetic is self-consistent
    inside one DST regime and silently one hour wrong across 2026-10-25 — a
    wrong time on the one message that matters.
    """
    return datetime.fromtimestamp(int(epoch), BERLIN).strftime("%H:%M")


def days_between(earlier_iso: str, later_iso: str) -> int:
    return (parse_iso_date(later_iso) - parse_iso_date(earlier_iso)).days


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

class Config:
    def __init__(self, env: dict):
        self.tg_token = (env.get("TG_BOT_TOKEN") or "").strip()
        self.tg_chat = (env.get("TG_CHAT_ID") or "").strip()
        self.target_before = (env.get("KVR_TARGET_BEFORE") or "").strip() or None
        self.fallnummer = (env.get("KVR_FALLNUMMER") or "").strip()
        self.gist_id = (env.get("GIST_ID") or "").strip()
        self.gist_token = (env.get("GIST_TOKEN") or "").strip()
        here = os.path.dirname(os.path.abspath(__file__))
        self.state_file = (env.get("KVR_STATE_FILE") or "").strip() or \
            os.path.join(here, "state.cloud.json")
        self.fail_threshold = _int(env.get("KVR_FAIL_THRESHOLD"), 3, lo=1, hi=50)
        self.daily_heartbeat = (env.get("KVR_DAILY_HEARTBEAT") or "1").strip() not in ("0", "false", "no", "")
        self.heartbeat_hour = _int(env.get("KVR_HEARTBEAT_HOUR"), 9, lo=0, hi=23)
        self.retire_after = (env.get("KVR_RETIRE_AFTER") or "").strip() or self.target_before
        self.min_write_interval = _int(env.get("KVR_STATE_MIN_WRITE"), 600, lo=0, hi=86400)
        self.empty_date_range = (env.get("KVR_EMPTY_DATE_RANGE") or "0").strip() in ("1", "true", "yes")
        self.horizon_days = _int(env.get("KVR_HORIZON_DAYS"), 180, lo=1, hi=MAX_HORIZON_DAYS)
        self.user_agent = (env.get("KVR_USER_AGENT") or "").strip() or DEFAULT_UA
        self.timeout = float(_int(env.get("KVR_TIMEOUT"), int(DEFAULT_TIMEOUT), lo=5, hi=120))

    def validate(self, *, need_telegram: bool) -> None:
        if self.target_before is not None:
            try:
                parse_iso_date(self.target_before)
            except ValueError:
                raise ConfigError("KVR_TARGET_BEFORE must be YYYY-MM-DD "
                                  f"(got {self.target_before!r})")
        if self.retire_after:
            try:
                parse_iso_date(self.retire_after)
            except ValueError:
                raise ConfigError("KVR_RETIRE_AFTER must be YYYY-MM-DD")
        if need_telegram:
            if not self.tg_token:
                raise ConfigError("TG_BOT_TOKEN is not set — there is no way to alert you")
            if not self.tg_chat:
                raise ConfigError("TG_CHAT_ID is not set — there is no way to alert you")
        if self.gist_id and not self.gist_token:
            raise ConfigError("GIST_ID is set but GIST_TOKEN is not")

    def uses_gist(self) -> bool:
        return bool(self.gist_id and self.gist_token)


def _int(raw: object, default: int, *, lo: int, hi: int) -> int:
    try:
        val = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, val))


# --------------------------------------------------------------------------- #
# Notifier
# --------------------------------------------------------------------------- #

class Notifier:
    """Telegram push. POST, form-encoded, token assembled at call time only."""

    def __init__(self, transport: Transport, token: str, chat_id: str, *,
                 dry_run: bool = False):
        self.transport = transport
        self._token = token
        self._chat = chat_id
        self.dry_run = dry_run
        self.sent: list[str] = []          # what --dry-run / --self-test inspect

    def send(self, text: str) -> tuple[bool, str]:
        self.sent.append(text)
        if self.dry_run:
            say("\n--- would send to Telegram ---\n" + text + "\n--- end ---")
            return True, "dry-run"
        url = f"{TELEGRAM_API}/bot{self._token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": self._chat,
            "text": text,
            "disable_notification": "false",   # a silent push defeats the point
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        try:
            status, _hdrs, body = self.transport.request(
                "POST", url, data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"})
        except PollFailure as exc:
            # exc carries the exception type and reason, never the URL.
            return False, f"telegram unreachable: {exc.detail}"
        except Exception as exc:
            return False, f"telegram error: {type(exc).__name__}"
        try:
            parsed = json.loads(body.decode("utf-8"))
        except Exception:
            return False, f"telegram HTTP {status}, unparseable reply"
        if status == 200 and parsed.get("ok"):
            return True, "sent"
        return False, f"telegram HTTP {status}: {scrub(parsed.get('description') or '')}"


# --------------------------------------------------------------------------- #
# State stores. Both fail open: an unreadable store yields {} and a reason.
# --------------------------------------------------------------------------- #

class FileStore:
    def __init__(self, path: str):
        self.path = path

    def describe(self) -> str:
        return f"file {self.path}"

    def load(self) -> tuple[dict, str]:
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if not isinstance(data, dict):
                return {}, "state file was not a JSON object"
            return data, ""
        except FileNotFoundError:
            return {}, "no state file yet (first run)"
        except Exception as exc:
            return {}, f"unreadable state file: {type(exc).__name__}: {exc}"

    def save(self, state: dict) -> tuple[bool, str]:
        try:
            directory = os.path.dirname(os.path.abspath(self.path)) or "."
            os.makedirs(directory, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=directory, prefix=".state-", suffix=".json")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(state, fh, indent=2, sort_keys=True)
            os.replace(tmp, self.path)          # atomic: never a half-written file
            return True, "saved"
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"


class GistStore:
    """A secret gist as the state store.

    Chosen over actions/cache because cache contents cannot be read from
    outside a runner — GitHub publishes list and delete endpoints and no
    download. That would make it impossible to answer "is my watcher still
    alive?" without scraping job logs, and impossible for the laptop watcher to
    share dedupe state with the cloud one. The gist is readable by both, which
    is what makes --heartbeat-check possible at all.
    """

    def __init__(self, transport: Transport, gist_id: str, token: str):
        self.transport = transport
        self.gist_id = gist_id
        self._token = token

    def describe(self) -> str:
        return f"gist {self.gist_id[:7]}..."

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def load(self) -> tuple[dict, str]:
        url = f"{GITHUB_API}/gists/{urllib.parse.quote(self.gist_id, safe='')}"
        try:
            status, _h, body = self.transport.request("GET", url, headers=self._headers())
        except PollFailure as exc:
            return {}, f"gist unreachable: {exc.detail}"
        except Exception as exc:
            return {}, f"gist error: {type(exc).__name__}"
        if status != 200:
            return {}, f"gist HTTP {status}"
        try:
            gist = json.loads(body.decode("utf-8"))
            entry = (gist.get("files") or {}).get(GIST_FILENAME)
            if not isinstance(entry, dict):
                return {}, f"gist has no file named {GIST_FILENAME}"
            content = entry.get("content") or ""
            if entry.get("truncated"):
                # 1 MB API cap. Current state is ~1.5 kB so this never fires,
                # but the fallback costs three lines.
                raw = entry.get("raw_url")
                if raw:
                    st2, _h2, b2 = self.transport.request("GET", raw, headers=self._headers())
                    if st2 == 200:
                        content = b2.decode("utf-8")
            if not content.strip():
                return {}, "gist state file was empty"
            data = json.loads(content)
            if not isinstance(data, dict):
                return {}, "gist state was not a JSON object"
            return data, ""
        except Exception as exc:
            return {}, f"gist state unparseable: {type(exc).__name__}"

    def save(self, state: dict) -> tuple[bool, str]:
        url = f"{GITHUB_API}/gists/{urllib.parse.quote(self.gist_id, safe='')}"
        payload = json.dumps(
            {"files": {GIST_FILENAME: {"content": json.dumps(state, indent=2, sort_keys=True)}}}
        ).encode("utf-8")
        try:
            status, _h, body = self.transport.request(
                "PATCH", url, headers=dict(self._headers(),
                                           **{"Content-Type": "application/json"}),
                data=payload)
        except PollFailure as exc:
            return False, f"gist unreachable: {exc.detail}"
        except Exception as exc:
            return False, f"gist error: {type(exc).__name__}"
        if status in (200, 201):
            return True, "saved"
        return False, f"gist HTTP {status}: {scrub(body[:200].decode('utf-8', 'replace'))}"


def build_store(cfg: Config, transport: Transport):
    if cfg.uses_gist():
        return GistStore(transport, cfg.gist_id, cfg.gist_token)
    return FileStore(cfg.state_file)


def blank_state() -> dict:
    return {
        "schema": STATE_SCHEMA,
        "poll_count": 0,
        "success_count": 0,
        "failure_count": 0,
        "consecutive_failures": 0,
        "last_poll_iso": "",
        "last_poll_epoch": 0,
        "last_success_iso": "",
        "last_success_epoch": 0,
        "last_error": "",
        "last_error_iso": "",
        "known_dates": [],
        "earliest_date": "",
        "alerted": {},
        "alert_count": 0,
        "broken_alert_at_failures": 0,
        "broken_alert_iso": "",
        "empty_calendar_streak": 0,
        "last_heartbeat_date": "",
        "last_write_epoch": 0,
    }


def normalise_state(raw: dict) -> dict:
    """Merge whatever we read over a known-good skeleton.

    A state file from an older schema, or one a human hand-edited badly, must
    not crash the poll — the poll is the point, the state is the convenience.
    """
    state = blank_state()
    if isinstance(raw, dict):
        for key, default in blank_state().items():
            if key in raw and isinstance(raw[key], type(default)):
                state[key] = raw[key]
        # ints stored as floats etc.
        for key in ("poll_count", "success_count", "failure_count",
                    "consecutive_failures", "alert_count", "broken_alert_at_failures",
                    "empty_calendar_streak", "last_poll_epoch", "last_success_epoch",
                    "last_write_epoch"):
            try:
                state[key] = int(raw.get(key, state[key]))
            except (TypeError, ValueError):
                pass
    state["known_dates"] = sorted({d for d in state["known_dates"] if isinstance(d, str)})
    if not isinstance(state["alerted"], dict):
        state["alerted"] = {}
    return state


# --------------------------------------------------------------------------- #
# The one API call
# --------------------------------------------------------------------------- #

def calendar_url(cfg: Config) -> str:
    """The discovery call. Param order matches the browser's own request.

    startDate and endDate MUST carry real dates. A hand-off note claimed the
    empty form (startDate=&endDate=) returns every bookable date; tested live
    on 2026-09-01 it does not — the server answers HTTP 400 with errorCode
    invalidStartDate, "startDate is required and must be a valid date". The
    dates are computed in Europe/Berlin, not in the runner's UTC, so a job
    starting at 23:30 UTC does not ask about yesterday.

    slotsStartDate / slotsEndDate are deliberately NEVER sent: they truncate
    availableDays to a month-scoped window AND snap forward silently to the
    first bookable day, which would make this poll lie by omission.
    """
    if cfg.empty_date_range:
        start = end = ""
    else:
        today = berlin_now().date()
        start = today.isoformat()
        end = (today + timedelta(days=cfg.horizon_days)).isoformat()
    query = urllib.parse.urlencode([
        ("startDate", start),
        ("endDate", end),
        ("officeIds", str(OFFICE_ID)),
        ("serviceIds", str(SERVICE_ID)),
        ("serviceCounts", str(SERVICE_COUNT)),
    ])
    return f"{CAL_URL}?{query}"


def fetch_calendar(transport: Transport, cfg: Config) -> tuple[dict, dict]:
    """THE single GET. Returns (payload, ratelimit headers). Raises PollFailure.

    Deliberately no retry loop. One invocation is one request; the next
    invocation ninety seconds later is the retry. That keeps the politeness
    promise checkable by reading twenty lines instead of reasoning about a
    backoff state machine.
    """
    url = calendar_url(cfg)
    status, headers, body = transport.request("GET", url)

    rl = {k: headers[k] for k in headers if k.startswith("x-ratelimit")}

    parsed: object = None
    if body:
        try:
            parsed = json.loads(body.decode("utf-8"))
        except Exception:
            parsed = None

    if status != 200:
        # Two error shapes exist and they must be branched on errorCode, never
        # on the human text: rateLimitExceeded and ipBlacklisted carry
        # IDENTICAL messages, and confusing them means either hammering a
        # server that has already banned us, or giving up on a soft throttle.
        code = ""
        message = ""
        if isinstance(parsed, dict):
            errors = parsed.get("errors")
            if isinstance(errors, list) and errors and isinstance(errors[0], dict):
                code = str(errors[0].get("errorCode") or "")
                message = str(errors[0].get("errorMessage") or "")
            elif "error" in parsed and "status" in parsed:
                message = str(parsed.get("error") or "")     # bare 400 shape
        if code == "ipBlacklisted":
            raise PollFailure("blacklisted",
                              "the KVR API returned ipBlacklisted — this IP is blocked",
                              status=status)
        if code == "rateLimitExceeded" or status == 429:
            raise PollFailure("ratelimited",
                              f"soft throttle ({code or status}) {message}".strip(),
                              status=status)
        raise PollFailure("http", f"HTTP {status} {code} {message}".strip(), status=status)

    if parsed is None:
        raise PollFailure("shape", f"HTTP 200 with an unparseable body "
                                   f"({len(body)} bytes, starts {body[:60]!r})")
    if not isinstance(parsed, dict) or "availableDays" not in parsed:
        raise PollFailure("shape", f"HTTP 200 but no availableDays key "
                                   f"(keys: {sorted(parsed)[:8] if isinstance(parsed, dict) else type(parsed).__name__})")
    return parsed, rl


def parse_calendar(payload: dict) -> dict[str, list[int]]:
    """availableDays -> {iso date: [epoch, ...]}.

    STRICT on purpose. Every listed day is genuinely bookable, including days
    whose "offices" is [] — that empty array only means the day lies outside
    the server's one-day slot window, not that the day is unavailable. But a
    field that is present and the WRONG SHAPE means the contract moved under
    us, and the right response to that is a loud failure, not a quiet
    'no appointments'.
    """
    days = payload.get("availableDays")
    if not isinstance(days, list):
        raise PollFailure("shape", f"availableDays is {type(days).__name__}, expected list")

    out: dict[str, list[int]] = {}
    for entry in days:
        if not isinstance(entry, dict):
            raise PollFailure("shape", f"availableDays entry is {type(entry).__name__}, expected object")
        iso = entry.get("date")
        try:
            parse_iso_date(iso)
        except (ValueError, TypeError):
            raise PollFailure("shape", f"availableDays entry has a bad date: {iso!r}") from None

        offices = entry.get("offices")
        if offices is None:
            offices = []
        if not isinstance(offices, list):
            raise PollFailure("shape", f"offices for {iso} is {type(offices).__name__}, expected list")

        slots: list[int] = []
        for office in offices:
            if not isinstance(office, dict):
                raise PollFailure("shape", f"office entry for {iso} is not an object")
            oid = office.get("officeId")
            if oid is not None and str(oid) != str(OFFICE_ID):
                continue
            appts = office.get("appointments")
            if appts is None:
                appts = []
            if not isinstance(appts, list):
                raise PollFailure("shape", f"appointments for {iso} is {type(appts).__name__}")
            for stamp in appts:
                if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
                    raise PollFailure("shape", f"appointment stamp for {iso} is {stamp!r}")
                stamp = int(stamp)
                if not (EPOCH_MIN <= stamp <= EPOCH_MAX):
                    raise PollFailure(
                        "shape",
                        f"appointment stamp {stamp} for {iso} is outside plausible epoch "
                        f"seconds — the API may have switched units")
                slots.append(stamp)
        out[str(iso)] = sorted(set(slots))
    return out


# --------------------------------------------------------------------------- #
# Messages
# --------------------------------------------------------------------------- #

def format_alert(new_dates: list[str], calendar: dict[str, list[int]],
                 cfg: Config) -> str:
    lines = ["KVR: EARLIER APPOINTMENT AVAILABLE", ""]
    for iso in new_dates:
        head = pretty_date(iso)
        if cfg.target_before:
            gap = days_between(iso, cfg.target_before)
            head += f"  ({gap} day{'s' if gap != 1 else ''} earlier than your {cfg.target_before})"
        lines.append(head)
        slots = calendar.get(iso) or []
        if slots:
            shown = ", ".join(slot_time(s) for s in slots[:24])
            more = f" (+{len(slots) - 24} more)" if len(slots) > 24 else ""
            lines.append(f"  Times (Europe/Berlin): {shown}{more}")
        else:
            # The server only returns slot epochs for the FIRST bookable day.
            # Saying "no times" here would be a lie; say what is actually true.
            lines.append("  Times: not included in this response — open the link to see them.")
        lines.append("")
    lines.append("Book it now. Cancellations are taken within minutes:")
    lines.append(DEEP_LINK)
    if cfg.fallnummer:
        lines.append("")
        lines.append(f"Fallnummer und Sachbearbeiter: {cfg.fallnummer}")
    lines.append("")
    lines.append("Cancel your old appointment only AFTER the new one is confirmed.")
    return "\n".join(lines)


def format_broken(state: dict, failure: str, cfg: Config, *, blacklisted: bool) -> str:
    if blacklisted:
        head = "KVR WATCHER BLOCKED — the API has blacklisted this IP"
        advice = ("The cloud watcher is blind until the block lifts. Your laptop "
                  "watcher, on a different IP, is not affected — start it if it is off.")
    else:
        head = "KVR WATCHER IS BLIND — it is not seeing the calendar"
        advice = ("Check the scheduled job's most recent run log. Until this is "
                  "fixed you are NOT being watched.")
    return "\n".join([
        head,
        "",
        f"Consecutive failed polls: {state.get('consecutive_failures', 0)}",
        f"Last error: {failure}",
        f"Last good poll: {state.get('last_success_iso') or 'never'}",
        "",
        advice,
        "",
        "This message means the watcher is telling you it broke. "
        "Silence would have meant the same thing without telling you.",
    ])


def format_recovered(state: dict) -> str:
    return "\n".join([
        "KVR watcher recovered — it can see the calendar again.",
        "",
        f"Earliest bookable: {state.get('earliest_date') or 'unknown'}",
        f"Bookable days visible: {len(state.get('known_dates') or [])}",
    ])


def format_heartbeat(state: dict, cfg: Config) -> str:
    earliest = state.get("earliest_date") or "unknown"
    verdict = "no improvement yet"
    if cfg.target_before and earliest != "unknown" and earliest < cfg.target_before:
        verdict = "EARLIER THAN YOUR BOOKING — see the alert above"
    return "\n".join([
        f"KVR watcher alive — {berlin_now().strftime('%a %d %b, %H:%M')} Berlin",
        "",
        f"Earliest bookable: {earliest} ({verdict})",
        f"Your booking: {cfg.target_before or 'not set'}",
        f"Bookable days visible: {len(state.get('known_dates') or [])}",
        f"Polls so far: {state.get('poll_count', 0)} "
        f"({state.get('failure_count', 0)} failed)",
        "",
        "If this daily message stops arriving, the watcher has died.",
    ])


def format_stale(state: dict, age_minutes: float, limit_minutes: int) -> str:
    return "\n".join([
        "KVR WATCHER HAS STOPPED — no poll in "
        f"{int(age_minutes)} minutes (limit {limit_minutes}).",
        "",
        f"Last poll: {state.get('last_poll_iso') or 'never'}",
        f"Last success: {state.get('last_success_iso') or 'never'}",
        "",
        "The scheduled job is not running. Check that the workflow is enabled "
        "and that its last run did not fail.",
    ])


# --------------------------------------------------------------------------- #
# The poll
# --------------------------------------------------------------------------- #

def run_poll(cfg: Config, store, transport: Transport, notifier: Notifier, *,
             dry_run: bool = False) -> int:
    now = time.time()
    raw, load_note = store.load()
    if load_note:
        # FAIL-OPEN. Unknown state is empty state, and empty state re-alerts.
        # A duplicate push is an annoyance; a suppressed push is the failure
        # this whole project exists to prevent.
        log(f"state: {load_note} — continuing with empty state (may re-alert)")
    state = normalise_state(raw)
    baseline = json.loads(json.dumps(state))      # deep copy, for change detection

    prev_known = set(state.get("known_dates") or [])
    state["poll_count"] = int(state.get("poll_count", 0)) + 1
    state["last_poll_epoch"] = int(now)
    state["last_poll_iso"] = datetime.fromtimestamp(now, BERLIN).isoformat(timespec="seconds")

    exit_code = 0
    try:
        payload, rl = fetch_calendar(transport, cfg)
        calendar = parse_calendar(payload)
    except PollFailure as failure:
        exit_code = handle_failure(cfg, state, notifier, failure, dry_run=dry_run)
    else:
        exit_code = handle_success(cfg, state, notifier, calendar, prev_known, rl,
                                   dry_run=dry_run)

    # PERSIST LAST. The alert has already been attempted by this point, so a
    # store that is down costs a duplicate next run, never a missed alert.
    if dry_run:
        log("dry-run: state not written")
        return exit_code

    # Write when something that MATTERS changed, or when the last write is old
    # enough that last_poll_iso would look stale to the dead-man's switch. The
    # bare counters (poll_count) change every run and are not worth a write.
    material = any(state.get(k) != baseline.get(k) for k in (
        "known_dates", "alerted", "alert_count", "consecutive_failures",
        "earliest_date", "empty_calendar_streak", "last_heartbeat_date",
        "broken_alert_at_failures"))
    stale_write = (now - float(state.get("last_write_epoch") or 0)) >= cfg.min_write_interval
    if material or stale_write:
        state["last_write_epoch"] = int(now)
        ok, detail = store.save(state)
        if not ok:
            log(f"state write FAILED ({detail}) — next run may re-alert")
        else:
            log(f"state written to {store.describe()}")
    else:
        log("state unchanged and recently written — skipping write")
    return exit_code


def handle_success(cfg: Config, state: dict, notifier: Notifier,
                   calendar: dict[str, list[int]], prev_known: set,
                   rl: dict, *, dry_run: bool) -> int:
    now = time.time()
    dates = sorted(calendar)
    state["success_count"] = int(state.get("success_count", 0)) + 1
    state["last_success_epoch"] = int(now)
    state["last_success_iso"] = datetime.fromtimestamp(now, BERLIN).isoformat(timespec="seconds")
    state["last_error"] = ""
    was_failing = int(state.get("consecutive_failures", 0))
    state["consecutive_failures"] = 0
    state["known_dates"] = dates
    state["earliest_date"] = dates[0] if dates else ""

    log(f"ok: {len(dates)} bookable day(s), earliest {dates[0] if dates else 'NONE'}"
        + (f", ratelimit remaining {rl.get('x-ratelimit-remaining')}" if rl else ""))

    # An entirely empty calendar is a legal response, but for THIS office it is
    # also what a changed office/service id looks like. Treat it as valid, count
    # it, and escalate if it persists — never silently call it "nothing found".
    if not dates:
        state["empty_calendar_streak"] = int(state.get("empty_calendar_streak", 0)) + 1
        log(f"WARNING: the calendar is completely empty "
            f"({state['empty_calendar_streak']} poll(s) in a row)")
        if state["empty_calendar_streak"] == cfg.fail_threshold:
            text = ("KVR WATCHER: the calendar has been COMPLETELY EMPTY for "
                    f"{state['empty_calendar_streak']} polls.\n\n"
                    "That is possible but unusual for this office. It also looks "
                    "exactly like a changed office/service id. Verify by hand:\n"
                    + DEEP_LINK)
            deliver(notifier, text, "empty-calendar warning")
    else:
        state["empty_calendar_streak"] = 0

    # Recovery notice, so a blind period has a visible end as well as a start.
    if was_failing and state.get("broken_alert_at_failures"):
        deliver(notifier, format_recovered(state), "recovery notice")
        state["broken_alert_at_failures"] = 0
        state["broken_alert_iso"] = ""

    qualifying = [d for d in dates if cfg.target_before is None or d < cfg.target_before]
    alerted = state.get("alerted") or {}

    # Alert if never alerted, OR if the date vanished and has come back — a
    # cancellation that reappears after being taken is a genuinely new chance,
    # and staying quiet about it would be the same silence we are avoiding.
    new_dates = [d for d in qualifying
                 if d not in alerted or d not in prev_known]

    if not qualifying:
        log(f"nothing earlier than {cfg.target_before} — earliest is "
            f"{dates[0] if dates else 'none'}")
    elif not new_dates:
        log(f"qualifying dates {qualifying} already alerted — staying quiet")
    else:
        text = format_alert(new_dates, calendar, cfg)
        ok = deliver(notifier, text, f"EARLIER APPOINTMENT {new_dates}")
        if ok or dry_run:
            stamp = datetime.fromtimestamp(now, BERLIN).isoformat(timespec="seconds")
            for iso in new_dates:
                alerted[iso] = stamp
            state["alerted"] = alerted
            state["alert_count"] = int(state.get("alert_count", 0)) + 1
        else:
            # Do NOT mark as alerted: next run must try again.
            log("alert not delivered — leaving it unmarked so the next poll retries")
            return 1

    if cfg.daily_heartbeat and not dry_run:
        today = datetime.fromtimestamp(now, BERLIN).date().isoformat()
        hour = datetime.fromtimestamp(now, BERLIN).hour
        if state.get("last_heartbeat_date") != today and hour >= cfg.heartbeat_hour:
            if deliver(notifier, format_heartbeat(state, cfg), "daily heartbeat"):
                state["last_heartbeat_date"] = today
    return 0


def handle_failure(cfg: Config, state: dict, notifier: Notifier,
                   failure: PollFailure, *, dry_run: bool) -> int:
    now = time.time()
    state["failure_count"] = int(state.get("failure_count", 0)) + 1
    state["consecutive_failures"] = int(state.get("consecutive_failures", 0)) + 1
    state["last_error"] = str(failure)
    state["last_error_iso"] = datetime.fromtimestamp(now, BERLIN).isoformat(timespec="seconds")
    fails = state["consecutive_failures"]
    log(f"POLL FAILED ({failure}) — consecutive failures: {fails}")

    blacklisted = failure.kind == "blacklisted"
    already = int(state.get("broken_alert_at_failures", 0))

    # A blacklist is not a transient blip: it means every subsequent poll is
    # also blind, so it is worth a push on the first occurrence. Everything else
    # waits for the threshold, then re-escalates on each doubling so a long
    # outage nags without spamming.
    if blacklisted:
        should = already == 0
    else:
        should = fails >= cfg.fail_threshold and (already == 0 or fails >= already * 2)

    if should:
        if deliver(notifier, format_broken(state, str(failure), cfg,
                                           blacklisted=blacklisted), "BLIND WATCHER"):
            state["broken_alert_at_failures"] = fails
            state["broken_alert_iso"] = state["last_error_iso"]
    else:
        log(f"not alerting yet (threshold {cfg.fail_threshold}, "
            f"last blind-alert at {already} failures)")
    return 1


def deliver(notifier: Notifier, text: str, label: str) -> bool:
    ok, detail = notifier.send(text)
    log(f"telegram {label}: {'sent' if ok else 'FAILED — ' + detail}")
    return ok


# --------------------------------------------------------------------------- #
# Dead-man's switch
# --------------------------------------------------------------------------- #

def run_heartbeat_check(cfg: Config, store, notifier: Notifier,
                        limit_minutes: int) -> int:
    """Reads state, makes NO KVR request, shouts if polling has stopped.

    This is the piece nothing else can provide: if the scheduled job stops
    running entirely — workflow disabled, token expired, repo archived — the
    job itself cannot tell you, because it is not running. Point this at the
    same gist from anywhere that still works.
    """
    raw, note = store.load()
    if note:
        text = ("KVR WATCHER: its state store is unreadable, so I cannot tell "
                f"whether it is still polling.\n\nReason: {note}")
        deliver(notifier, text, "state unreadable")
        return 1
    state = normalise_state(raw)
    last = float(state.get("last_poll_epoch") or 0)
    if last <= 0:
        deliver(notifier, "KVR WATCHER: state exists but records no poll at all.",
                "never polled")
        return 1
    age_min = (time.time() - last) / 60.0
    if age_min > limit_minutes:
        deliver(notifier, format_stale(state, age_min, limit_minutes), "STALE watcher")
        return 1
    say(f"watcher healthy: last poll {int(age_min)} min ago "
        f"({state.get('last_poll_iso')}), earliest {state.get('earliest_date') or 'none'}")
    return 0


# --------------------------------------------------------------------------- #
# Self-test — offline, exercises the whole pipeline
# --------------------------------------------------------------------------- #

# Fixed epochs, computed once against the real tz database. They are the DST
# assertion: 2026-10-06 is CEST (+02:00) and 2026-11-03 is CET (+01:00), and a
# naive fixed-offset implementation gets exactly one of these wrong.
T_OCT06_0805 = 1791266700
T_OCT06_0920 = 1791271200
T_NOV03_0900 = 1793692800


class FakeTransport(Transport):
    def __init__(self):
        super().__init__()
        self.calls: list[tuple[str, str]] = []
        self.kvr_status = 200
        self.kvr_body = b"{}"
        self.telegram_ok = True

    def request(self, method, url, *, headers=None, data=None):
        self.calls.append((method, url.split("?")[0]))
        if url.startswith(CAL_URL):
            return self.kvr_status, {"x-ratelimit-remaining": "118"}, self.kvr_body
        if url.startswith(TELEGRAM_API):
            if self.telegram_ok:
                return 200, {}, b'{"ok":true,"result":{}}'
            return 403, {}, b'{"ok":false,"description":"blocked"}'
        raise AssertionError(f"self-test made an unexpected request to {url}")


class Results:
    def __init__(self):
        self.passed = 0
        self.failed = 0

    def check(self, label: str, condition: bool, detail: str = "") -> None:
        if condition:
            self.passed += 1
            say(f"  PASS  {label}")
        else:
            self.failed += 1
            say(f"  FAIL  {label}" + (f"\n        {detail}" if detail else ""))


def _wipe(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)


def _payload(days: list[tuple[str, list[int]]]) -> bytes:
    return json.dumps({
        "availableDays": [
            {"date": iso, "offices": ([{"officeId": OFFICE_ID, "appointments": slots}]
                                      if slots else [])}
            for iso, slots in days
        ]
    }).encode("utf-8")


def run_self_test() -> int:
    res = Results()
    tmpdir = tempfile.mkdtemp(prefix="kvr-selftest-")
    fake_token = "1234567890:AAFAKEfakeFAKEfakeFAKEfakeFAKEfake123"
    register_secret(fake_token)

    env = {
        "TG_BOT_TOKEN": fake_token,
        "TG_CHAT_ID": "111111111",
        "KVR_TARGET_BEFORE": "2026-10-13",
        "KVR_FALLNUMMER": "CASE-123 / Muster",
        "KVR_STATE_FILE": os.path.join(tmpdir, "state.json"),
        "KVR_FAIL_THRESHOLD": "3",
        "KVR_DAILY_HEARTBEAT": "0",
        "KVR_STATE_MIN_WRITE": "0",
        "KVR_RETIRE_AFTER": "2099-01-01",
    }
    cfg = Config(env)
    cfg.validate(need_telegram=True)

    def fresh():
        t = FakeTransport()
        store = FileStore(cfg.state_file)
        n = Notifier(t, cfg.tg_token, cfg.tg_chat)
        return t, store, n

    say("\n[1] timezone conversion across the 2026-10-25 DST boundary")
    res.check("2026-10-06 epoch renders 08:05 CEST", slot_time(T_OCT06_0805) == "08:05",
              f"got {slot_time(T_OCT06_0805)}")
    res.check("2026-11-03 epoch renders 09:00 CET", slot_time(T_NOV03_0900) == "09:00",
              f"got {slot_time(T_NOV03_0900)}")
    res.check("weekday naming is locale-proof",
              pretty_date("2026-10-06") == "Tue 06 Oct 2026", pretty_date("2026-10-06"))

    say("\n[2] a synthetic earlier date fires a complete alert")
    t, store, n = fresh()
    t.kvr_body = _payload([
        ("2026-10-06", [T_OCT06_0805, T_OCT06_0920]),
        ("2026-10-13", []),
        ("2026-11-03", [T_NOV03_0900]),
    ])
    code = run_poll(cfg, store, t, n, dry_run=False)
    msg = n.sent[-1] if n.sent else ""
    res.check("exit code 0", code == 0, f"got {code}")
    res.check("exactly one GET to the calendar",
              sum(1 for m, u in t.calls if u == CAL_URL) == 1, str(t.calls))
    res.check("one Telegram push", len(n.sent) == 1, f"{len(n.sent)}")
    res.check("carries the date", "Tue 06 Oct 2026" in msg)
    res.check("carries the day gap", "7 days earlier" in msg, msg[:200])
    res.check("carries Berlin slot times", "08:05, 09:20" in msg)
    res.check("carries the deep link", DEEP_LINK in msg)
    res.check("carries the Fallnummer", "CASE-123 / Muster" in msg)
    # The target date is allowed to appear as "earlier than your 2026-10-13";
    # what must never appear is 2026-10-13 rendered as an offered date.
    res.check("does NOT offer the target date itself",
              "Tue 13 Oct 2026" not in msg, msg)
    res.check("does NOT offer later dates", "Nov 2026" not in msg, msg)

    say("\n[3] the same calendar again stays silent (no spam)")
    t2, store2, n2 = fresh()
    t2.kvr_body = t.kvr_body
    code = run_poll(cfg, store2, t2, n2, dry_run=False)
    res.check("exit code 0", code == 0, f"got {code}")
    res.check("no second push", len(n2.sent) == 0, str(n2.sent))

    say("\n[4] a date that vanished and came back alerts again")
    t3, store3, n3 = fresh()
    t3.kvr_body = _payload([("2026-10-13", [])])          # 10-06 disappeared
    run_poll(cfg, store3, t3, n3, dry_run=False)
    res.check("no push while it is gone", len(n3.sent) == 0, str(n3.sent))
    t4, store4, n4 = fresh()
    t4.kvr_body = _payload([("2026-10-06", [T_OCT06_0805]), ("2026-10-13", [])])
    run_poll(cfg, store4, t4, n4, dry_run=False)
    res.check("re-alerts when it reappears", len(n4.sent) == 1, str(n4.sent))

    say("\n[5] a broken API is a FAILURE, never 'no appointments'")
    for label, status, body in [
        ("HTTP 500", 500, b"<html>gateway</html>"),
        ("HTTP 200 with HTML", 200, b"<html>maintenance</html>"),
        ("HTTP 200 without availableDays", 200, b'{"foo":1}'),
        ("availableDays is a string", 200, b'{"availableDays":"soon"}'),
        ("a day with a bad date", 200, b'{"availableDays":[{"date":"tomorrow"}]}'),
        ("appointments in milliseconds", 200,
         b'{"availableDays":[{"date":"2026-10-06","offices":[{"appointments":[1791266700000]}]}]}'),
    ]:
        _wipe(cfg.state_file)
        tt, ss, nn = fresh()
        tt.kvr_status, tt.kvr_body = status, body
        code = run_poll(cfg, ss, tt, nn, dry_run=False)
        res.check(f"{label} -> non-zero exit", code == 1, f"got {code}")
        res.check(f"{label} -> no 'all clear' push", len(nn.sent) == 0, str(nn.sent))

    say("\n[6] repeated failure sends a DISTINCT blind-watcher push, once")
    _wipe(cfg.state_file)
    store6 = FileStore(cfg.state_file)
    pushes = []
    for i in range(5):
        t6 = FakeTransport()
        t6.kvr_status, t6.kvr_body = 503, b"nope"
        n6 = Notifier(t6, cfg.tg_token, cfg.tg_chat)
        run_poll(cfg, store6, t6, n6, dry_run=False)
        pushes.extend(n6.sent)
    res.check("silent for the first 2 failures, fires on the 3rd", len(pushes) == 1,
              f"{len(pushes)} pushes")
    res.check("the blind push is distinct from an appointment alert",
              bool(pushes) and "BLIND" in pushes[0] and "EARLIER APPOINTMENT" not in pushes[0],
              pushes[0][:120] if pushes else "")
    res.check("it names the error", bool(pushes) and "HTTP 503" in pushes[0])

    say("\n[7] recovery is announced, then the real alert follows")
    t7 = FakeTransport()
    t7.kvr_body = _payload([("2026-10-06", [T_OCT06_0805]), ("2026-10-13", [])])
    n7 = Notifier(t7, cfg.tg_token, cfg.tg_chat)
    code = run_poll(cfg, store6, t7, n7, dry_run=False)
    res.check("exit code 0", code == 0, f"got {code}")
    res.check("two pushes: recovered + appointment", len(n7.sent) == 2, str(len(n7.sent)))
    res.check("first is the recovery notice", bool(n7.sent) and "recovered" in n7.sent[0])
    res.check("second is the appointment", len(n7.sent) > 1 and "Tue 06 Oct 2026" in n7.sent[1])

    say("\n[8] ipBlacklisted is escalated immediately and by name")
    _wipe(cfg.state_file)
    t8, store8, n8 = fresh()
    t8.kvr_status = 403
    t8.kvr_body = b'{"errors":[{"errorCode":"ipBlacklisted","statusCode":403,"errorMessage":"Zu viele Anfragen"}]}'
    code = run_poll(cfg, store8, t8, n8, dry_run=False)
    res.check("exit code 1", code == 1, f"got {code}")
    res.check("pushes on the FIRST blacklist, not the third", len(n8.sent) == 1, str(len(n8.sent)))
    res.check("says blocked, not merely blind",
              bool(n8.sent) and "BLOCKED" in n8.sent[0], n8.sent[0][:100] if n8.sent else "")

    say("\n[9] rateLimitExceeded is a soft throttle, not a blacklist")
    _wipe(cfg.state_file)
    t9, store9, n9 = fresh()
    t9.kvr_status = 429
    t9.kvr_body = b'{"errors":[{"errorCode":"rateLimitExceeded","statusCode":429,"errorMessage":"Zu viele Anfragen"}]}'
    code = run_poll(cfg, store9, t9, n9, dry_run=False)
    res.check("exit code 1", code == 1, f"got {code}")
    res.check("no immediate blacklist push", len(n9.sent) == 0, str(n9.sent))
    st = normalise_state(store9.load()[0])
    res.check("classified as ratelimited", "ratelimited" in st.get("last_error", ""),
              st.get("last_error", ""))

    say("\n[10] a totally empty calendar is valid, counted, and escalated")
    _wipe(cfg.state_file)
    store10 = FileStore(cfg.state_file)
    empties = []
    for i in range(3):
        t10 = FakeTransport()
        t10.kvr_body = b'{"availableDays":[]}'
        n10 = Notifier(t10, cfg.tg_token, cfg.tg_chat)
        code = run_poll(cfg, store10, t10, n10, dry_run=False)
        empties.extend(n10.sent)
    res.check("empty calendar is not an error exit", code == 0, f"got {code}")
    res.check("no appointment alert for an empty calendar",
              not any("EARLIER APPOINTMENT" in m for m in empties), str(empties))
    res.check("but a persistent empty calendar is escalated once", len(empties) == 1,
              f"{len(empties)} pushes")

    say("\n[11] state store failure FAILS OPEN (duplicate, never silence)")
    class DeadStore:
        def describe(self): return "dead store"
        def load(self): return {}, "simulated gist 503"
        def save(self, state): return False, "simulated gist 503"
    t11 = FakeTransport()
    t11.kvr_body = _payload([("2026-10-06", [T_OCT06_0805]), ("2026-10-13", [])])
    n11 = Notifier(t11, cfg.tg_token, cfg.tg_chat)
    code = run_poll(cfg, DeadStore(), t11, n11, dry_run=False)
    res.check("still alerts with no usable state", len(n11.sent) == 1, str(len(n11.sent)))
    res.check("exit code 0 — a dead store is not a dead watcher", code == 0, f"got {code}")

    say("\n[12] a failed Telegram send is retried next poll, not swallowed")
    _wipe(cfg.state_file)
    store12 = FileStore(cfg.state_file)
    t12 = FakeTransport()
    t12.telegram_ok = False
    t12.kvr_body = _payload([("2026-10-06", [T_OCT06_0805]), ("2026-10-13", [])])
    n12 = Notifier(t12, cfg.tg_token, cfg.tg_chat)
    code = run_poll(cfg, store12, t12, n12, dry_run=False)
    res.check("exit code 1 when the alert could not be delivered", code == 1, f"got {code}")
    st12 = normalise_state(store12.load()[0])
    res.check("the date is NOT marked as alerted", "2026-10-06" not in st12.get("alerted", {}),
              str(st12.get("alerted")))
    t12b = FakeTransport()
    t12b.kvr_body = t12.kvr_body
    n12b = Notifier(t12b, cfg.tg_token, cfg.tg_chat)
    run_poll(cfg, store12, t12b, n12b, dry_run=False)
    res.check("the next poll retries the alert", len(n12b.sent) == 1, str(n12b.sent))

    say("\n[13] the dead-man's switch spots a watcher that stopped")
    stale = blank_state()
    stale["last_poll_epoch"] = int(time.time() - 3 * 3600)
    stale["last_poll_iso"] = "2026-09-01T18:00:00+02:00"
    stale_path = os.path.join(tmpdir, "stale.json")
    with open(stale_path, "w", encoding="utf-8") as fh:
        json.dump(stale, fh)
    t13 = FakeTransport()
    n13 = Notifier(t13, cfg.tg_token, cfg.tg_chat)
    code = run_heartbeat_check(cfg, FileStore(stale_path), n13, 60)
    res.check("exit code 1 on a stale watcher", code == 1, f"got {code}")
    res.check("pushes a STOPPED warning", bool(n13.sent) and "STOPPED" in n13.sent[0])
    res.check("makes no KVR request", not any(u == CAL_URL for _m, u in t13.calls), str(t13.calls))

    say("\n[14] secrets never survive into printable output")
    leaky = f"boom at https://api.telegram.org/bot{fake_token}/sendMessage"
    res.check("registered token is scrubbed", fake_token not in scrub(leaky), scrub(leaky))
    res.check("url-encoded token is scrubbed",
              urllib.parse.quote(fake_token, safe="") not in
              scrub("x " + urllib.parse.quote(fake_token, safe="")))
    res.check("an unregistered token-shaped string is scrubbed too",
              "999888777:BBotherTOKENotherTOKENotherTOKEN123" not in
              scrub("leak 999888777:BBotherTOKENotherTOKENotherTOKEN123 end"))
    joined = "\n".join(n.sent + n7.sent + n8.sent)
    res.check("no push body contains the token", fake_token not in joined)

    say("\n[15] the URL is exactly the verified contract")
    url = calendar_url(cfg)
    res.check("officeIds=10471", "officeIds=10471" in url, url)
    res.check("serviceIds=1071907", "serviceIds=1071907" in url, url)
    res.check("serviceCounts=1", "serviceCounts=1" in url, url)
    today = berlin_now().date()
    res.check("startDate is today in Berlin, not empty",
              f"startDate={today.isoformat()}" in url, url)
    res.check("endDate is inside the ~12-month server limit",
              f"endDate={(today + timedelta(days=cfg.horizon_days)).isoformat()}" in url
              and cfg.horizon_days <= MAX_HORIZON_DAYS, url)
    res.check("no slotsStartDate/slotsEndDate (they truncate availableDays)",
              "slots" not in url, url)
    res.check("read-only path", "/available-calendar/" in url and "reserve" not in url, url)

    say(f"\n{'=' * 62}\nself-test: {res.passed} passed, {res.failed} failed\n{'=' * 62}")
    return 0 if res.failed == 0 else 1


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="One polite read-only poll of the Munich KVR calendar.")
    parser.add_argument("--dry-run", action="store_true",
                        help="do the live GET, print what would be sent, send and save nothing")
    parser.add_argument("--self-test", action="store_true",
                        help="offline end-to-end test with a synthetic earlier date")
    parser.add_argument("--heartbeat-check", type=int, metavar="MAX_MINUTES", default=0,
                        help="make no KVR request; alert if the last poll is older than this")
    parser.add_argument("--quiet", action="store_true", help="suppress the stderr log")
    args = parser.parse_args(argv)

    global _QUIET
    _QUIET = args.quiet

    if args.self_test:
        return run_self_test()

    cfg = Config(os.environ)
    register_secret(cfg.tg_token)
    register_secret(cfg.gist_token)
    register_secret(cfg.fallnummer)
    try:
        cfg.validate(need_telegram=not args.dry_run)
    except ConfigError as exc:
        log(f"CONFIG ERROR: {exc}")
        return 2

    transport = Transport(timeout=cfg.timeout, user_agent=cfg.user_agent)
    store = build_store(cfg, transport)
    notifier = Notifier(transport, cfg.tg_token, cfg.tg_chat, dry_run=args.dry_run)

    if args.heartbeat_check:
        return run_heartbeat_check(cfg, store, notifier, args.heartbeat_check)

    # Retire on its own. After the appointment has passed there is nothing to
    # improve on, and a watcher that keeps polling a municipal API forever
    # because nobody remembered to switch it off is bad manners.
    if cfg.retire_after:
        try:
            if berlin_now().date() > parse_iso_date(cfg.retire_after):
                log(f"retired: today is past KVR_RETIRE_AFTER={cfg.retire_after}. "
                    f"Delete or disable the schedule.")
                return 0
        except ValueError:
            pass

    log(f"watch.py {VERSION} — target_before={cfg.target_before} "
        f"state={store.describe()} dry_run={args.dry_run}")
    return run_poll(cfg, store, transport, notifier, dry_run=args.dry_run)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
