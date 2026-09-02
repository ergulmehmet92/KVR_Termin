# KVR_Termin — Einbürgerungsurkunde appointment watcher

Watches Munich's KVR booking calendar for a cancellation that frees an
**Einbürgerungsurkunde** appointment earlier than the one I already hold, and
pushes to Telegram when one appears.

Personal tool, one person, one appointment. It is **read-only**: it issues `GET`
requests against the public availability endpoint and never books, reserves or
cancels anything.

## What it watches

| | |
|---|---|
| Office | `10471` — Einbürgerungsbehörde, Ruppertstr. 19, München |
| Service | `1071907` — Einbürgerung |
| Endpoint | `www48.muenchen.de/buergeransicht/api/citizen/available-calendar/` |
| Alerts when | a bookable date is **strictly earlier** than `KVR_TARGET_BEFORE` |
| Booking link | [`#/services/1071907/locations/10471`](https://stadt.muenchen.de/buergerservice/terminvereinbarung.html#/services/1071907/locations/10471) |

## How it runs

GitHub Actions, `*/10 * * * *`. Each run polls **6 times over ~9 minutes** with
75–105 s jitter, so the effective cadence is roughly every 90 seconds — while
GitHub's scheduler only has to fire every 10 minutes. That matters because
GitHub documents that scheduled runs *"can be delayed during periods of high
load"* and may be dropped, with no retry and no published SLA. Treating cron as
the clock would leave real holes; treating it as a coarse trigger does not.

Public repo, so Actions minutes are unmetered and this costs nothing. On a
private repo the same schedule would exhaust GitHub Free's 2,000 min/month and
the watcher would silently die partway through each month.

Request rate is ~40/hour against an advertised limit of 120/60s — about 0.6% of
budget. The endpoint sits behind an F5 layer that IP-blacklists, so jitter and
restraint matter more than frequency.

## Configuration

**Secrets** (Settings → Secrets and variables → Actions → Secrets)

| name | what |
|---|---|
| `TG_BOT_TOKEN` | @BotFather token |
| `TG_CHAT_ID` | Telegram chat id |
| `KVR_FALLNUMMER` | `Fallnummer / caseworker` — personal data, pasted into the alert |

**Variables** (same page → Variables)

| name | what |
|---|---|
| `KVR_TARGET_BEFORE` | `YYYY-MM-DD` — the appointment currently held |

No personal access token is involved. State is committed back to this repo by
the workflow using the built-in `GITHUB_TOKEN`, which exists only for the life
of a run.

To change the target date, edit the `KVR_TARGET_BEFORE` variable. Nothing to
redeploy.

## State

`state/kvr-state.json` — which dates have already been alerted on, the failure
counter, and the last heartbeat. Dates and counters only: never a token, never
the Fallnummer.

Unchanged state is rewritten at most once a day, so the commit log stays quiet.
Anything that *matters* — a new date, an alert, a failure streak — commits
immediately.

## How to tell it has gone stale

This is the question that matters for a watcher, because a dead one looks
exactly like a quiet one.

1. **The daily heartbeat.** One "still alive" push each morning (09:00 Berlin).
   If it stops arriving, the watcher is dead — that is the whole point of it.
2. **The commit log.** `state/kvr-state.json` should get a commit at least
   daily. No commit for over a day means the cron has stopped firing.
3. **The Actions tab.** Runs should appear every ~10 minutes.

It also pushes a *distinct* "watcher is blind" message after 3 consecutive
failed polls, so an API change or an IP block announces itself rather than
looking like "no appointments yet".

## Failure stance

Fail-open, deliberately. Every state-store failure — unreadable file, corrupt
blob, two runs overlapping — degrades to a **duplicate alert**, never to
silence. Unknown state is treated as empty, empty state means everything looks
new, and new means it shouts.

A non-200, an HTML error page or an unparseable body is a **failure**, never
"no appointments available". Reporting an empty calendar because the API broke
is the worst thing this could do.

## Retirement

`KVR_RETIRE_AFTER` is pinned to the target date, so the watcher stops polling a
municipal API on its own once the appointment has passed. To stop it sooner:

```
gh workflow disable kvr-watch.yml --repo ergulmehmet92/KVR_Termin
```

## Local use

```
python3 cloud/watch.py --self-test     # 64 offline checks, no network
python3 cloud/watch.py --dry-run       # one live GET, prints what it would send
```
