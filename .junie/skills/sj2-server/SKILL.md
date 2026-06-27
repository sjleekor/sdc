---
name: sj2-server
description: Use when accessing sj2-server (the production host for stock_data_collector / sdc) — Cronicle scheduler at http://sj2-server:3012, SSH access as whi@sj2-server, wrapper scripts under /home/whi/apps/sdc/bin/, the production compose at /home/whi/apps/sdc/compose.yaml. Trigger on requests like "Cronicle 이벤트 확인", "schedule 조회", "wrapper 스크립트 검증", "sj2-server 상태", "원격 compose / collector 동작 확인", or anything that requires inspecting prod schedule or remote deployment state.
---

# SJ2 Server Access

This skill captures the auth, endpoints, paths, and safety rules for inspecting the production sj2-server. The host runs both the Cronicle scheduler and the SDC collector deploy.

`.claude/` is gitignored, so this skill lives only in the local checkout. If you also want codex/other agents to use the same playbook, mirror the file under `./.agents/skills/sj2-server/` after the user asks.

## Credentials

All sj2-server / Cronicle credentials live in the secrets directory **outside** this repo:

```
/Users/whishaw/wss_p/stock_data_collector_secrets/cronicle_info
```

That file contains the Cronicle UI USERNAME / PASSWORD and an `APIKEY:` line. Read it on demand; never copy these values into committed files, this skill, logs, or transcript text. When invoking the API, pass the key through an env var or inline header, e.g.:

```bash
APIKEY=$(awk -F': *' '/^APIKEY:/ {print $2}' /Users/whishaw/wss_p/stock_data_collector_secrets/cronicle_info)
curl -fsS -H "X-API-Key: $APIKEY" 'http://sj2-server:3012/api/app/get_schedule/v1'
```

If the secrets file is missing, stop and ask the user — do not guess or hard-code.

## Cronicle (HTTP API)

- Base URL: `http://sj2-server:3012`
- UI: `http://sj2-server:3012/#Schedule` — SPA route, fragment is client-side only. **Don't try to WebFetch the UI URL** to learn schedule contents; use the API instead.
- Auth: `X-API-Key: <APIKEY>` header on every API call.

### Read-only endpoints (safe to call freely)

| Purpose | Endpoint |
|---|---|
| List all events | `GET /api/app/get_schedule/v1` |
| One event by id | `GET /api/app/get_event/v1?id=<event-id>` |
| Past runs of an event | `GET /api/app/get_event_history/v1?id=<event-id>` |
| Job log (per run) | `GET /api/app/get_job_log/v1?id=<job-id>` (use `&format=text` for plain log) |
| Categories | `GET /api/app/get_categories/v1` |
| Cluster/master status | `GET /api/app/get_master/v1` |

Pipe through `python3 -m json.tool` or `jq` for readability. Schedule listings are typically short enough not to need pagination, but `get_event_history` supports `&offset=` / `&limit=`.

### Mutating endpoints (require explicit user authorization in the current turn)

Do **not** call without the user asking in this exact session, even if a prior session approved similar actions:

- `POST /api/app/run_event/v1` — fire an event now
- `POST /api/app/update_event/v1` — edit schedule entry
- `POST /api/app/create_event/v1`
- `POST /api/app/delete_event/v1`
- `POST /api/app/abort_job/v1`
- Anything matching `/api/app/(create|update|delete|run|abort|reset)_*`

Default posture is read-only. State the planned mutation in plain text, get explicit go-ahead, then act.

## SSH access

- Host: `whi@sj2-server`. The user's ssh-agent / key already authorize this; no password handling needed in scripts.
- App layout on the host:

| Path | Purpose |
|---|---|
| `/home/whi/apps/sdc/` | Production deploy of this repo |
| `/home/whi/apps/sdc/compose.yaml` | Compose file edited by `--remote-update` releases |
| `/home/whi/apps/sdc/compose.yaml.bak.<timestamp>` | Auto-backups created by the release script — don't delete |
| `/home/whi/apps/sdc/bin/` | Wrapper scripts invoked by Cronicle events |
| `/home/whi/apps/sdc/.env` | Production env (secrets — never read into transcript) |

### Cronicle events and their wrappers (snapshot 2026-04-26 — verify with the API before relying)

| Cronicle event | Wrapper sequence (each: `docker compose run --rm collector <cmd>`) |
|---|---|
| `sdc_daily_pipeline` | `universe-sync.sh` → `prices-backfill-incremental.sh` → `flows-sync.sh` |
| `sdc_daily_accounts_flows` | `dart-sync-corp.sh` → `dart-sync-financials.sh` → `dart-sync-share-info.sh` → `dart-sync-xbrl.sh` → `metrics-normalize.sh` |

Both events were `timing: false`, `max_children: 1`, `multiplex: 0`, `chain: ""`. Concurrency analysis: write sets are disjoint (KRX/price tables vs DART tables), upserts use `INSERT … ON CONFLICT DO UPDATE`, no advisory locks needed → simultaneous runs were assessed safe in the 2026-04-26 review. Re-verify before relying — the schedule is the source of truth, this snapshot drifts.

### Safety rules for SSH

- Default to **read-only**: `ls`, `cat`, `wc -l`, `docker compose ps` (no flags that mutate), `docker compose logs --no-color --tail 200`.
- The harness may still gate even read-only SSH calls when the target is "production config" (e.g., reading `compose.yaml` was gated in the past). If a benign read is denied, report the denial and ask the user to allow it — do **not** chain alternative commands to bypass the intent.
- Never run any of the following without an explicit, in-turn user request:
  - `docker compose down|up|restart|rm`, `docker rm`, `docker volume rm`, `docker system prune`
  - File mutations under `/home/whi/apps/sdc/` (compose edits go through the release skill, not ad-hoc ssh)
  - `systemctl`, package installs, anything that affects services beyond the SDC stack
  - Reading `.env` or other secret files (request via the user instead)
- When you need to run a sequence, prefer one batched `ssh whi@sj2-server '<script>'` so the user sees one transcript line per logical step rather than many connections.

### Useful one-liners (read-only)

```bash
# List wrapper scripts
ssh whi@sj2-server 'ls -1 /home/whi/apps/sdc/bin/'

# Dump every wrapper at once (one-shot inspection)
ssh whi@sj2-server 'cd /home/whi/apps/sdc/bin && for f in *.sh; do echo "===== $f ====="; cat "$f"; echo; done'

# See what services are running for the SDC project
ssh whi@sj2-server 'cd /home/whi/apps/sdc && docker compose ps'

# Tail the latest collector run logs (no follow)
ssh whi@sj2-server 'cd /home/whi/apps/sdc && docker compose logs --no-color --tail 200 collector'

# Verify the compose file image tag matches the just-released git tag
ssh whi@sj2-server 'grep -nE "^\s*image:" /home/whi/apps/sdc/compose.yaml'
```

## Database inspection

To query the collected data itself (row counts, date/business-year coverage, comparing local vs production), use the `sdc-db` skill instead of ad-hoc SSH into the DB container. It wraps both the local `mydb` mirror and the sj2-server `krx_data` DB:

```bash
.agents/skills/sdc-db/scripts/dbq.sh sj2 "select min(trade_date), max(trade_date) from daily_ohlcv;"
```

## Cross-cutting tips

- The Cronicle UI fragment routes (`#Schedule`, `#History`, `#Activity`, etc.) are SPA-only — never WebFetch them. Use the API.
- Cronicle event `params.script` is a multi-line bash blob. The wrapper paths inside it are the source of truth for what runs; the SCRIPT field has no `cd` and the wrappers themselves do `cd "$HOME/apps/sdc"` before `docker compose run`.
- `docker compose run --rm collector` produces an anonymous container (no `container_name` in `compose.yaml`), so two events firing at once won't collide on container names. Same `db` service is shared (already up with `restart: unless-stopped`).
- For release-related compose edits, defer to the `sdc-release` skill rather than editing compose.yaml over SSH directly.

## Reporting back

When asked to inspect schedule state or wrappers, structure findings as:

1. Source of truth used (API endpoint or SSH path).
2. The events / scripts seen, with the relevant fields (id, timing, command, max_children, chain).
3. The concrete question answered (e.g. "동시 실행 가능?", "wrapper가 무엇을 호출?", "마지막 실행 시각?").
4. Any gating or denial encountered, so the user can grant the missing permission once if needed.
