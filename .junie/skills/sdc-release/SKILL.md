---
name: sdc-release
description: Use when releasing stock_data_collector / sdc — bump the project version in pyproject.toml, run lint+unit tests, commit, create and push a vX.Y.Z git tag, and update whi@sj2-server:/home/whi/apps/sdc/compose.yaml to the released image tag. Trigger on requests to release/deploy this repo, "tag a release", "bump patch/minor/major", "push and tag", or "update sj2 compose to the new version".
---

# SDC Release

Release helper for this repository. Runs through a single Python script that supports dry-run by default and only mutates state when `--apply` is passed.

The bundled script is mirrored under `./.agents/skills/sdc-release/scripts/release.py` (codex skill copy). This Claude skill reuses that same script — do not duplicate it. Always invoke it via `uv run python .agents/skills/sdc-release/scripts/release.py ...` from the repository root.

## Safety Rules

- Start with `git status --short` and identify any pre-existing dirty files. Do not revert unrelated user changes.
- Always run a dry run first, in the same turn, before any real release operation:
  `uv run python .agents/skills/sdc-release/scripts/release.py --bump patch --stage-all --remote-update`
- Only run with `--apply` when the user has explicitly asked for the actual commit/push/tag/remote update in the current turn. A prior approval does not carry over to a new release.
- If the dry run shows unexpected files staged, an existing tag, a detached HEAD, or a remote compose tag that the script cannot match, stop and report the blocker — do not pass `--apply`.
- The release tag format is `vX.Y.Z`; the package version in `pyproject.toml` is `X.Y.Z`.
- The release commit message format is `release: vX.Y.Z` unless the user provides a different message.
- Never run destructive git commands (`reset --hard`, `checkout --`, `push --force`, branch deletes) as part of this workflow.
- Never pass `--skip-tests` unless the user explicitly asks for it in the current turn.

## Standard Workflow

1. Inspect current state:
   `git status --short`
2. Dry-run the release (no mutations):
   `uv run python .agents/skills/sdc-release/scripts/release.py --bump patch --stage-all --remote-update`
3. Show the dry-run output to the user and wait for explicit approval to apply. If approved, execute:
   `uv run python .agents/skills/sdc-release/scripts/release.py --bump patch --stage-all --remote-update --apply`
4. Report back:
   - released version (old → new)
   - release commit hash
   - tag pushed
   - remote compose file updated or skipped (and which lines changed)
   - any verification commands run and their results
5. Remind the user that redeploy is a manual step, since the script intentionally stops at compose-file update. Concretely: wait for the GHCR build triggered by the tag push to finish (`.github/workflows/docker.yml`), then SSH to the remote host and run `docker compose -f /home/whi/apps/sdc/compose.yaml pull && docker compose -f /home/whi/apps/sdc/compose.yaml up -d`. Do not run these via the release script or trigger them remotely on the user's behalf unless explicitly asked.

## Common Options

- `--version X.Y.Z` — release an explicit version (mutually exclusive with `--bump`).
- `--bump patch|minor|major` — derive the next version from `pyproject.toml`.
- `--stage-all` — stage all current repo changes for the release commit (otherwise only `pyproject.toml` is staged).
- `--commit-message "..."` — override the default `release: vX.Y.Z` commit message.
- `--skip-tests` — skip `ruff` and `pytest tests/unit`. Only when the user explicitly requests it.
- `--remote-update` — SSH to `whi@sj2-server` and bump the image tag in `/home/whi/apps/sdc/compose.yaml`. The script only rewrites the image tag and runs `docker compose config` to validate; it does NOT run `docker compose pull` or `up -d`. Redeployment is a separate manual step that the operator runs after the GHCR build for the new tag has finished.
- `--remote-host`, `--remote-compose` — override the SSH target / compose path if the user is releasing somewhere non-standard.

## Remote Compose Behavior

The helper updates only `image:` lines whose tag is the current project version, with or without a leading `v`. For example, `image: repo/sdc:0.8.0` becomes `image: repo/sdc:0.8.1`, and `image: repo/sdc:v0.8.0` becomes `image: repo/sdc:v0.8.1`.

If the remote compose file uses `latest`, a digest, an environment variable, or a tag scheme that does not match `vX.Y.Z` / `X.Y.Z`, the script will fail with `No image tag matching ... found`. In that case, stop, surface the file's current image lines, and ask the user how to proceed — do not modify the script or compose file unprompted.

## Pre-flight Checklist (before suggesting `--apply`)

- Working tree contents make sense for a release commit (no stray debug files, secrets, or unrelated WIP).
- HEAD is on a real branch (`git branch --show-current` is non-empty); the script will refuse a detached HEAD.
- Tag `v<new-version>` does not already exist locally or on the remote.
- The user explicitly asked to release in the current turn.
- Lint and unit tests passed in the dry run, unless `--skip-tests` was explicitly requested.
