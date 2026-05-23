#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/apps/sdc"
docker compose run --rm collector universe sync --source fdr --markets kospi,kosdaq
