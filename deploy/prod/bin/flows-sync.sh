#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/apps/sdc"
docker compose run --rm collector flows sync --use-price-range
