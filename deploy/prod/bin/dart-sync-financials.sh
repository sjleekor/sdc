#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/apps/sdc"
# 기존 데이터 보존하며 누락된 보고서 코드(11011,11012,11013,11014) 수집
docker compose run --rm collector dart sync-financials \
  --reprt-codes 11011,11012,11013,11014 \
  --bsns-years "$(date +%Y),$(($(date +%Y)-1))"

