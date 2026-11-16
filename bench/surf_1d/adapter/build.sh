#!/usr/bin/env bash
# Build climate × SuRF adapter (header-only SuRF).
set -euo pipefail
cd "$(dirname "$0")"
g++ -std=c++17 -O2 -mpopcnt \
  -I../SuRF/include \
  climate_surf_bench.cpp \
  -o climate_surf_bench
echo "built: $(pwd)/climate_surf_bench"
