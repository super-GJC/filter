#!/usr/bin/env bash
# Build m4 × SNARF SuRF-style adapter.
# -DNDEBUG: upstream snarf_model_builder asserts N>10000; per-chunk keys are smaller.
set -euo pipefail
cd "$(dirname "$0")"
g++ -std=c++17 -O3 -w -fpermissive -DNDEBUG \
  -I../../SNARF \
  m4_snarf_bench.cpp \
  -o m4_snarf_bench
echo "built: $(pwd)/m4_snarf_bench"
