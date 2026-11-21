#!/usr/bin/env bash
# Build m4 × REncoder SuRF-style adapter.
# NO -DUSE_SIMD / NO -mavx512f: this host has AVX2 only; use non-SIMD path in RBF.h.
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"
REN="$ROOT/REncoder/src"
g++ -std=c++17 -O2 -mcmodel=medium -march=native -w \
  -I"$REN" \
  m4_rencoder_bench.cpp \
  -o m4_rencoder_bench
echo "built: $(pwd)/m4_rencoder_bench"
