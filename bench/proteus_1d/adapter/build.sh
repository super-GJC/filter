#!/usr/bin/env bash
# Build m4 × Proteus SuRF-style adapter.
# -march=native: required by Proteus clhash (PCLMUL).
# Links only bench/Proteus/include/*.cpp (not RocksDB / not Proteus's SuRF bench).
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"
PROT="$ROOT/Proteus"
g++ -std=c++14 -Ofast -march=native -w \
  -I"$PROT/include" \
  m4_proteus_bench.cpp \
  "$PROT"/include/*.cpp \
  -o m4_proteus_bench
echo "built: $(pwd)/m4_proteus_bench"
