#!/usr/bin/env bash
# Build minimal m5 × Grafite 1D-best probe (header-only + lib/sux).
set -euo pipefail
cd "$(dirname "$0")"
g++ -std=c++17 -O2 \
  -I../../Grafite/include \
  -I../../Grafite/lib/sux \
  -DSUCCINCT_LIB_SUX \
  m5_grafite_best.cpp \
  -o m5_grafite_best
echo "built: $(pwd)/m5_grafite_best"
