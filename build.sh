#!/bin/sh
# 主程序（需与 VSCode tasks 中源文件列表保持同步）
g++ -std=c++17 -g \
  BlockManager.cpp Timer.cpp bfilter.cpp rfilter.cpp query.cpp \
  workloadAnalyzer.cpp main.cpp \
  -o main

# 工作负载分析器自测（从仓库根目录执行；可执行文件在 test/）
# g++ -std=c++17 -g -I. \
#   BlockManager.cpp Timer.cpp bfilter.cpp rfilter.cpp query.cpp \
#   workloadAnalyzer.cpp test/workload_analyzer_test.cpp \
#   -o test/workload_analyzer_test && ./test/workload_analyzer_test
