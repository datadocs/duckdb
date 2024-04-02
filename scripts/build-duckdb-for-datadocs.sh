#!/usr/bin/env bash

#
# Description:
#
#   A bash script for building DuckDB with the Datadocs extension
#   (This script has been tested on Ubuntu 22.04 and MacOS Sonoma 14)
# 
# Author:  Liu Yue @hangxingliu
# Version: 2024-04-03
#
# Required Softwares:
#
#   cmake, clang, ninja, git, curl
#
#     sudo -E apt install build-essential ninja-build libssl-dev clang git curl
#
# Tips:
#
#   Because building the Datadocs DuckDB extension requires fetching dependencies 
#     from the internet (e.g., github.com). If you want CMake to fetch them
#     via a HTTP proxy server, please follow these instructions:
#   1. Please make sure your CMake is built with the `--system-curl` configuration
#      (Otherwise, CMake may not recognize the proxy-related environment variables)
#      You can use the following command for building a suitable CMake on Ubuntu:
#
#        sudo -E apt install libcurl4-openssl-dev
#        wget https://github.com/Kitware/CMake/releases/download/v3.29.0/cmake-3.29.0.tar.gz
#        tar xf cmake-3.29.0.tar.gz && cd cmake-3.29.0/
#        ./configure --parallel="$(nproc)" --system-curl
#        make -j "$(nproc)" && sudo make install
#
#   2. Then export the environment variables `HTTP_PROXY` and `HTTPS_PROXY` before executing
#      this script. Here are example commands:
#
#        export HTTP_PROXY=http://127.0.0.1:8888
#        export HTTPS_PROXY=http://127.0.0.1:8888
#
throw() { echo -e "fatal: $1" >&2; exit 1; }
execute() { echo "$ $*"; "$@" || throw "Failed to execute '$1'"; }
has_flag() {
    local flag="$1"; shift;
    for arg; do [[ "$arg" == "$flag" ]] && return 0; done
    return 1;
}

command -v cmake >/dev/null || throw "cmake is not installed!";
command -v ninja >/dev/null || throw "ninja is not installed!";
command -v clang >/dev/null || throw "clang is not installed!";
command -v clang++ >/dev/null || throw "clang++ is not installed!";

pushd "$( dirname -- "${BASH_SOURCE[0]}" )/.." >/dev/null || exit 1;

# Build the following extension
execute export BUILD_AUTOCOMPLETE=1;  # for the auto-completion feature in REPL 
execute export BUILD_JSON=1;          # it is a dependency of Datadocs extension
execute export BUILD_DATADOCS=1;

# Using Clang as the compiler
execute export CC="$(command -v clang)"
execute export CXX="$(command -v clang++)"

# https://cmake.org/cmake/help/latest/envvar/CMAKE_BUILD_PARALLEL_LEVEL.html
# execute export CMAKE_BUILD_PARALLEL_LEVEL="$(nproc)";

SECONDS=0
execute export GEN=ninja;
execute make "-j$(nproc)";

echo "";
echo "build done: +${SECONDS}s"
echo "";

# Executing the built DuckDB shell for testing
if has_flag "--shell" "${@}"; then execute ./build/release/duckdb; fi
