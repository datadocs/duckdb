#!/usr/bin/env bash

#
# Description:
#
#   A bash script for building DuckDB with the Datadocs extension
#   (This script has been tested on Ubuntu 22.04 and MacOS Sonoma 14)
# 
# Usage: build-duckdb-for-datadocs.sh [debug|cldebug|release|...] [--shell] [-v${version}]
#
#        --shell        open built duckdb shell after build done
#        --v${version}  build duckdb with a explicit version string (e.g., -v1.2.1)
#
# Author:  Liu Yue @hangxingliu
# Version: 2025-04-09
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
print_cmd() { printf "\$ %s\n" "$*" >&2; }
execute() { print_cmd "$@"; "$@" || throw "Failed to execute '$1'"; }

# this relative path is based on the root of the project
log_file="./scripts/logs/build-$(date "+%Y%m%d-%H%M").log"; 
open_duckdb_shell=
make_target=()
explicit_version=
parse_args() {
    local arg
    while [ "${#@}" -gt 0 ]; do
        arg="$1"; shift;
        case "$arg" in
            --shell) open_duckdb_shell=1;;
            -v) explicit_version="v${1}"; shift;;
            -v*) explicit_version="v${arg#'-v'}";;
            *) make_target+=( "$arg" );;
        esac
    done
}
parse_args "$@";
[ "${#make_target[@]}" -gt 0 ] || make_target=( release );

command -v cmake >/dev/null || throw "cmake is not installed!";
command -v ninja >/dev/null || throw "ninja is not installed!";

# Using Clang as the compiler
if [ -z "$CC" ]; then
    CLANG="$(command -v clang)";
    [ -z "$CLANG" ] && CLANG="$(command -v clang-19)";
    [ -z "$CLANG" ] && CLANG="$(command -v clang-18)";
    [ -z "$CLANG" ] && throw "clang is not found!";
    execute export CC="${CLANG}"
fi
if [ -z "$CXX" ]; then
    CLANG="$(command -v clang++)";
    [ -z "$CLANG" ] && CLANG="$(command -v clang++-19)";
    [ -z "$CLANG" ] && CLANG="$(command -v clang++-18)";
    [ -z "$CLANG" ] && throw "clang++ is not found!";
    execute export CXX="${CLANG}"
fi

pushd "$( dirname -- "${BASH_SOURCE[0]}" )/.." >/dev/null || exit 1;
execute mkdir -p "$(dirname -- "${log_file}")";

#
# Set explicit version string
#
if [ -n "$explicit_version" ]; then
    extra_cmake_vars="-DDUCKDB_EXPLICIT_VERSION=${explicit_version}";
    execute export EXTRA_CMAKE_VARIABLES="${extra_cmake_vars}"
    # make sure the "DUCKDB_EXPLICIT_VERSION" in DuckDB's CMakeLists.txt doesn't changed:
    grep -q -F 'DUCKDB_EXPLICIT_VERSION' CMakeLists.txt || 
        throw "'DUCKDB_EXPLICIT_VERSION' is unknown (the CMakeLists.txt in might be changed)";
fi

#
# Build the following extension
#
execute export BUILD_AUTOCOMPLETE=1;  # for the auto-completion feature in REPL 
execute export BUILD_JSON=1;          # it is a dependency of Datadocs extension
execute export BUILD_DATADOCS=1;

#
# Use CMkae `Ninja` generator
# https://cmake.org/cmake/help/latest/manual/cmake-generators.7.html#ninja-generators
#
execute export GEN=ninja;

# https://cmake.org/cmake/help/latest/envvar/CMAKE_BUILD_PARALLEL_LEVEL.html
# execute export CMAKE_BUILD_PARALLEL_LEVEL="$(nproc)";

SECONDS=0
printf "\n  log file: %s\n\n" "$log_file";

#
# the main command for building:
#
# make release
#
make_cmd=( make "-j$(nproc)" "${make_target[@]}" );
print_cmd "${make_cmd[@]}" | tee "${log_file}";
"${make_cmd[@]}" 2>&1 | tee -a "${log_file}";

exitcode="${PIPESTATUS[0]}"
if [ "$exitcode" != 0 ]; then
    printf "\n  log file: %s\n\n" "$log_file";
    throw "Failed to build";
fi

echo "";
echo "build done: +${SECONDS}s"
echo "";

# Executing the built DuckDB shell for testing
if [ -n "$open_duckdb_shell" ]; then execute ./build/release/duckdb; fi
