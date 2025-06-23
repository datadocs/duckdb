#!/usr/bin/env bash

#
# Description:
#
#   A bash script for building DuckDB with the Datadocs extension
#   (This script has been tested on Ubuntu 24.04 and MacOS Sonoma 14)
# 
# Usage: build-duckdb-for-datadocs.sh [debug|cldebug|release|...] [--shell] [--clean] [-v${version}]
#
#        --shell        open built duckdb shell after build done
#        --clean        clean the target directory before building for a brand new build
#        -v${version}   build duckdb with a explicit version string (e.g., -v1.2.1)
#
# Author:  Liu Yue @hangxingliu
# Version: 2026-06-23
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

# Define the path to the log file
# this relative path is based on the root of the project
log_file="./scripts/logs/build-$(date "+%Y%m%d-%H%M").log";
has_log_file=false;

# Define some basic functions
throw() { 
    echo -e "fatal: $1" >&2; 
    $has_log_file && tell_user_where_is_the_log_file; 
    exit 1;
}
tell_user_where_is_the_log_file() { printf "\n  log file: %s\n\n" "$log_file"; }
printf_to_log_file() { $has_log_file && printf "$@" | tee -a "$log_file"; }
print_cmd() { printf_to_log_file "\$ %s\n" "$*"; }
execute() { 
    print_cmd "$@"; 
    "$@" || throw "Failed to execute '$1'";
}
get_stdout() { 
    print_cmd "$@";
    get_stdout_result="$("$@")";
}
execute_to_log_file() {
    print_cmd "$@";
    "${@}" 2>&1 | tee -a "$log_file";
    exitcode="${PIPESTATUS[0]}";
    [ "$exitcode" != 0 ] && throw "Failed to execute '$1' (exitcode=${exitcode})";
}

#
# region Parse command line arguments
open_duckdb_shell=false;
do_clean=false;
make_target=()
explicit_version=
parse_args() {
    local arg
    while [ "${#@}" -gt 0 ]; do
        arg="$1"; shift;
        case "$arg" in
            --shell) open_duckdb_shell=true;;
            --clean) do_clean=true;;
            -v) explicit_version="v${1}"; shift;;
            -v*) explicit_version="v${arg#'-v'}";;
            *) make_target+=( "$arg" );;
        esac
    done
}
parse_args "$@";
[ "${#make_target[@]}" -gt 0 ] || make_target=( release );
# endregion 
#

# Precheck required softwares
command -v cmake >/dev/null || throw "cmake is not installed!";
command -v ninja >/dev/null || throw "ninja is not installed!";

# Change current working directory to the root of DuckDB project
pushd "$( dirname -- "${BASH_SOURCE[0]}" )/.." >/dev/null || exit 1;
execute mkdir -p "$(dirname -- "${log_file}")";
has_log_file=true;
printf_to_log_file "cli args: %s\n" "$*";

# Using Clang as the compiler by default
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

# Dump the environment
execute_to_log_file uname -a;
execute_to_log_file "$CC" --version;
execute_to_log_file "$CXX" --version;
[ -d .git ] && execute_to_log_file git log -n1;


# Set explicit version string
if [ -n "$explicit_version" ]; then
    get_stdout git log -n 1 --format=%h;
    git_describe="v${explicit_version#'v'}-0-g${get_stdout_result}";
    execute export OVERRIDE_GIT_DESCRIBE="${git_describe}";
    # execute export EXTRA_CMAKE_VARIABLES="-DOVERRIDE_GIT_DESCRIBE='${git_describe}'"
    
    # make sure the "OVERRIDE_GIT_DESCRIBE" in DuckDB's CMakeLists.txt doesn't changed:
    grep -q -F 'OVERRIDE_GIT_DESCRIBE' CMakeLists.txt || 
        throw "'OVERRIDE_GIT_DESCRIBE' is unknown (the CMakeLists.txt in might be changed)";
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

if $do_clean; then execute_to_log_file make clean; fi

SECONDS=0;
tell_user_where_is_the_log_file;

#
#   ____    ___    ____    _____ 
#  / ___|  / _ \  |  _ \  | ____|
# | |     | | | | | |_) | |  _|  
# | |___  | |_| | |  _ <  | |___ 
#  \____|  \___/  |_| \_\ |_____|
#
# the core command for building:
#   make release
execute_to_log_file make "-j$(nproc)" "${make_target[@]}";

printf_to_log_file "\n  build done: +%ss\n\n" "${SECONDS}";

# Executing the built DuckDB shell for testing
if $open_duckdb_shell; then execute "./build/${make_target[0]}/duckdb"; fi
