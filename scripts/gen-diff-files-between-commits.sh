#!/usr/bin/env bash

#
# Description:
#
#   This script dump info of each commit between two commits into the `git-diff-logs` directory
#     for easily searching historical changes.
#
# Author:  Liu Yue @hangxingliu
# Version: 2026-06-23
#

TARGET_DIR="git-diff-logs";

# https://github.com/duckdb/duckdb/compare/ingest-v1.2.2-base...datadocs:duckdb-wasm:master
# Backport @szarnyasg's PR 16999 (#17013)
first_commit=${1:-'7c039464e452ddc3330e2691d3fa6d305521d09b'}
# On Windows CI use zip from msys2 instead of choco (#17993) ...
last_commit=${2:-'bb046a2afc0e4e6172299b43a9d2e367a02e17eb'}

throw() { printf "fatal: %s\n" "$1" >&2; exit 1; }
print_cmd() { printf "\$ %s\n" "$*"; }
execute() { print_cmd "$@"; "$@" || throw "Failed to execute '$1'"; }
get_stdout() { print_cmd "$@"; get_stdout_result="$("$@")"; }

# change the current directory to the project directory
pushd "$( dirname -- "${BASH_SOURCE[0]}" )/.." >/dev/null || exit 1;
execute mkdir -p "$TARGET_DIR";

# usage: <commit_form> <commit_to> <base-dir>
gen_diff_files() {
    git_cmd=( git rev-list --reverse "${1}..${2}" );
    get_stdout "${git_cmd[@]}";

    commit_hash=();
    count=0;
    while read -r line; do
        [ -z "$line" ] && continue;
        commit_hash+=( "$line" );
        count=$((count+1));
    done <<< "${get_stdout_result}";

    echo "";
    echo "found ${count} commits";
    echo "";

    commit_index=0;
    while [[ "$commit_index" -lt "$count" ]]; do
        commit_ptr="${commit_hash[$commit_index]}";
        file_name="${3}/${commit_index}-${commit_ptr}.patch";

        commit_index=$((commit_index+1));
        git_cmd=( git show --stat -p -n1 "${commit_ptr}");
        print_cmd "${git_cmd[@]}" "> ${file_name}";
        "${git_cmd[@]}" > "${file_name}" || throw "failed to run 'git format-patch'";
    done
}

gen_diff_files "$first_commit" "$last_commit" "$TARGET_DIR";

