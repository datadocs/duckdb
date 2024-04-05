#!/usr/bin/env bash

throw() { echo -e "fatal: $1" >&2; exit 1; }
execute() { echo "$ $*"; "$@" || throw "Failed to execute '$1'"; }
CYAN="\x1b[36m";
RESET="\x1b[0m";

pushd "$( dirname -- "${BASH_SOURCE[0]}" )/.." >/dev/null || exit 1;

DUCKDB_SHELL="$(pwd)/build/release/duckdb";
[ -x "$DUCKDB_SHELL" ] || DUCKDB_SHELL="$(pwd)/build/debug/duckdb";

[ -x "$DUCKDB_SHELL" ] || throw "'$DUCKDB_SHELL' is not a executable file";

popd >/dev/null || exit 1;
[ -n "$1" ] || throw "Please provide a file path for debugging";
[ -f "$1" ] || throw "'$1' is not a file";

FILE_PREFIX="debug_file_io_$(date '+%m%d_%H%M%S')";
LOG_FILE="${FILE_PREFIX}.log";
EXPORT_FILE="${FILE_PREFIX}.csv";

# https://duckdb.org/docs/sql/statements/profiling
SQL="explain analyze copy (select * exclude(__rownum__) from ingest_file('$1')) to '$EXPORT_FILE';"

printf "SQL = ${CYAN}%s${RESET}\n" "${SQL}";
printf "LOG = ${CYAN}%s${RESET}\n" "${LOG_FILE}";

SECONDS=0
echo "$SQL" | "$DUCKDB_SHELL" | 
    awk -v prefix=">>> D7NX >>> " -v log_file="$LOG_FILE" \
    'index($0, prefix)==1 { print substr($0, length(prefix)+1)  > log_file; next; }
    { print $0 > log_file; print $0; }';
echo "done: +${SECONDS}s"