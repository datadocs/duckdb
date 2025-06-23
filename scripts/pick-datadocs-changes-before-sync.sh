#!/usr/bin/env bash
#
#  WARNING: This script is used for manually maintaining project only.
#
#  Description:
#
#   This script is used for checking code changes from the upstream before we synced
#   our forked version of codebase with the latest version of upstream.
#
#  Usage:
#   
#   Step 1. Find the commits between our base commit and the latest upstream commit. For example:
#             https://github.com/datadocs/duckdb/compare/ingest-v1.2.2-base...v1.3.1
#
#   Step 2. Use bisection method to pick a commit between above commits. Then run this script 
#             with the hash of this commit. 
#           However, please make sure this commit has passed official CI. You could check it by the
#             following URL (there will be an green check mark on the page if it has passed CI):
#             https://github.com/duckdb/duckdb/commit/${COMMIT_HASH}
#           Run the following command: (This script will hard reset a temporary branch 
#             on the given commit and pick our commits onto it)
#
#             ./scripts/pick-datadocs-changes-before-sync.sh ${COMMIT_HASH}
#
#   Step 3. This script might be failed due to the conflicts during `git cherry-pick`,
#             you could resolve the conflicts then run `git cherry-pick --continue`. For example:
#
#             git rm -r .github
#             vim path/to/file-with-conflicts
#             git add -A -- path/to/file-with-conflicts
#             git cherry-pick --continue
# 
#   Step 4. Build this project by running the command:
#             
#             ./scripts/build-duckdb-for-datadocs.sh release -v1.3.1 --clean
#
#   Step 5. Check if the generated `duckdb` shell works expectedlly. Then you could read 
#             the document ../submodules/duckdb/SYNCING_WITH_UPSTREAM.md for syncing the code
#             with the upstream
#           If the build command doesn't work, please try fixing the code for upstream changes 
#             and try building again.
#           If you still could not find and fix the errors. Please use bisection method to 
#             pick another commit to find the source of these breaking changes. (See the step 2)
#
#   Author:  Liu Yue @hangxingliu
#   Version: 2026-06-23

BRANCH_NAME="liu/tmp-test-build";
BASE_COMMIT="$1";
# 1037: 0f233ec8cba02ba223cd647f8a19534ca9310b99 PASS
# 3123: 10fcb65b1012e84f799f2ef5804efc85ee15515b FAIL
# 2612: 263025fb444ca04616f5acf3477b4595ae367c37 FAIL
# 1723: 2a997d65a1a7ee288393285d7a9ccc7f801804c5 FAIL
# 1340: 42f17b1b82192d906429767ff078a9d08cbe5f6d PASS
# 1509: b4713559e0396e746ae6c5e2df44c0ea8cd57d7b PASS
# 1641: 43c5f3489858c0377d4a6e6d6e7ed8d0502ba1df PASS
# 1673: 242d3f78e4651f5f99f77f33cc05d2b72a986d3d PASS
# 1710: c310df6b80db7d940b74840bdd7067dc30b6ac5e FAIL
# 1689: c87ae7a2009de8d22fd8410051fecd6807aa7435 FAIL
# 1677: 9e00d4436ccbd256f059f4d52272c13459026d19 PASS

# from: Init datadocs extension
# to:   Fix errors caused by breaking changes between v1.2.2 to v1.3.1
COMMITS="46905b4fa83473da3af69095ea272b0575787707^..bb027ab4b834b94224dc8d94a5cf9d53bc91479a"

throw() { printf "fatal: %s\n" "$1" >&2; exit 1; }
print_cmd() { printf "\$ %s\n" "$*"; }
execute() { print_cmd "$@"; "$@" || throw "Failed to execute '$1' code: $?"; }
get_stdout() { print_cmd "$@"; get_stdout_result="$("$@")"; }

# go to the root of project
pushd "$( dirname -- "${BASH_SOURCE[0]}" )/.." >/dev/null || exit 1;
[ -n "$BASE_COMMIT" ] || throw "Please provide the hash of the base commit";

if [[ -n "$(git status --porcelain | awk '!/^\?\?/')" ]]; then
    echo "Warn: There are uncommitted changes in the working directory or index:"
    git status --short;

    execute git stash save -m "Save changes for '$*'";
    # exit 1;
fi

commit_hashes=();
get_stdout git rev-list --reverse "${COMMITS}"
while read -r line; do
    [ -z "$line" ] && continue;
    commit_hashes+=( "$line" );
done <<< "${get_stdout_result}";

if git rev-parse --verify "${BRANCH_NAME}" 2>/dev/null; 
then execute git checkout "${BRANCH_NAME}";
else execute git checkout -b "${BRANCH_NAME}";
fi
execute git reset --hard "${BASE_COMMIT}";
execute git cherry-pick "${commit_hashes[@]}";
