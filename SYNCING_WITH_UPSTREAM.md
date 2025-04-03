---
author: hangxingliu
updated_at: 2025-04-03 19:12:46
---
# Syncing with Upstream

This is a SOP document for regularly synchronizing with the upstream codebase: <https://github.com/duckdb/duckdb>

Relevant links:

- [DuckDB Release](https://github.com/duckdb/duckdb/releases)
- [Sublime Merge](https://www.sublimemerge.com/)
- [GitKraken](https://www.gitkraken.com/)

**It is highly recommended to prepare an independent local code of this repo** before starting the instructions in the next section.

Because you might need to compare API changes between current forked version and the latest version and inspect source files without any [Git conflict markers](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/addressing-merge-conflicts/resolving-a-merge-conflict-using-the-command-line) during Git rebase.

And here are example commands for this preparation:

``` bash
git clone --depth=1 https://github.com/datadocs/duckdb.git datadocs-duckdb-snapshot
# Then you can inspect the code in the directory `datadocs-duckdb-snapshot`
```



## Instructions

Firstly, make sure you have added the URL of official repository in your git remote. Here is the command for it:
```bash
git remote add upstream https://github.com/duckdb/duckdb.git
```


Then, please update your local git history from the upstream repo:

```
git fetch upstream
```


We can first define two environment variables `LAST_VER` and `NEW_VER` to simplify the subsequent commands:

```bash
# The version of last sync
LAST_VER="v1.2.1"

# The new version you want to sync from the upstream repo. 
# Any branch name or tag name in upstream repo can be used here: (e.g., "main")
NEW_VERSION="v1.2.2"
```

Next, create a new branch to merge our changes into the new version of upstream source code:

```bash
git checkout ingest-$LAST_VER
git checkout -b ingest-$NEW_VER

# It is highly RECOMMENDED to rebase our changes into the target branch/tag by GUI program
# to avoid elementary mistakes.
# For example: Sublime Merge, GitKraken
git rebase --interactive $NEW_VER
```

> [!TIP]
>
> 1. All changes on `.github`  can be removed during Git rebase. This forked repo doesn't use them.
> 2. Here is a brief explanation about Git conflict markers:
>    - **Current Change**: The latest code from the upstream
>    - **Incoming Change**: The changes we made previously
>
> ![A screenshot showing Git conflicts that appear when rebasing code](./images/git-conflicts-in-sync.jpg)



Next, after successfully rebasing the all changes into the target branch/tag, please do the following checks  :

1. Clean and rebuild DuckDB again: `make clean && ./scripts/build-duckdb-for-datadocs.sh release`
2. Run built DuckDB shell (bin file: `./build/release/duckdb`) perform at least the following tests:
   1. The `VARIANT` type (e.g., `SELECT 1::VARIANT::JSON`, ...)
3. Build DuckDB WASM again with this latest change (pointing `submodules/duckdb` sub-module to the this `ingest-$NEW_VER` branch) and test that WASM file in the browser
   1. `ingest_file`



Finally, you push this new branch to our forked repository: 

```bash
# Please change the following `origin` to your custom remote name if you changed it
git push origin ingest-$NEW_VER

# If the new version is based on a Git tag, please also push this tag to the repo
# for a reference purpose:
git push origin tag $NEW_VER
```

