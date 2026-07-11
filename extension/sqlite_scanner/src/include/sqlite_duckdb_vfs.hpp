//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlite_duckdb_vfs.hpp
//
// A SQLite VFS that routes all file I/O through DuckDB's own FileSystem.
//
// In duckdb-wasm the default SQLite VFS operates on an in-memory/emscripten
// filesystem that is never written back to OPFS, so SQLite writes do not
// persist across reloads. By opening the SQLite database through this VFS the
// reads/writes go through `duckdb::FileSystem` (the OPFS-backed WebFileSystem in
// the browser) — exactly the path DuckDB's own database files use to persist.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

//! Name of the registered VFS; pass to sqlite3_open_v2 as the vfs argument.
const char *DuckDBSQLiteVFSName();

//! Register (idempotently) a SQLite VFS backed by `fs`. Safe to call on every
//! extension load; the most recently registered FileSystem is used.
void RegisterDuckDBSQLiteVFS(FileSystem &fs);

} // namespace duckdb
