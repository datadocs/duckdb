#ifndef DUCKDB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_BUILD_LOADABLE_EXTENSION
#endif
#include "duckdb.hpp"

#include "sqlite_db.hpp"
#include "sqlite_scanner.hpp"
#include "sqlite_storage.hpp"
#include "sqlite_scanner_extension.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#ifdef __EMSCRIPTEN__
#include "sqlite_duckdb_vfs.hpp"
#include "duckdb/common/file_system.hpp"
#endif

using namespace duckdb;

extern "C" {

static void SetSqliteDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
	SQLiteDB::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

static void LoadInternal(DatabaseInstance &db) {
#ifdef __EMSCRIPTEN__
	// Route SQLite file I/O through DuckDB's FileSystem (OPFS-backed in the
	// browser) so that ATTACHed SQLite databases read AND persist writes the
	// same way DuckDB's own database files do. SQLiteDB::Open opens through this
	// VFS by name. See sqlite_duckdb_vfs.cpp.
	RegisterDuckDBSQLiteVFS(FileSystem::GetFileSystem(db));
#endif

	SqliteScanFunction sqlite_fun;
	ExtensionUtil::RegisterFunction(db, sqlite_fun);

	SqliteAttachFunction attach_func;
	ExtensionUtil::RegisterFunction(db, attach_func);

	SQLiteQueryFunction query_func;
	ExtensionUtil::RegisterFunction(db, query_func);

	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("sqlite_all_varchar", "Load all SQLite columns as VARCHAR columns", LogicalType::BOOLEAN);

	config.AddExtensionOption("sqlite_debug_show_queries", "DEBUG SETTING: print all queries sent to SQLite to stdout",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), SetSqliteDebugQueryPrint);

	config.storage_extensions["sqlite_scanner"] = make_uniq<SQLiteStorageExtension>();
	// Also register under the "sqlite" alias so that `ATTACH '...' (TYPE sqlite)`
	// resolves this statically-linked storage extension directly at startup,
	// instead of treating "sqlite" as an unknown type and triggering an autoload
	// (which, in duckdb-wasm, fails with "Can't find the home directory ...").
	config.storage_extensions["sqlite"] = make_uniq<SQLiteStorageExtension>();
}

void SqliteScannerExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

DUCKDB_EXTENSION_API void sqlite_scanner_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *sqlite_scanner_version() {
	return DuckDB::LibraryVersion();
}

DUCKDB_EXTENSION_API void sqlite_scanner_storage_init(DBConfig &config) {
	config.storage_extensions["sqlite_scanner"] = make_uniq<SQLiteStorageExtension>();
}
}
