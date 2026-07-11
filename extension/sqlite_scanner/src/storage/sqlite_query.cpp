#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "sqlite_scanner.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "storage/sqlite_transaction.hpp"
#include "sqlite_db.hpp"
#include "sqlite_stmt.hpp"
#include "sqlite_utils.hpp"

namespace duckdb {

static unique_ptr<FunctionData> SQLiteQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<SqliteBindData>();

	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to sqlite_query cannot be NULL");
	}

	// look up the database to query
	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in sqlite_query", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "sqlite") {
		throw BinderException("Attached database \"%s\" does not refer to a SQLite database", db_name);
	}
	auto &sqlite_catalog = catalog.Cast<SQLiteCatalog>();
	auto &transaction = SQLiteTransaction::Get(context, catalog);
	auto sql = input.inputs[1].GetValue<string>();
	// strip any trailing semicolons
	StringUtil::RTrim(sql);
	while (!sql.empty() && sql.back() == ';') {
		sql = sql.substr(0, sql.size() - 1);
		StringUtil::RTrim(sql);
	}

	auto &con = transaction.GetDB();
	auto stmt = con.Prepare(sql);
	if (!stmt.stmt) {
		throw BinderException("Failed to prepare query \"%s\"", sql);
	}
	for (idx_t c = 0; c < stmt.GetColumnCount(); c++) {
		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back(stmt.GetName(c));
	}
	stmt.Close();
	if (names.empty()) {
		throw BinderException("Failed to execute query \"%s\" - query must return data", sql);
	}
	result->rows_per_group = optional_idx();
	result->sql = std::move(sql);
	result->all_varchar = true;
	result->file_name = sqlite_catalog.GetDBPath();
	result->global_db = &con;
	return std::move(result);
}

SQLiteQueryFunction::SQLiteQueryFunction()
    : TableFunction("sqlite_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr, SQLiteQueryBind) {
	SqliteScanFunction scan_function;
	init_global = scan_function.init_global;
	init_local = scan_function.init_local;
	function = scan_function.function;
	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}
} // namespace duckdb
