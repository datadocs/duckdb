#include "storage/sqlite_transaction.hpp"
#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_index_entry.hpp"
#include "storage/sqlite_schema_entry.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

SQLiteTransaction::SQLiteTransaction(SQLiteCatalog &sqlite_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), sqlite_catalog(sqlite_catalog) {
	if (sqlite_catalog.InMemory()) {
		// in-memory database - get a reference to the in-memory connection
		db = sqlite_catalog.GetInMemoryDatabase();
	} else {
		// on-disk database - open a new database connection
		owned_db = SQLiteDB::Open(sqlite_catalog.path, sqlite_catalog.options, true);
		db = &owned_db;
	}
}

SQLiteTransaction::~SQLiteTransaction() {
	sqlite_catalog.ReleaseInMemoryDatabase();
}

void SQLiteTransaction::Start() {
	db->Execute("BEGIN TRANSACTION");
}
void SQLiteTransaction::Commit() {
	db->Execute("COMMIT");
}
void SQLiteTransaction::Rollback() {
	db->Execute("ROLLBACK");
}

SQLiteDB &SQLiteTransaction::GetDB() {
	return *db;
}

SQLiteTransaction &SQLiteTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<SQLiteTransaction>();
}

string ExtractSelectStatement(const string &create_view) {
	Parser parser;
	parser.ParseQuery(create_view);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw BinderException(
		    "Failed to create view from SQL string - \"%s\" - statement did not contain a single CREATE VIEW statement",
		    create_view);
	}
	auto &create_statement = parser.statements[0]->Cast<CreateStatement>();
	if (create_statement.info->type != CatalogType::VIEW_ENTRY) {
		throw BinderException(
		    "Failed to create view from SQL string - \"%s\" - view did not contain a CREATE VIEW statement",
		    create_view);
	}
	auto &view_info = create_statement.info->Cast<CreateViewInfo>();
	return view_info.query->ToString();
}

void ExtractColumnIds(const ParsedExpression &expr, TableCatalogEntry &table, CreateIndexInfo &info) {
	if (expr.GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto &colname = colref.GetColumnName();
		auto &column_def = table.GetColumn(colname);
		auto index = column_def.Oid();
		if (std::find(info.column_ids.begin(), info.column_ids.end(), index) == info.column_ids.end()) {
			info.column_ids.push_back(index);
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { ExtractColumnIds(child, table, info); });
}

unique_ptr<CreateIndexInfo> FromCreateIndex(ClientContext &context, TableCatalogEntry &table, string sql) {
	// parse the SQL statement
	Parser parser;
	parser.ParseQuery(sql);

	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw BinderException("Failed to create index from SQL string - \"%s\" - statement did not contain a single "
		                      "CREATE INDEX statement",
		                      sql);
	}
	auto &create_statement = parser.statements[0]->Cast<CreateStatement>();
	if (create_statement.info->type != CatalogType::INDEX_ENTRY) {
		throw BinderException(
		    "Failed to create view from SQL string - \"%s\" - view did not contain a CREATE INDEX statement", sql);
	}
	auto info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(create_statement.info));
	info->sql = std::move(sql);
	for (auto &expr : info->expressions) {
		ExtractColumnIds(*expr, table, *info);
	}
	return info;
}

optional_ptr<CatalogEntry> SQLiteTransaction::GetCatalogEntry(const string &entry_name) {
	auto entry = catalog_entries.find(entry_name);
	if (entry != catalog_entries.end()) {
		return entry->second.get();
	}
	// catalog entry not found - look up table in main SQLite database
	auto type = db->GetEntryType(entry_name);
	if (type == CatalogType::INVALID) {
		// no table or view found
		return nullptr;
	}
	unique_ptr<CatalogEntry> result;
	switch (type) {
	case CatalogType::TABLE_ENTRY: {
		CreateTableInfo info(sqlite_catalog.GetMainSchema(), entry_name);
		bool all_varchar = false;
		Value sqlite_all_varchar;
		if (context.lock()->TryGetCurrentSetting("sqlite_all_varchar", sqlite_all_varchar)) {
			all_varchar = BooleanValue::Get(sqlite_all_varchar);
		}
		db->GetTableInfo(entry_name, info.columns, info.constraints, all_varchar);
		D_ASSERT(!info.columns.empty());

		result = make_uniq<SQLiteTableEntry>(sqlite_catalog, sqlite_catalog.GetMainSchema(), info, all_varchar);
		break;
	}
	case CatalogType::VIEW_ENTRY: {
		string sql;
		db->GetViewInfo(entry_name, sql);

		unique_ptr<CreateViewInfo> view_info;
		try {
			view_info = CreateViewInfo::FromCreateView(*context.lock(), sqlite_catalog.GetMainSchema(), sql);
		} catch (std::exception &ex) {
			auto view_sql = ExtractSelectStatement(sql);
			auto catalog_name = StringUtil::Replace(sqlite_catalog.GetName(), "\"", "\"\"");
			auto escaped_view_sql = StringUtil::Replace(view_sql, "'", "''");
			auto view_def = StringUtil::Format("CREATE VIEW %s AS FROM sqlite_query(\"%s\", '%s')", entry_name,
			                                   catalog_name, escaped_view_sql);
			view_info = CreateViewInfo::FromCreateView(*context.lock(), sqlite_catalog.GetMainSchema(), view_def);
		}
		view_info->internal = false;
		result = make_uniq<ViewCatalogEntry>(sqlite_catalog, sqlite_catalog.GetMainSchema(), *view_info);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		string table_name;
		string sql;
		db->GetIndexInfo(entry_name, sql, table_name);
		if (sql.empty()) {
			throw InternalException("SQL is empty");
		}
		auto &table = GetCatalogEntry(table_name)->Cast<TableCatalogEntry>();
		auto index_info = FromCreateIndex(*context.lock(), table, std::move(sql));
		index_info->catalog = sqlite_catalog.GetName();

		auto index_entry = make_uniq<SQLiteIndexEntry>(sqlite_catalog, sqlite_catalog.GetMainSchema(), *index_info,
		                                               std::move(table_name));
		result = std::move(index_entry);
		break;
	}
	default:
		throw InternalException("Unrecognized catalog entry type");
	}
	auto result_ptr = result.get();
	catalog_entries[entry_name] = std::move(result);
	return result_ptr;
}

void SQLiteTransaction::ClearTableEntry(const string &table_name) {
	catalog_entries.erase(table_name);
}

string GetDropSQL(CatalogType type, const string &table_name, bool cascade) {
	string result;
	result = "DROP ";
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		result += "TABLE ";
		break;
	case CatalogType::VIEW_ENTRY:
		result += "VIEW ";
		break;
	case CatalogType::INDEX_ENTRY:
		result += "INDEX ";
		break;
	default:
		throw InternalException("Unsupported type for drop");
	}
	result += KeywordHelper::WriteOptionallyQuoted(table_name);
	return result;
}

void SQLiteTransaction::DropEntry(CatalogType type, const string &table_name, bool cascade) {
	catalog_entries.erase(table_name);
	db->Execute(GetDropSQL(type, table_name, cascade));
}

} // namespace duckdb
