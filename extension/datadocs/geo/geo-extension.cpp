#define DUCKDB_EXTENSION_MAIN

#include "../include/datadocs_extension.hpp"
#include "../include/datadocs.hpp"

#include "accessor-functions.hpp"
#include "constructor-functions.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "formatter-functions.hpp"
#include "geo_aggregate_function.hpp"
#include "measure-functions.hpp"
#include "parser-functions.hpp"
#include "predicate-functions.hpp"
#include "transformation-functions.hpp"

namespace duckdb {

void DatadocsExtension::LoadGeo(DatabaseInstance &inst) {
	DDGeoType = LogicalType(LogicalTypeId::BLOB);
	DDGeoType.SetAlias("GEOGRAPHY");
	const LogicalType &geo_type = DDGeoType;

	ExtensionUtil::RegisterType(inst, "Geography", geo_type);

	// add geo casts
	auto &casts = DBConfig::GetConfig(inst).GetCastFunctions();
	casts.RegisterCastFunction(LogicalType::VARCHAR, geo_type, GeoFunctions::CastVarcharToGEO, 100);
	casts.RegisterCastFunction(geo_type, LogicalType::VARCHAR, GeoFunctions::CastGeoToVarchar);

	// add geo functions
	std::vector<ScalarFunctionSet> geo_function_set {};
	// **Constructors (3)**
	auto constructor_func_set = GetConstructorScalarFunctions(geo_type);
	geo_function_set.insert(geo_function_set.end(), constructor_func_set.begin(), constructor_func_set.end());
	// **Formatters (4)**
	auto formatter_func_set = GetFormatterScalarFunctions(geo_type);
	geo_function_set.insert(geo_function_set.end(), formatter_func_set.begin(), formatter_func_set.end());
	// **Parsers (5)**
	auto parser_func_set = GetParserScalarFunctions(geo_type);
	geo_function_set.insert(geo_function_set.end(), parser_func_set.begin(), parser_func_set.end());
	// **Transformations (10)**
	auto transformation_func_set = GetTransformationScalarFunctions(geo_type);
	geo_function_set.insert(geo_function_set.end(), transformation_func_set.begin(), transformation_func_set.end());
	//  **Accessors (15)**
	auto accessor_func_set = GetAccessorScalarFunctions(geo_type);
	geo_function_set.insert(geo_function_set.end(), accessor_func_set.begin(), accessor_func_set.end());
	// **Predicates (9)**
	auto predicate_func_set = GetPredicateScalarFunctions(geo_type);
	geo_function_set.insert(geo_function_set.end(), predicate_func_set.begin(), predicate_func_set.end());
	// **Measures (9)**
	auto measure_func_set = GetMeasureScalarFunctions(geo_type);
	geo_function_set.insert(geo_function_set.end(), measure_func_set.begin(), measure_func_set.end());

	for (auto &func_set : geo_function_set) {
		ExtensionUtil::RegisterFunction(inst, func_set);
	}

	auto cluster_db_scan = GetClusterDBScanAggregateFunction(geo_type);
	CreateAggregateFunctionInfo cluster_db_scan_func_info(std::move(cluster_db_scan));
	auto &system_catalog = Catalog::GetSystemCatalog(inst);
	auto data = CatalogTransaction::GetSystemTransaction(inst);
	system_catalog.CreateFunction(data, cluster_db_scan_func_info);
}

} // namespace duckdb
