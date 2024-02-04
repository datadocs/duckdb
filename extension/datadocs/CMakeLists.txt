cmake_minimum_required(VERSION 3.6)

project(DatadocsExtension)

include_directories(${CMAKE_BINARY_DIR}/include)

set(CMAKE_CXX_STANDARD 17)

if (NOT WIN32)
  # No exceptions
  #add_compile_options(-fno-exceptions)
  add_definitions(-DBOOST_NO_EXCEPTIONS)
  # Enable all warnings
  add_compile_options(-Wall)
  #add_compile_options(-Wextra)
endif ()

function (fetch_dependency name cmake_file)
  if(EXISTS ${CMAKE_CURRENT_BINARY_DIR}/${name}-build)
    message(WARNING "${Cyan}Dependency found - not rebuilding - ${CMAKE_CURRENT_BINARY_DIR}/${name}-build${ColorReset}")
  else()
    message("${Cyan}Fetching Dependency '${name}' - ${CMAKE_CURRENT_BINARY_DIR}/${name}-download${ColorReset}")
    configure_file(${cmake_file} ${name}-download/CMakeLists.txt)
    execute_process(COMMAND emcmake ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${name}-download )
    if(result)
      message(FATAL_ERROR "CMake step for ${name} failed: ${result}")
    endif()
    execute_process(COMMAND ${CMAKE_COMMAND} --build .
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${name}-download )
    if(result)
      message(FATAL_ERROR "Build step for ${name} failed: ${result}")
    endif()
  endif()
endfunction()

###################
# fetch dependencies
###################

# Zlib
fetch_dependency(zlib "cmake/zlib/zlib.txt.in")
include(cmake/zlib/zlib.cmake.txt)

# ICU
fetch_dependency(icu "cmake/icu/icu.txt.in")
include(cmake/icu/icu.cmake.txt)

# libexpat
fetch_dependency(expat "cmake/libexpat/libexpat.txt.in")
include(cmake/libexpat/libexpat.cmake.txt)

# Boost
find_package(Boost 1.76.0...<2.0)
if(Boost_FOUND)
  message(NOTICE "Boost system-wide '${Boost_INCLUDE_DIRS}' will be used")
else()
  fetch_dependency(boost "cmake/boost/boost.txt.in")
endif()

##########################
# simple_icu_init library
##########################
add_library(simple_icu_init STATIC src/simple_icu_init/simple_icu_init.cpp)

# Includes
target_include_directories(simple_icu_init
        # Interface headers
        PUBLIC src/simple_icu_init
        # Privately includes icu
        PRIVATE $<TARGET_PROPERTY:icu,INTERFACE_INCLUDE_DIRECTORIES>
        )

target_link_libraries(simple_icu_init
        # Publicly link to icu
        PUBLIC icu)

add_subdirectory(geo)

set(DATADOCS_EXTENSION_FILES
	datadocs_extension.cpp
	vector_proxy.cpp
	column.cpp
	converters.cpp
	variant.cpp
  parse_num.cpp
  comparison_functions.cpp
  variant_operators.cpp
	xls/src/endian.c
	xls/src/ole.c
	xls/src/xls.c
	xls/src/xlstool.c
	xls/xlsx/xlsxio_read.c
	xls/read_xls.cpp
	xls/read_xlsx.cpp
	xls/xlscommon.cpp
	xls/zip_memory.cpp
	csv_reader.cpp
	file_reader.cpp
	inferrer.cpp
	ingest.cpp
	ingest_schema.cpp
	json_reader.cpp
	type_conv.cpp
	utility.cpp
	wkt.cpp
	xls_reader.cpp
	xml_reader.cpp
	zip_reader.cpp
	${GEO_EXTENSION_FILES})

add_library(datadocs_extension STATIC ${DATADOCS_EXTENSION_FILES})

# RapidJSON
# The config logic in this if statement is used to build DuckDB with this extension 
#   in a standalone directory. (In another words, without DuckDB WASM)
if(NOT EXISTS "${CMAKE_CURRENT_LIST_DIR}/../../../rapidjson/include")
  message(WARNING "CMake will download RapidJSON from the internet by the config file: contrib/rapidjson/CMakeLists.txt")
  include(contrib/rapidjson/CMakeLists.txt)
  add_dependencies(datadocs_extension rapidjson)
endif()
add_definitions(-DRAPIDJSON_HAS_STDSTRING=1)

target_include_directories(datadocs_extension
	PRIVATE include
	PRIVATE geo/include
	PRIVATE ../json/include
	PRIVATE ../json/yyjson/include
  PRIVATE ../../../rapidjson/include
  PRIVATE ${CMAKE_BINARY_DIR}/include
	# Boost includes
	PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/boost
	# libexpat includes
	PRIVATE $<TARGET_PROPERTY:expat,INTERFACE_INCLUDE_DIRECTORIES>
	# Privately includes icu
	PRIVATE $<TARGET_PROPERTY:icu,INTERFACE_INCLUDE_DIRECTORIES>)

# Boost system wide includes (OPTIONAL)
if(Boost_FOUND AND DEFINED Boost_INCLUDE_DIRS)
  target_include_directories(datadocs_extension PRIVATE ${Boost_INCLUDE_DIRS})
endif()

target_link_libraries(datadocs_extension
        PRIVATE zlib
        PRIVATE expat
        # Publicly link to icu
        PRIVATE icu)

if(WIN32)
  target_compile_definitions(datadocs_extension PUBLIC LIBXML_STATIC)
endif()

install(
  TARGETS datadocs_extension zlib expat icu
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
