# usage:
#	target_link_libraries(xxx rapidjson)

cmake_minimum_required(VERSION 3.6)
project(rapidjson)

include(ExternalProject)
ExternalProject_Add(rapidjson-download
  GIT_REPOSITORY    https://github.com/Tencent/rapidjson.git
  #GIT_TAG           v1.1.0
  GIT_SHALLOW       true 
  UPDATE_COMMAND    ""
  CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}" -DRAPIDJSON_BUILD_DOC=OFF -DRAPIDJSON_BUILD_EXAMPLES=OFF -DRAPIDJSON_BUILD_TESTS=OFF
)

add_library(rapidjson INTERFACE IMPORTED GLOBAL)
add_dependencies(rapidjson rapidjson-download)
