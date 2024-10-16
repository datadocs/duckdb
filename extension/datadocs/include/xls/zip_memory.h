#ifndef ZIP_MEMORY_H
#define ZIP_MEMORY_H

#include <contrib/minizip/unzip.h>

#include "file_reader.h"
#include "xlscommon.h"

namespace xls {

unzFile unzOpenMemory(MemBuffer* buffer);
unzFile unzOpenFS(duckdb::BaseReader *reader);

}

#endif