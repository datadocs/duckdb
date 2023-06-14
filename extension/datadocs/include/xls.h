/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 *
 * Copyright 2004 Komarov Valery
 * Copyright 2006 Christophe Leitienne
 * Copyright 2008-2017 David Hoerl
 * Copyright 2013-2018 Evan Miller
 *
 * This file is part of libxls -- A multiplatform, C/C++ library for parsing
 * Excel(TM) files.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY Evan Miller ''AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#ifndef XLS_INCLUDE
#define XLS_INCLUDE

#include "libxls/xlstypes.h"
#include "libxls/xlsstruct.h"
#include "libxls/xlstool.h"

#ifdef __cplusplus
namespace libxls {
extern "C" {
#endif

typedef enum {
    LIBXLS_OK,
    LIBXLS_ERROR_OPEN,
    LIBXLS_ERROR_SEEK,
    LIBXLS_ERROR_READ,
    LIBXLS_ERROR_PARSE,
    LIBXLS_ERROR_MALLOC
} xls_error_t;

const char* xls_getVersion(void);
const char* xls_getError(xls_error_t code);

int xls(int debug);	// Set debug. Force library to load?
void xls_set_formula_hander(xls_formula_handler handler);

xls_error_t xls_parseWorkBook(xlsWorkBook* pWB);
xls_error_t xls_parseWorkSheet(xlsWorkSheet* pWS);

#ifdef __cplusplus
} // extern c block
} // namespace
#endif

#endif

