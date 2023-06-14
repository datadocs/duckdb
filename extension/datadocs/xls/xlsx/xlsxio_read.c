#define XML_STATIC

#include "xlsxio_private.h"
#include "xlsxio_read.h"
#include "xlsxio_version.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <string.h>

#if defined(_MSC_VER)
#  undef DLL_EXPORT_XLSXIO
#  define DLL_EXPORT_XLSXIO
#endif

#define PARSE_BUFFER_SIZE 256
//#define PARSE_BUFFER_SIZE 4

static const XLSXIOCHAR* xlsx_content_type = X("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml");
static const XLSXIOCHAR* xlsm_content_type = X("application/vnd.ms-excel.sheet.macroEnabled.main+xml");
static const XLSXIOCHAR* xltx_content_type = X("application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml");
static const XLSXIOCHAR* xltm_content_type = X("application/vnd.ms-excel.template.macroEnabled.main+xml");

//UTF-8 version
#define XML_Char_dupchar strdup

ZIPFILEENTRYTYPE* XML_Char_openzip (ZIPFILETYPE* archive, const XML_Char* filename, int flags)
{
  if (!filename || !*filename)
    return NULL;
#ifdef USE_MINIZIP
  if (unzLocateFile(archive, filename, 0) != UNZ_OK)
    return NULL;
  if (unzOpenCurrentFile(archive) != UNZ_OK)
    return NULL;
  return archive;
#else
  return zip_fopen(archive, filename, flags);
#endif
}

DLL_EXPORT_XLSXIO void xlsxioread_get_version (int* pmajor, int* pminor, int* pmicro)
{
  if (pmajor)
    *pmajor = XLSXIO_VERSION_MAJOR;
  if (pminor)
    *pminor = XLSXIO_VERSION_MINOR;
  if (pmicro)
    *pmicro = XLSXIO_VERSION_MICRO;
}

DLL_EXPORT_XLSXIO const XLSXIOCHAR* xlsxioread_get_version_string ()
{
  return (const XLSXIOCHAR*)XLSXIO_VERSION_STRING;
}

////////////////////////////////////////////////////////////////////////

//process XML file contents
int expat_process_zip_file (ZIPFILETYPE* zip, const XML_Char* filename, XML_StartElementHandler start_handler, XML_EndElementHandler end_handler, XML_CharacterDataHandler data_handler, void* callbackdata, XML_Parser* xmlparser)
{
  ZIPFILEENTRYTYPE* zipfile;
  XML_Parser parser;
  void* buf;
#ifdef USE_MINIZIP
  int buflen;
#else
  zip_int64_t buflen;
#endif
  int done;
  enum XML_Status status = XML_STATUS_ERROR;
  if ((zipfile = XML_Char_openzip(zip, filename, 0)) == NULL) {
    return -1;
  }
  parser = XML_ParserCreate(NULL);
  XML_SetUserData(parser, callbackdata);
  XML_SetElementHandler(parser, start_handler, end_handler);
  XML_SetCharacterDataHandler(parser, data_handler);
  if (xmlparser)
    *xmlparser = parser;
  buf = XML_GetBuffer(parser, PARSE_BUFFER_SIZE);
#ifdef USE_MINIZIP
    while (buf && (buflen = unzReadCurrentFile(zip, buf, PARSE_BUFFER_SIZE)) >= 0) {
#else
    while (buf && (buflen = zip_fread(zipfile, buf, PARSE_BUFFER_SIZE)) >= 0) {
#endif
      done = buflen < PARSE_BUFFER_SIZE;
      if ((status = XML_ParseBuffer(parser, (int)buflen, (done ? 1 : 0))) == XML_STATUS_ERROR) {
        break;
      }
      if (xmlparser && status == XML_STATUS_SUSPENDED)
        return 0;
      if (done)
        break;
      buf = XML_GetBuffer(parser, PARSE_BUFFER_SIZE);
    }
  XML_ParserFree(parser);
#ifdef USE_MINIZIP
  unzCloseCurrentFile(zip);
#else
  zip_fclose(zipfile);
#endif
  //return (status == XML_STATUS_ERROR != XML_ERROR_FINISHED ? 1 : 0);
  return 0;
}

enum XML_Status expat_process_zip_file_resume (ZIPFILEENTRYTYPE* zipfile, XML_Parser xmlparser)
{
  enum XML_Status status;
  status = XML_ResumeParser(xmlparser);
  if (status == XML_STATUS_SUSPENDED)
    return status;
  if (status == XML_STATUS_ERROR && XML_GetErrorCode(xmlparser) != XML_ERROR_NOT_SUSPENDED)
    return status;
  void* buf;
#ifdef USE_MINIZIP
  int buflen;
#else
  zip_int64_t buflen;
#endif
  int done;
  buf = XML_GetBuffer(xmlparser, PARSE_BUFFER_SIZE);
#ifdef USE_MINIZIP
  while (buf && (buflen = unzReadCurrentFile(zipfile, buf, PARSE_BUFFER_SIZE)) >= 0) {
#else
  while (buf && (buflen = zip_fread(zipfile, buf, PARSE_BUFFER_SIZE)) >= 0) {
#endif
    done = buflen < PARSE_BUFFER_SIZE;
    if ((status = XML_ParseBuffer(xmlparser, (int)buflen, (done ? 1 : 0))) == XML_STATUS_ERROR)
      return status;
    if (status == XML_STATUS_SUSPENDED)
      return status;
    if (done)
      break;
    buf = XML_GetBuffer(xmlparser, PARSE_BUFFER_SIZE);
  }
  //XML_ParserFree(xmlparser);
  return status;
}

//compare XML name ignoring case and ignoring namespace (returns 0 on match)
#ifdef ASSUME_NO_NAMESPACE
#define XML_Char_icmp_ins XML_Char_icmp
#else
int XML_Char_icmp_ins (const XML_Char* value, const XML_Char* name)
{
  size_t valuelen = XML_Char_len(value);
  size_t namelen = XML_Char_len(name);
  if (valuelen == namelen)
    return XML_Char_icmp(value, name);
  if (valuelen > namelen) {
    if (value[valuelen - namelen - 1] != ':')
      return 1;
    return XML_Char_icmp(value + (valuelen - namelen), name);
  }
  return -1;
}
#endif

//get expat attribute by name, returns NULL if not found
const XML_Char* get_expat_attr_by_name (const XML_Char** atts, const XML_Char* name)
{
  const XML_Char** p = atts;
  if (p) {
    while (*p) {
      //if (XML_Char_icmp(*p++, name) == 0)
      if (XML_Char_icmp_ins(*p++, name) == 0)
        return *p;
      p++;
    }
  }
  return NULL;
}

//generate .rels filename, returns NULL on error, caller must free result
XML_Char* get_relationship_filename (const XML_Char* filename)
{
  XML_Char* result;
  size_t filenamelen = XML_Char_len(filename);
  if ((result = XML_Char_malloc(filenamelen + 12)) != NULL) {
    size_t i = filenamelen;
    while (i > 0) {
      if (filename[i - 1] == '/')
        break;
      i--;
    }
    XML_Char_poscpy(result, 0, filename, i);
    XML_Char_poscpy(result, i, X("_rels/"), 6);
    XML_Char_poscpy(result, i + 6, filename + i, filenamelen - i);
    XML_Char_poscpy(result, filenamelen + 6, X(".rels"), 6);
  }
  return result;
}

//join basepath and filename (caller must free result)
XML_Char* join_basepath_filename (const XML_Char* basepath, const XML_Char* filename)
{
  XML_Char* result = NULL;
  if (filename && *filename) {
    if (filename[0] == '/' && filename[1]) {
      //file is absolute: remove leading slash
      result = XML_Char_dup(filename + 1);
    } else {
      //file is relative: prepend base path
      size_t basepathlen = (basepath ? XML_Char_len(basepath) : 0);
      size_t filenamelen = XML_Char_len(filename);
      if ((result = XML_Char_malloc(basepathlen + filenamelen + 1)) != NULL) {
        if (basepathlen > 0)
          XML_Char_poscpy(result, 0, basepath, basepathlen);
        XML_Char_poscpy(result, basepathlen, filename, filenamelen);
        result[basepathlen + filenamelen] = 0;
      }
    }
  }
  return result;
}

//determine column number based on cell coordinate (e.g. "A1"), returns 1-based column number or 0 on error
size_t get_col_nr (const XML_Char* A1col)
{
  const XML_Char* p = A1col;
  size_t result = 0;
  if (p) {
    while (*p) {
      if (*p >= 'A' && *p <= 'Z')
        result = result * 26 + (*p - 'A') + 1;
      else if (*p >= 'a' && *p <= 'z')
        result = result * 26 + (*p - 'a') + 1;
      else if (*p >= '0' && *p <= '9' && p != A1col)
        return result;
      else
        break;
      p++;
    }
  }
  return 0;
}

//determine row number based on cell coordinate (e.g. "A1"), returns 1-based row number or 0 on error
size_t get_row_nr (const XML_Char* A1col)
{
  const XML_Char* p = A1col;
  size_t result = 0;
  if (p) {
    while (*p) {
      if ((*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z'))
        ;
      else if (*p >= '0' && *p <= '9' && p != A1col)
        result = result * 10 + (*p - '0');
      else
        return 0;
      p++;
    }
  }
  return result;
}

////////////////////////////////////////////////////////////////////////

//callback function definition for use with iterate_files_by_contenttype
typedef void (*contenttype_file_callback_fn)(ZIPFILETYPE* zip, const XML_Char* filename, const XML_Char* contenttype, void* callbackdata);

struct iterate_files_by_contenttype_callback_data {
  ZIPFILETYPE* zip;
  const XML_Char* contenttype;
  contenttype_file_callback_fn filecallbackfn;
  void* filecallbackdata;
};

//expat callback function for element start used by iterate_files_by_contenttype
void iterate_files_by_contenttype_expat_callback_element_start (void* callbackdata, const XML_Char* name, const XML_Char** atts)
{
  struct iterate_files_by_contenttype_callback_data* data = (struct iterate_files_by_contenttype_callback_data*)callbackdata;
  if (XML_Char_icmp_ins(name, X("Override")) == 0) {
    //explicitly specified file
    const XML_Char* contenttype;
    const XML_Char* partname;
    if ((contenttype = get_expat_attr_by_name(atts, X("ContentType"))) != NULL && XML_Char_icmp(contenttype, data->contenttype) == 0) {
      if ((partname = get_expat_attr_by_name(atts, X("PartName"))) != NULL) {
        if (partname[0] == '/')
          partname++;
        data->filecallbackfn(data->zip, partname, contenttype, data->filecallbackdata);
      }
    }
  } else if (XML_Char_icmp_ins(name, X("Default")) == 0) {
    //by extension
    const XML_Char* contenttype;
    const XML_Char* extension;
    if ((contenttype = get_expat_attr_by_name(atts, X("ContentType"))) != NULL && XML_Char_icmp(contenttype, data->contenttype) == 0) {
      if ((extension = get_expat_attr_by_name(atts, X("Extension"))) != NULL) {
        XML_Char* filename;
        size_t filenamelen;
        size_t extensionlen = XML_Char_len(extension);
#ifdef USE_MINIZIP
#define UNZIP_FILENAME_BUFFER_STEP 32
        char* buf;
        size_t buflen;
        int status;
unz_global_info zipglobalinfo;
unzGetGlobalInfo(data->zip, &zipglobalinfo);
        buf = (char*)malloc(buflen = UNZIP_FILENAME_BUFFER_STEP);
        status = unzGoToFirstFile(data->zip);
        while (status == UNZ_OK) {
          buf[buflen - 1] = 0;
          while ((status = unzGetCurrentFileInfo(data->zip, NULL, buf, buflen, NULL, 0, NULL, 0)) == UNZ_OK && buf[buflen - 1] != 0) {
            buflen += UNZIP_FILENAME_BUFFER_STEP;
            buf = (char*)realloc(buf, buflen);
            buf[buflen - 1] = 0;
          }
          if (status != UNZ_OK)
            break;
          filename = XML_Char_dupchar(buf);
          status = unzGoToNextFile(data->zip);
#else
        zip_int64_t i;
        zip_int64_t zipnumfiles = zip_get_num_entries(data->zip, 0);
        for (i = 0; i < zipnumfiles; i++) {
          filename = XML_Char_dupchar(zip_get_name(data->zip, i, ZIP_FL_ENC_GUESS));
#endif
          filenamelen = XML_Char_len(filename);
          if (filenamelen > extensionlen && filename[filenamelen - extensionlen - 1] == '.' && XML_Char_icmp(filename + filenamelen - extensionlen, extension) == 0) {
            data->filecallbackfn(data->zip, filename, contenttype, data->filecallbackdata);
          }
          free(filename);
        }
#ifdef USE_MINIZIP
        free(buf);
#endif
      }
    }
  }
}

//list file names by content type
int iterate_files_by_contenttype (ZIPFILETYPE* zip, const XML_Char* contenttype, contenttype_file_callback_fn filecallbackfn, void* filecallbackdata, XML_Parser* xmlparser)
{
  struct iterate_files_by_contenttype_callback_data callbackdata = {
    .zip = zip,
    .contenttype = contenttype,
    .filecallbackfn = filecallbackfn,
    .filecallbackdata = filecallbackdata
  };
  return expat_process_zip_file(zip, X("[Content_Types].xml"), iterate_files_by_contenttype_expat_callback_element_start, NULL, NULL, &callbackdata, xmlparser);
}

////////////////////////////////////////////////////////////////////////

//determine relationship id for specific sheet name
void main_sheet_get_relid_expat_callback_element_start (void* callbackdata, const XML_Char* name, const XML_Char** atts)
{
  struct main_sheet_get_rels_callback_data* data = (struct main_sheet_get_rels_callback_data*)callbackdata;
  if (XML_Char_icmp_ins(name, X("sheet")) == 0) {
    const XML_Char* sheetname = get_expat_attr_by_name(atts, X("name"));
    if (!data->sheetname || XML_Char_icmp(sheetname, data->sheetname) == 0) {
      const XML_Char* relid = get_expat_attr_by_name(atts, X("r:id"));
      if (relid && *relid) {
        data->sheetrelid = XML_Char_dup(relid);
        XML_StopParser(data->xmlparser, XML_FALSE);
        return;
      }
    }
  }
}

//determine file names for specific relationship id
void main_sheet_get_sheetfile_expat_callback_element_start (void* callbackdata, const XML_Char* name, const XML_Char** atts)
{
  struct main_sheet_get_rels_callback_data* data = (struct main_sheet_get_rels_callback_data*)callbackdata;
  if (data->sheetrelid) {
    if (XML_Char_icmp_ins(name, X("Relationship")) == 0) {
      const XML_Char* reltype;
      if ((reltype = get_expat_attr_by_name(atts, X("Type"))) != NULL) {
        if (XML_Char_icmp(reltype, X("http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet")) == 0) {
          const XML_Char* relid = get_expat_attr_by_name(atts, X("Id"));
          if (XML_Char_icmp(relid, data->sheetrelid) == 0) {
            const XML_Char* filename = get_expat_attr_by_name(atts, X("Target"));
            if (filename && *filename) {
              data->sheetfile = join_basepath_filename(data->basepath, filename);
            }
          }
        } else if (XML_Char_icmp(reltype, X("http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings")) == 0) {
          const XML_Char* filename = get_expat_attr_by_name(atts, X("Target"));
          if (filename && *filename) {
            data->sharedstringsfile = join_basepath_filename(data->basepath, filename);
          }
        } else if (XML_Char_icmp(reltype, X("http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles")) == 0) {
          const XML_Char* filename = get_expat_attr_by_name(atts, X("Target"));
          if (filename && *filename) {
            data->stylesfile = join_basepath_filename(data->basepath, filename);
          }
        }
      }
    }
  }
}

//determine the file name for a specified sheet name
void main_sheet_get_sheetfile_callback (ZIPFILETYPE* zip, const XML_Char* filename, const XML_Char* contenttype, void* callbackdata)
{
  struct main_sheet_get_rels_callback_data* data = (struct main_sheet_get_rels_callback_data*)callbackdata;
  if (!data->sheetrelid) {
    expat_process_zip_file(zip, filename, main_sheet_get_relid_expat_callback_element_start, NULL, NULL, callbackdata, &data->xmlparser);
  }
  if (data->sheetrelid) {
    XML_Char* relfilename;
    //determine base name (including trailing slash)
    size_t i = XML_Char_len(filename);
    while (i > 0) {
      if (filename[i - 1] == '/')
        break;
      i--;
    }
    if (data->basepath)
      free(data->basepath);
    if ((data->basepath = XML_Char_malloc(i + 1)) != NULL) {
      XML_Char_poscpy(data->basepath, 0, filename, i);
      data->basepath[i] = 0;
    }
    //find sheet filename in relationship contents
    if ((relfilename = get_relationship_filename(filename)) != NULL) {
      expat_process_zip_file(zip, relfilename, main_sheet_get_sheetfile_expat_callback_element_start, NULL, NULL, callbackdata, &data->xmlparser);
      free(relfilename);
    } else {
      free(data->sheetrelid);
      data->sheetrelid = NULL;
      if (data->basepath) {
        free(data->basepath);
        data->basepath = NULL;
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////

void xlsxioread_preprocess (xlsxioreader handle, struct main_sheet_get_rels_callback_data* getrelscallbackdata)
{
  iterate_files_by_contenttype(handle->zip, xlsx_content_type, main_sheet_get_sheetfile_callback, getrelscallbackdata, NULL);
  if (!getrelscallbackdata->sheetrelid)
    iterate_files_by_contenttype(handle->zip, xlsm_content_type, main_sheet_get_sheetfile_callback, getrelscallbackdata, NULL);
  if (!getrelscallbackdata->sheetrelid)
    iterate_files_by_contenttype(handle->zip, xltx_content_type, main_sheet_get_sheetfile_callback, getrelscallbackdata, NULL);
  if (!getrelscallbackdata->sheetrelid)
    iterate_files_by_contenttype(handle->zip, xltm_content_type, main_sheet_get_sheetfile_callback, getrelscallbackdata, NULL);
}

////////////////////////////////////////////////////////////////////////

void xlsxioread_find_main_sheet_file_callback (ZIPFILETYPE* zip, const XML_Char* filename, const XML_Char* contenttype, void* callbackdata)
{
  XML_Char** data = (XML_Char**)callbackdata;
  *data = XML_Char_dup(filename);
}

XML_Char* xlsxioread_workbook_file (xlsxioreader handle)
{
  //determine main sheet name
  XML_Char* mainsheetfile = NULL;
  iterate_files_by_contenttype(handle->zip, xlsx_content_type, xlsxioread_find_main_sheet_file_callback, &mainsheetfile, NULL);
  if (!mainsheetfile)
    iterate_files_by_contenttype(handle->zip, xlsm_content_type, xlsxioread_find_main_sheet_file_callback, &mainsheetfile, NULL);
  if (!mainsheetfile)
    iterate_files_by_contenttype(handle->zip, xltx_content_type, xlsxioread_find_main_sheet_file_callback, &mainsheetfile, NULL);
  if (!mainsheetfile)
    iterate_files_by_contenttype(handle->zip, xltm_content_type, xlsxioread_find_main_sheet_file_callback, &mainsheetfile, NULL);
  return mainsheetfile;
}
