#include "xls/zip_memory.h"

#include <cstring>

// https://github.com/zlib-ng/minizip-ng/blob/master/mz_compat.c

namespace xls {

static voidpf ZCALLBACK zm_open_file(voidpf opaque, const char* filename, int mode)
{
	if (mode != (ZLIB_FILEFUNC_MODE_READ | ZLIB_FILEFUNC_MODE_EXISTING))
		return nullptr;
	((MemBuffer*)opaque)->pos = 0;
	return opaque;
}

long ZCALLBACK zm_seek(voidpf opaque, voidpf stream, uLong offset, int origin) {
	MemBuffer *mem = (MemBuffer *)stream;
	long new_pos;
	switch (origin) {
	case ZLIB_FILEFUNC_SEEK_CUR:
		new_pos = mem->pos + offset;
		break;
	case ZLIB_FILEFUNC_SEEK_END:
		new_pos = mem->size - offset;
		break;
	case ZLIB_FILEFUNC_SEEK_SET:
		new_pos = offset;
		break;
	default:
		return -1;
	}
	if (new_pos < 0 || new_pos > mem->size)
		return 1;
	mem->pos = (size_t) new_pos;
	return 0;
}

long ZCALLBACK zm_tell(voidpf opaque, voidpf stream)
{
	return ((MemBuffer*)opaque)->pos;
}

uLong ZCALLBACK zm_read(voidpf opaque, voidpf stream, void* buf, uLong size)
{
	MemBuffer* mem = (MemBuffer*)stream;
	if (size > mem->size - mem->pos)
		size = mem->size - mem->pos;
	std::memcpy(buf, mem->buffer + mem->pos, size);
	mem->pos += size;
	return size;
}

int ZCALLBACK zm_close(voidpf opaque, voidpf stream)
{
	((MemBuffer*)stream)->clear();
	return 0;
}

int ZCALLBACK zm_error(voidpf opaque, voidpf stream)
{
	return 0;
}

unzFile unzOpenMemory(MemBuffer* buffer)
{
	if (!buffer->buffer || buffer->size == 0)
		return nullptr;
	zlib_filefunc_def filefunc32 = {};
	filefunc32.zopen_file = zm_open_file;
	filefunc32.zread_file = zm_read;
	filefunc32.ztell_file = zm_tell;
	filefunc32.zseek_file = zm_seek;
	filefunc32.zclose_file = zm_close;
	filefunc32.zerror_file = zm_error;
	filefunc32.opaque = (void*)buffer;
	return unzOpen2(nullptr, &filefunc32);
}

static voidpf ZCALLBACK zf_open_file(voidpf opaque, const char* filename, int mode)
{
	if (mode != (ZLIB_FILEFUNC_MODE_READ | ZLIB_FILEFUNC_MODE_EXISTING))
		return nullptr;
	if (!((duckdb::FileReader*)opaque)->open())
		return nullptr;
	return ((duckdb::FileReader*)opaque)->file_handle.get();
}

long ZCALLBACK zf_seek(voidpf opaque, voidpf stream, uLong offset, int origin) {
	auto reader = (duckdb::FileReader *)opaque;
	// auto handle = (duckdb::FileHandle*)stream;
	long new_pos;
	switch (origin) {
	case ZLIB_FILEFUNC_SEEK_CUR:
		new_pos = reader->tell() + offset;
		break;
	case ZLIB_FILEFUNC_SEEK_END:
		new_pos = reader->filesize() - offset;
		break;
	case ZLIB_FILEFUNC_SEEK_SET:
		new_pos = offset;
		break;
	default:
		debug_file_io("zf_seek(offset=%zu, origin=%d) ERROR: unknown origin", offset, origin);
		return -1;
	}

	debug_file_io("zf_seek(offset=%zu, origin=%d) new_pos=%ld", offset, origin, new_pos);
	if (new_pos < 0 || new_pos > reader->filesize()) {
		debug_file_io("zf_seek ERROR: new_pos is invalid");
		return 1;
	}

	reader->seek((size_t)new_pos);
	return 0;
}

long ZCALLBACK zf_tell(voidpf opaque, voidpf stream) {
	size_t position = ((duckdb::FileReader *)opaque)->tell();
	// auto position = ((duckdb::FileHandle *)stream)->SeekPosition();
	debug_file_io("zf_tell() => %zu", position);
	return position;
}

uLong ZCALLBACK zf_read(voidpf opaque, voidpf stream, void *buf, uLong size) {
	size_t result = ((duckdb::FileReader *)opaque)->read((char *)buf, size);
	// size_t result = ((duckdb::FileHandle *)stream)->Read(buf, size);
	// debug_file_io("zf_read(%zu) [0]=%02x", size, ((unsigned char*)buf)[0]);
	return result;
}

int ZCALLBACK zf_close(voidpf opaque, voidpf stream) {
	((duckdb::FileReader *)opaque)->close();
	// ((duckdb::FileHandle *)stream)->Close();
	return 0;
}

int ZCALLBACK zf_error(voidpf opaque, voidpf stream)
{
	return 0;
}

unzFile unzOpenFS(duckdb::BaseReader *reader)
{
	zlib_filefunc_def filefunc32 = {};
	filefunc32.zopen_file = zf_open_file;
	filefunc32.zread_file = zf_read;
	filefunc32.ztell_file = zf_tell;
	filefunc32.zseek_file = zf_seek;
	filefunc32.zclose_file = zf_close;
	filefunc32.zerror_file = zf_error;
	filefunc32.opaque = (void*)reader;
	return unzOpen2(nullptr, &filefunc32);
}

} // namespace xls
