#include <cstring>

#include "xls/zip_memory.h"

namespace xls {

static voidpf ZCALLBACK zm_open_file(voidpf opaque, const char* filename, int mode)
{
	if (mode != (ZLIB_FILEFUNC_MODE_READ | ZLIB_FILEFUNC_MODE_EXISTING))
		return nullptr;
	((MemBuffer*)opaque)->pos = 0;
	return opaque;
}

long ZCALLBACK zm_seek(voidpf opaque, voidpf stream, uLong offset, int origin)
{
	MemBuffer* mem = (MemBuffer*)stream;
	uLong new_pos;
	switch (origin)
	{
	case ZLIB_FILEFUNC_SEEK_CUR:
		new_pos = mem->pos + offset;
		break;
	case ZLIB_FILEFUNC_SEEK_END:
		new_pos = mem->size + offset;
		break;
	case ZLIB_FILEFUNC_SEEK_SET:
		new_pos = offset;
		break;
	default:
		return -1;
	}
	if (new_pos > mem->size)
		return 1;
	mem->pos = new_pos;
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

}
