#include "sqlite_duckdb_vfs.hpp"
#include "sqlite_utils.hpp"

#include "duckdb/common/file_open_flags.hpp"

#include <cstring>

namespace duckdb {
namespace {

//! A sqlite3_file backed by a duckdb::FileHandle. `base` must stay first so we
//! can cast a sqlite3_file* to this and back.
struct DuckDBSQLiteFile {
	sqlite3_file base;
	FileHandle *handle; // owned; closed + deleted in xClose
	FileSystem *fs;
};

int VfsRead(sqlite3_file *file_ptr, void *buf, int amt, sqlite3_int64 ofst) {
	auto *f = reinterpret_cast<DuckDBSQLiteFile *>(file_ptr);
	try {
		int64_t size = f->fs->GetFileSize(*f->handle);
		if (ofst >= size) {
			std::memset(buf, 0, (size_t)amt);
			return SQLITE_IOERR_SHORT_READ;
		}
		int64_t avail = size - ofst;
		int64_t to_read = avail < amt ? avail : (int64_t)amt;
		if (to_read > 0) {
			f->fs->Read(*f->handle, buf, to_read, (idx_t)ofst);
		}
		if (to_read < amt) {
			// SQLite requires the tail of a short read to be zero-filled.
			std::memset(static_cast<char *>(buf) + to_read, 0, (size_t)(amt - to_read));
			return SQLITE_IOERR_SHORT_READ;
		}
		return SQLITE_OK;
	} catch (...) {
		return SQLITE_IOERR_READ;
	}
}

int VfsWrite(sqlite3_file *file_ptr, const void *buf, int amt, sqlite3_int64 ofst) {
	auto *f = reinterpret_cast<DuckDBSQLiteFile *>(file_ptr);
	try {
		f->fs->Write(*f->handle, const_cast<void *>(buf), (int64_t)amt, (idx_t)ofst);
		return SQLITE_OK;
	} catch (...) {
		return SQLITE_IOERR_WRITE;
	}
}

int VfsTruncate(sqlite3_file *file_ptr, sqlite3_int64 size) {
	auto *f = reinterpret_cast<DuckDBSQLiteFile *>(file_ptr);
	try {
		f->fs->Truncate(*f->handle, (int64_t)size);
		return SQLITE_OK;
	} catch (...) {
		return SQLITE_IOERR_TRUNCATE;
	}
}

int VfsSync(sqlite3_file *file_ptr, int) {
	auto *f = reinterpret_cast<DuckDBSQLiteFile *>(file_ptr);
	try {
		f->fs->FileSync(*f->handle);
		return SQLITE_OK;
	} catch (...) {
		return SQLITE_IOERR_FSYNC;
	}
}

int VfsFileSize(sqlite3_file *file_ptr, sqlite3_int64 *out) {
	auto *f = reinterpret_cast<DuckDBSQLiteFile *>(file_ptr);
	try {
		*out = (sqlite3_int64)f->fs->GetFileSize(*f->handle);
		return SQLITE_OK;
	} catch (...) {
		return SQLITE_IOERR_FSTAT;
	}
}

int VfsClose(sqlite3_file *file_ptr) {
	auto *f = reinterpret_cast<DuckDBSQLiteFile *>(file_ptr);
	if (f->handle) {
		try {
			f->handle->Close();
		} catch (...) {
		}
		delete f->handle;
		f->handle = nullptr;
	}
	return SQLITE_OK;
}

// Single SQLite connection per wasm worker → locking is a no-op.
int VfsLock(sqlite3_file *, int) {
	return SQLITE_OK;
}
int VfsUnlock(sqlite3_file *, int) {
	return SQLITE_OK;
}
int VfsCheckReservedLock(sqlite3_file *, int *out) {
	*out = 0;
	return SQLITE_OK;
}
int VfsFileControl(sqlite3_file *, int, void *) {
	return SQLITE_NOTFOUND;
}
int VfsSectorSize(sqlite3_file *) {
	return 4096;
}
int VfsDeviceCharacteristics(sqlite3_file *) {
	return 0;
}

const sqlite3_io_methods g_io_methods = {
    1,                          // iVersion
    VfsClose,                   // xClose
    VfsRead,                    // xRead
    VfsWrite,                   // xWrite
    VfsTruncate,                // xTruncate
    VfsSync,                    // xSync
    VfsFileSize,                // xFileSize
    VfsLock,                    // xLock
    VfsUnlock,                  // xUnlock
    VfsCheckReservedLock,       // xCheckReservedLock
    VfsFileControl,             // xFileControl
    VfsSectorSize,              // xSectorSize
    VfsDeviceCharacteristics,   // xDeviceCharacteristics
    nullptr, nullptr, nullptr,  // xShmMap, xShmLock, xShmBarrier (iVersion>=2)
    nullptr,                    // xShmUnmap
    nullptr, nullptr,           // xFetch, xUnfetch (iVersion>=3)
};

FileOpenFlags TranslateFlags(int sqlite_flags) {
	FileOpenFlags f = (sqlite_flags & SQLITE_OPEN_READONLY)
	                      ? FileFlags::FILE_FLAGS_READ
	                      : (FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE);
	if (sqlite_flags & SQLITE_OPEN_CREATE) {
		f |= FileFlags::FILE_FLAGS_FILE_CREATE;
	}
	return f;
}

int VfsOpen(sqlite3_vfs *vfs, const char *name, sqlite3_file *file_ptr, int flags, int *out_flags) {
	auto *f = reinterpret_cast<DuckDBSQLiteFile *>(file_ptr);
	std::memset(f, 0, sizeof(DuckDBSQLiteFile));
	auto *fs = static_cast<FileSystem *>(vfs->pAppData);
	if (!fs || !name) {
		return SQLITE_CANTOPEN;
	}
	try {
		auto handle = fs->OpenFile(string(name), TranslateFlags(flags));
		if (!handle) {
			return SQLITE_CANTOPEN;
		}
		f->base.pMethods = &g_io_methods;
		f->handle = handle.release();
		f->fs = fs;
		if (out_flags) {
			*out_flags = flags;
		}
		return SQLITE_OK;
	} catch (...) {
		return SQLITE_CANTOPEN;
	}
}

int VfsDelete(sqlite3_vfs *vfs, const char *name, int) {
	auto *fs = static_cast<FileSystem *>(vfs->pAppData);
	if (!fs || !name) {
		return SQLITE_OK;
	}
	try {
		if (fs->FileExists(string(name))) {
			fs->RemoveFile(string(name));
		}
	} catch (...) {
	}
	return SQLITE_OK;
}

int VfsAccess(sqlite3_vfs *vfs, const char *name, int, int *out) {
	auto *fs = static_cast<FileSystem *>(vfs->pAppData);
	*out = 0;
	if (!fs || !name) {
		return SQLITE_OK;
	}
	try {
		*out = fs->FileExists(string(name)) ? 1 : 0;
	} catch (...) {
		*out = 0;
	}
	return SQLITE_OK;
}

int VfsFullPathname(sqlite3_vfs *, const char *name, int n_out, char *out) {
	// OPFS paths are already the canonical names files are registered under.
	size_t len = std::strlen(name);
	if ((int)len >= n_out) {
		len = (size_t)(n_out - 1);
	}
	std::memcpy(out, name, len);
	out[len] = '\0';
	return SQLITE_OK;
}

// Delegate the non-file VFS bits to the platform default VFS.
int VfsRandomness(sqlite3_vfs *, int n, char *out) {
	sqlite3_vfs *def = sqlite3_vfs_find(nullptr);
	if (def && def->xRandomness) {
		return def->xRandomness(def, n, out);
	}
	std::memset(out, 0, (size_t)n);
	return n;
}
int VfsSleep(sqlite3_vfs *, int micros) {
	sqlite3_vfs *def = sqlite3_vfs_find(nullptr);
	if (def && def->xSleep) {
		return def->xSleep(def, micros);
	}
	return 0;
}
int VfsCurrentTime(sqlite3_vfs *, double *out) {
	sqlite3_vfs *def = sqlite3_vfs_find(nullptr);
	if (def && def->xCurrentTime) {
		return def->xCurrentTime(def, out);
	}
	*out = 2440587.5; // unix epoch as a Julian day number
	return SQLITE_OK;
}
int VfsGetLastError(sqlite3_vfs *, int, char *) {
	return 0;
}

sqlite3_vfs g_vfs;
bool g_registered = false;

} // namespace

const char *DuckDBSQLiteVFSName() {
	return "duckdb_fs";
}

void RegisterDuckDBSQLiteVFS(FileSystem &fs) {
	if (g_registered) {
		// One DuckDB instance per wasm worker in practice; keep the latest fs.
		g_vfs.pAppData = &fs;
		return;
	}
	std::memset(&g_vfs, 0, sizeof(g_vfs));
	g_vfs.iVersion = 1;
	g_vfs.szOsFile = sizeof(DuckDBSQLiteFile);
	g_vfs.mxPathname = 1024;
	g_vfs.zName = DuckDBSQLiteVFSName();
	g_vfs.pAppData = &fs;
	g_vfs.xOpen = VfsOpen;
	g_vfs.xDelete = VfsDelete;
	g_vfs.xAccess = VfsAccess;
	g_vfs.xFullPathname = VfsFullPathname;
	g_vfs.xRandomness = VfsRandomness;
	g_vfs.xSleep = VfsSleep;
	g_vfs.xCurrentTime = VfsCurrentTime;
	g_vfs.xGetLastError = VfsGetLastError;
	sqlite3_vfs_register(&g_vfs, 0 /* do not make default */);
	g_registered = true;
}

} // namespace duckdb
