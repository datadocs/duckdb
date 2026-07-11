#include "duckdb.h"
#include "duckdb/common/dl.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/capi/extension_api.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "mbedtls_wrapper.hpp"

#ifndef DUCKDB_NO_THREADS
#include <thread>
#endif // DUCKDB_NO_THREADS

#ifdef WASM_LOADABLE_EXTENSIONS
#include <emscripten.h>
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Extension C API
//===--------------------------------------------------------------------===//

//! State that is kept during the load phase of a C API extension
struct DuckDBExtensionLoadState {
	explicit DuckDBExtensionLoadState(DatabaseInstance &db_p, ExtensionInitResult &init_result_p)
	    : db(db_p), init_result(init_result_p), database_data(nullptr) {
	}

	//! Create a DuckDBExtensionLoadState reference from a C API opaque pointer
	static DuckDBExtensionLoadState &Get(duckdb_extension_info info) {
		D_ASSERT(info);
		return *reinterpret_cast<duckdb::DuckDBExtensionLoadState *>(info);
	}

	//! Convert to an opaque C API pointer
	duckdb_extension_info ToCStruct() {
		return reinterpret_cast<duckdb_extension_info>(this);
	}

	//! Ref to the database being loaded
	DatabaseInstance &db;

	//! The init result from initializing the extension
	ExtensionInitResult &init_result;

	//! This is the duckdb_database struct that will be passed to the extension during initialization. Note that the
	//! extension does not need to free it.
	unique_ptr<DatabaseWrapper> database_data;

	//! The function pointer struct passed to the extension. The extension is expected to copy this struct during
	//! initialization
	duckdb_ext_api_v1 api_struct;

	//! Error handling
	bool has_error = false;
	//! The stored error from the loading process
	ErrorData error_data;
};

//! Contains the callbacks that are passed to CAPI extensions to allow initialization
struct ExtensionAccess {
	//! Create the struct of function pointers to pass to the extension for initialization
	static duckdb_extension_access CreateAccessStruct() {
		return {SetError, GetDatabase, GetAPI};
	}

	//! Called by the extension to indicate failure to initialize the extension
	static void SetError(duckdb_extension_info info, const char *error) {
		auto &load_state = DuckDBExtensionLoadState::Get(info);

		load_state.has_error = true;
		load_state.error_data =
		    error ? ErrorData(error)
		          : ErrorData(ExceptionType::UNKNOWN_TYPE, "Extension has indicated an error occured during "
		                                                   "initialization, but did not set an error message.");
	}

	//! Called by the extension get a pointer to the database that is loading it
	static duckdb_database *GetDatabase(duckdb_extension_info info) {
		auto &load_state = DuckDBExtensionLoadState::Get(info);

		try {
			// Create the duckdb_database
			load_state.database_data = make_uniq<DatabaseWrapper>();
			load_state.database_data->database = make_shared_ptr<DuckDB>(load_state.db);
			return reinterpret_cast<duckdb_database *>(load_state.database_data.get());
		} catch (std::exception &ex) {
			load_state.has_error = true;
			load_state.error_data = ErrorData(ex);
			return nullptr;
		} catch (...) {
			load_state.has_error = true;
			load_state.error_data =
			    ErrorData(ExceptionType::UNKNOWN_TYPE, "Unknown error in GetDatabase when trying to load extension!");
			return nullptr;
		}
	}

	//! Called by the extension get a pointer the correctly versioned extension C API struct.
	static const void *GetAPI(duckdb_extension_info info, const char *version) {
		string version_string = version;
		auto &load_state = DuckDBExtensionLoadState::Get(info);

		if (load_state.init_result.abi_type == ExtensionABIType::C_STRUCT) {
			idx_t major, minor, patch;
			auto parsed = VersioningUtils::ParseSemver(version_string, major, minor, patch);

			if (!parsed || !VersioningUtils::IsSupportedCAPIVersion(major, minor, patch)) {
				load_state.has_error = true;
				load_state.error_data = ErrorData(
				    ExceptionType::UNKNOWN_TYPE,
				    "Unsupported C CAPI version detected during extension initialization: " + string(version));
				return nullptr;
			}
		} else if (load_state.init_result.abi_type == ExtensionABIType::C_STRUCT_UNSTABLE) {
			// NOTE: we currently don't check anything here: the version of extensions of ABI type C_STRUCT_UNSTABLE is
			// ignored because C_STRUCT_UNSTABLE extensions are tied 1:1 to duckdb verions meaning they will always
			// receive the whole function pointer struct
		} else {
			load_state.has_error = true;
			load_state.error_data =
			    ErrorData(ExceptionType::UNKNOWN_TYPE,
			              StringUtil::Format("Unknown ABI Type of value '%d' found when loading extension '%s'",
			                                 static_cast<uint8_t>(load_state.init_result.abi_type),
			                                 load_state.init_result.filename));
			return nullptr;
		}

		load_state.api_struct = load_state.db.GetExtensionAPIV1();
		return &load_state.api_struct;
	}
};

//===--------------------------------------------------------------------===//
// Load External Extension
//===--------------------------------------------------------------------===//
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
// The C++ init function
typedef void (*ext_init_fun_t)(DatabaseInstance &);
// The C init function
typedef bool (*ext_init_c_api_fun_t)(duckdb_extension_info info, duckdb_extension_access *access);
typedef const char *(*ext_version_fun_t)(void);
typedef bool (*ext_is_storage_t)(void);

template <class T>
static T LoadFunctionFromDLL(void *dll, const string &function_name, const string &filename) {
	auto function = dlsym(dll, function_name.c_str());
	if (!function) {
		throw IOException("File \"%s\" did not contain function \"%s\": %s", filename, function_name, GetDLError());
	}
	return (T)function;
}
#endif

template <class T>
static T TryLoadFunctionFromDLL(void *dll, const string &function_name, const string &filename) {
	auto function = dlsym(dll, function_name.c_str());
	if (!function) {
		return nullptr;
	}
	return (T)function;
}

static void ComputeSHA256String(const string &to_hash, string *res) {
	// Invoke MbedTls function to actually compute sha256
	*res = duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(to_hash);
}

static void ComputeSHA256FileSegment(FileHandle *handle, const idx_t start, const idx_t end, string *res) {
	idx_t iter = start;
	const idx_t segment_size = 1024ULL * 8ULL;

	duckdb_mbedtls::MbedTlsWrapper::SHA256State state;

	string to_hash;
	while (iter < end) {
		idx_t len = std::min(end - iter, segment_size);
		to_hash.resize(len);
		handle->Read((void *)to_hash.data(), len, iter);

		state.AddString(to_hash);

		iter += segment_size;
	}

	*res = state.Finalize();
}

static string FilterZeroAtEnd(string s) {
	while (!s.empty() && s.back() == '\0') {
		s.pop_back();
	}
	return s;
}

ParsedExtensionMetaData ExtensionHelper::ParseExtensionMetaData(const char *metadata) noexcept {
	ParsedExtensionMetaData result;

	vector<string> metadata_field;
	for (idx_t i = 0; i < 8; i++) {
		string field = string(metadata + i * 32, 32);
		metadata_field.emplace_back(field);
	}

	std::reverse(metadata_field.begin(), metadata_field.end());

	// Fetch the magic value and early out if this is invalid: the rest will just be bogus
	result.magic_value = FilterZeroAtEnd(metadata_field[0]);
	if (!result.AppearsValid()) {
		return result;
	}

	result.platform = FilterZeroAtEnd(metadata_field[1]);

	result.extension_version = FilterZeroAtEnd(metadata_field[3]);

	auto extension_abi_metadata = FilterZeroAtEnd(metadata_field[4]);

	if (extension_abi_metadata == "C_STRUCT") {
		result.abi_type = ExtensionABIType::C_STRUCT;
		result.duckdb_capi_version = FilterZeroAtEnd(metadata_field[2]);
	} else if (extension_abi_metadata == "C_STRUCT_UNSTABLE") {
		result.abi_type = ExtensionABIType::C_STRUCT_UNSTABLE;
		result.duckdb_version = FilterZeroAtEnd(metadata_field[2]);
	} else if (extension_abi_metadata == "CPP" || extension_abi_metadata.empty()) {
		result.abi_type = ExtensionABIType::CPP;
		result.duckdb_version = FilterZeroAtEnd(metadata_field[2]);
	} else {
		result.abi_type = ExtensionABIType::UNKNOWN;
		result.duckdb_version = "unknown";
		result.extension_abi_metadata = extension_abi_metadata;
	}

	result.signature = string(metadata, ParsedExtensionMetaData::FOOTER_SIZE - ParsedExtensionMetaData::SIGNATURE_SIZE);
	return result;
}

ParsedExtensionMetaData ExtensionHelper::ParseExtensionMetaData(FileHandle &handle) {
	const string engine_version = string(ExtensionHelper::GetVersionDirectoryName());
	const string engine_platform = string(DuckDB::Platform());

	string metadata_segment;
	metadata_segment.resize(ParsedExtensionMetaData::FOOTER_SIZE);

	if (handle.GetFileSize() < ParsedExtensionMetaData::FOOTER_SIZE) {
		throw InvalidInputException(
		    "File '%s' is not a DuckDB extension. Valid DuckDB extensions must be at least %llu bytes", handle.path,
		    ParsedExtensionMetaData::FOOTER_SIZE);
	}

	handle.Read((void *)metadata_segment.data(), metadata_segment.size(),
	            handle.GetFileSize() - ParsedExtensionMetaData::FOOTER_SIZE);

	return ParseExtensionMetaData(metadata_segment.data());
}

bool ExtensionHelper::CheckExtensionSignature(FileHandle &handle, ParsedExtensionMetaData &parsed_metadata,
                                              const bool allow_community_extensions) {
	auto signature_offset = handle.GetFileSize() - ParsedExtensionMetaData::SIGNATURE_SIZE;

	const idx_t maxLenChunks = 1024ULL * 1024ULL;
	const idx_t numChunks = (signature_offset + maxLenChunks - 1) / maxLenChunks;
	vector<string> hash_chunks(numChunks);
	vector<idx_t> splits(numChunks + 1);

	for (idx_t i = 0; i < numChunks; i++) {
		splits[i] = maxLenChunks * i;
	}
	splits.back() = signature_offset;

#ifndef DUCKDB_NO_THREADS
	vector<std::thread> threads;
	threads.reserve(numChunks);
	for (idx_t i = 0; i < numChunks; i++) {
		threads.emplace_back(ComputeSHA256FileSegment, &handle, splits[i], splits[i + 1], &hash_chunks[i]);
	}

	for (auto &thread : threads) {
		thread.join();
	}
#else
	for (idx_t i = 0; i < numChunks; i++) {
		ComputeSHA256FileSegment(&handle, splits[i], splits[i + 1], &hash_chunks[i]);
	}
#endif // DUCKDB_NO_THREADS

	string hash_concatenation;
	hash_concatenation.reserve(32 * numChunks); // 256 bits -> 32 bytes per chunk

	for (auto &hash_chunk : hash_chunks) {
		hash_concatenation += hash_chunk;
	}

	string two_level_hash;
	ComputeSHA256String(hash_concatenation, &two_level_hash);

	// TODO maybe we should do a stream read / hash update here
	handle.Read((void *)parsed_metadata.signature.data(), parsed_metadata.signature.size(), signature_offset);

	for (auto &key : ExtensionHelper::GetPublicKeys(allow_community_extensions)) {
		if (duckdb_mbedtls::MbedTlsWrapper::IsValidSha256Signature(key, parsed_metadata.signature, two_level_hash)) {
			return true;
			break;
		}
	}

	return false;
}

bool ExtensionHelper::TryInitialLoad(DatabaseInstance &db, FileSystem &fs, const string &extension,
                                     ExtensionInitResult &result, string &error) {
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Loading external extensions is disabled through a compile time flag");
#else
	if (!db.config.options.enable_external_access) {
		throw PermissionException("Loading external extensions is disabled through configuration");
	}
	auto filename = fs.ConvertSeparators(extension);

	bool direct_load;

	// shorthand case
	if (!ExtensionHelper::IsFullPath(extension)) {
		direct_load = false;
		string extension_name = ApplyExtensionAlias(extension);
#ifdef WASM_LOADABLE_EXTENSIONS
		ExtensionRepository repository;

		string custom_endpoint = db.config.options.custom_extension_repo;
		if (!custom_endpoint.empty()) {
			repository = ExtensionRepository("custom", custom_endpoint);
		}
               {
                       auto preferredRepo = DatabaseInstance::GetPreferredRepository(extension);
                       if (!preferredRepo.empty()) {
                               repository = ExtensionRepository("x", preferredRepo);
                       }
               }

		string url_template = ExtensionUrlTemplate(db, repository, "");
		string url = ExtensionFinalizeUrlTemplate(url_template, extension_name);

		char *str = (char *)EM_ASM_PTR(
		    {
			    var jsString = ((typeof runtime == 'object') && runtime && (typeof runtime.whereToLoad == 'function') &&
			                    runtime.whereToLoad)
			                       ? runtime.whereToLoad(UTF8ToString($0))
			                       : (UTF8ToString($1));
			    var lengthBytes = lengthBytesUTF8(jsString) + 1;
			    // 'jsString.length' would return the length of the string as UTF-16
			    // units, but Emscripten C strings operate as UTF-8.
			    var stringOnWasmHeap = _malloc(lengthBytes);
			    stringToUTF8(jsString, stringOnWasmHeap, lengthBytes);
			    return stringOnWasmHeap;
		    },
		    filename.c_str(), url.c_str());
		string address(str);
		free(str);

		filename = address;
#else

		string local_path = !db.config.options.extension_directory.empty()
		                        ? db.config.options.extension_directory
		                        : ExtensionHelper::DefaultExtensionFolder(fs);

		// convert random separators to platform-canonic
		local_path = fs.ConvertSeparators(local_path);
		// expand ~ in extension directory
		local_path = fs.ExpandPath(local_path);
		auto path_components = PathComponents();
		for (auto &path_ele : path_components) {
			local_path = fs.JoinPath(local_path, path_ele);
		}
		filename = fs.JoinPath(local_path, extension_name + ".duckdb_extension");
#endif
	} else {
		direct_load = true;
		filename = fs.ExpandPath(filename);
	}
	/*	if (!fs.FileExists(filename)) {
	        string message;
	        bool exact_match = ExtensionHelper::CreateSuggestions(extension, message);
	        if (exact_match) {
	            message += "\nInstall it first using \"INSTALL " + extension + "\".";
	        }
	        error = StringUtil::Format("Extension \"%s\" not found.\n%s", filename, message);
	        return false;
	    }
	*/

	/*	auto handle = fs.OpenFile(filename, FileFlags::FILE_FLAGS_READ);

	    // Parse the extension metadata from the extension binary
	    auto parsed_metadata = ParseExtensionMetaData(*handle);

	    auto metadata_mismatch_error = parsed_metadata.GetInvalidMetadataError();

	    if (!metadata_mismatch_error.empty()) {
	        metadata_mismatch_error = StringUtil::Format("Failed to load '%s', %s", extension, metadata_mismatch_error);
	    }

	    if (!db.config.options.allow_unsigned_extensions) {
	        bool signature_valid =
	            CheckExtensionSignature(*handle, parsed_metadata, db.config.options.allow_community_extensions);

		if (!metadata_mismatch_error.empty()) {
			throw InvalidInputException(metadata_mismatch_error);
		}

		if (!signature_valid) {
			throw IOException(db.config.error_manager->FormatException(ErrorType::UNSIGNED_EXTENSION, filename));
		}
	} else if (!db.config.options.allow_extensions_metadata_mismatch) {
		if (!metadata_mismatch_error.empty()) {
			// Unsigned extensions AND configuration allowing n, loading allowed, mainly for
			// debugging purposes
			throw InvalidInputException(metadata_mismatch_error);
		}
	}

	        if (!metadata_mismatch_error.empty()) {
	            // Signed extensions perform the full check
	            throw InvalidInputException(metadata_mismatch_error);
	        }
	    } else if (!db.config.options.allow_extensions_metadata_mismatch) {
	        if (!metadata_mismatch_error.empty()) {
	            // Unsigned extensions AND configuration allowing n, loading allowed, mainly for
	            // debugging purposes
	            throw InvalidInputException(metadata_mismatch_error);
	        }
	    }
	*/
	auto filebase = fs.ExtractBaseName(filename);

#ifdef WASM_LOADABLE_EXTENSIONS
	auto basename = fs.ExtractBaseName(filename);
	char *exe = NULL;
	exe = (char *)EM_ASM_PTR(
	    {
		    // Next few lines should argubly in separate JavaScript-land function call
		    // TODO: move them out / have them configurable

		    var url = (UTF8ToString($0));

		    if (typeof XMLHttpRequest === "undefined") {
			    const os = require('os');
			    const path = require('path');
			    const fs = require('fs');

			    var array = url.split("/");
			    var l = array.length;

			    var folder = path.join(os.homedir(), ".duckdb/extensions/" + array[l - 4] + "/" + array[l - 3] + "/" +
			                                             array[l - 2] + "/");
			    var filePath = path.join(folder, array[l - 1]);

			    try {
				    if (!fs.existsSync(folder)) {
					    fs.mkdirSync(folder, {recursive : true});
				    }

				    if (!fs.existsSync(filePath)) {
					    const int32 = new Int32Array(new SharedArrayBuffer(8));
					    var Worker = require('node:worker_threads').Worker;
                var worker = new Worker("const {Worker,isMainThread,parentPort,workerData,} = require('node:worker_threads');var times = 0;var SAB = 23;var Z = 0; async function ZZZ(e) {var x = await fetch(e);var res = await x.arrayBuffer();Atomics.store(SAB, 1, res.byteLength);Atomics.store(SAB, 0, 1);Atomics.notify(SAB, 1);Atomics.notify(SAB, 0);Z = res;};parentPort.on('message', function(event) {if (times == 0) {times++;SAB = event;} else if (times == 1) {times++; ZZZ(event);} else {const a = new Uint8Array(Z);const b = new Uint8Array(event.buffer);var K = Z.byteLength;for (var i = 0; i < K; i++) {b[i] = a[i];}Atomics.notify(event, 0);Atomics.store(SAB, 0, 2);Atomics.notify(SAB, 0);}});", {
                    eval: true
                });
					    var uInt8Array;

					    int32[0] = 0;
					    int32[2] = 4;
					    worker.postMessage(int32);

					    worker.postMessage(url);
					    Atomics.wait(int32, 0, 0);

					    const int32_2 = new Int32Array(new SharedArrayBuffer(int32[1] + 3 - ((int32[1] + 3) % 4)));
					    worker.postMessage(int32_2);

					    Atomics.wait(int32, 0, 1);

					    var x = new Uint8Array(int32_2.buffer, 0, int32[1]);
					    uInt8Array = x;
					    worker.terminate();
					    fs.writeFileSync(filePath, uInt8Array);

				    } else {
					    uInt8Array = fs.readFileSync(filePath);
				    }
			    } catch (e) {
				    console.log("Error fetching module", e);
				    return 0;
			    }
		    } else {
			    const xhr = new XMLHttpRequest();
			    xhr.open("GET", url, false);
			    xhr.responseType = "arraybuffer";
			    xhr.send(null);
			    if (xhr.status != 200)
				    return 0;
			    uInt8Array = xhr.response;
		    }

		    var valid = WebAssembly.validate(uInt8Array);
		    var len = uInt8Array.byteLength;
		    var fileOnWasmHeap = _malloc(len + 4);

		    var properArray = new Uint8Array(uInt8Array);

		    for (var iii = 0; iii < len; iii++) {
			    Module.HEAPU8[iii + fileOnWasmHeap + 4] = properArray[iii];
		    }
		    var LEN123 = new Uint8Array(4);
		    LEN123[0] = len % 256;
		    len -= LEN123[0];
		    len /= 256;
		    LEN123[1] = len % 256;
		    len -= LEN123[1];
		    len /= 256;
		    LEN123[2] = len % 256;
		    len -= LEN123[2];
		    len /= 256;
		    LEN123[3] = len % 256;
		    len -= LEN123[3];
		    len /= 256;
		    Module.HEAPU8.set(LEN123, fileOnWasmHeap);
		    // FIXME: found how to expose those to the logger interface
		    // console.log(LEN123);
		    // console.log(properArray);
		    // console.log(new Uint8Array(Module.HEAPU8, fileOnWasmHeap, len+4));
		    // console.log('Loading extension ', UTF8ToString($1));

		    // Here we add the uInt8Array to Emscripten's filesystem, for it to be found by dlopen
		    FS.writeFile(UTF8ToString($1), new Uint8Array(uInt8Array));
		    return fileOnWasmHeap;
	    },
	    filename.c_str(), basename.c_str());
	if (!exe) {
		throw IOException("Extension %s is not available", filename);
	}

	auto dopen_from = basename;
	if (!db.config.options.allow_unsigned_extensions) {
		// signature is the last 256 bytes of the file

		string signature;
		signature.resize(256);

		D_ASSERT(exe);
		uint64_t LEN = 0;
		LEN *= 256;
		LEN += ((uint8_t *)exe)[3];
		LEN *= 256;
		LEN += ((uint8_t *)exe)[2];
		LEN *= 256;
		LEN += ((uint8_t *)exe)[1];
		LEN *= 256;
		LEN += ((uint8_t *)exe)[0];
		auto signature_offset = LEN - signature.size();

		const idx_t maxLenChunks = 1024ULL * 1024ULL;
		const idx_t numChunks = (signature_offset + maxLenChunks - 1) / maxLenChunks;
		std::vector<std::string> hash_chunks(numChunks);
		std::vector<idx_t> splits(numChunks + 1);

		for (idx_t i = 0; i < numChunks; i++) {
			splits[i] = maxLenChunks * i;
		}
		splits.back() = signature_offset;

		for (idx_t i = 0; i < numChunks; i++) {
			string x;
			x.resize(splits[i + 1] - splits[i]);
			for (idx_t j = 0; j < x.size(); j++) {
				x[j] = ((uint8_t *)exe)[j + 4 + splits[i]];
			}
			ComputeSHA256String(x, &hash_chunks[i]);
		}

		string hash_concatenation;
		hash_concatenation.reserve(32 * numChunks); // 256 bits -> 32 bytes per chunk

		for (auto &hash_chunk : hash_chunks) {
			hash_concatenation += hash_chunk;
		}

		string two_level_hash;
		ComputeSHA256String(hash_concatenation, &two_level_hash);

		for (idx_t j = 0; j < signature.size(); j++) {
			signature[j] = ((uint8_t *)exe)[4 + signature_offset + j];
		}
		bool any_valid = false;
		for (auto &key : ExtensionHelper::GetPublicKeys(db.config.options.allow_community_extensions)) {
			if (duckdb_mbedtls::MbedTlsWrapper::IsValidSha256Signature(key, signature, two_level_hash)) {
				any_valid = true;
				break;
			}
		}
		if (!any_valid) {
			throw IOException(db.config.error_manager->FormatException(ErrorType::UNSIGNED_EXTENSION, filename));
		}
	}
	if (exe) {
		free(exe);
	}
#else
	auto dopen_from = filename;
#endif

	auto lib_hdl = dlopen(dopen_from.c_str(), RTLD_NOW | RTLD_LOCAL);
	if (!lib_hdl) {
		throw IOException("Extension \"%s\" could not be loaded: %s", filename, GetDLError());
	}

	auto lowercase_extension_name = StringUtil::Lower(filebase);

	// Initialize the ExtensionInitResult
	result.filebase = lowercase_extension_name;
	result.filename = filename;
	result.lib_hdl = lib_hdl;
	result.abi_type = ExtensionABIType::CPP;

	if (!direct_load) {
		/*
		        auto info_file_name = filename + ".info";

		        result.install_info = ExtensionInstallInfo::TryReadInfoFile(fs, info_file_name,
		   lowercase_extension_name);

		        if (result.install_info->mode == ExtensionInstallMode::UNKNOWN) {
		            // The info file was missing, we just set the version, since we have it from the parsed footer
		            result.install_info->version = parsed_metadata.extension_version;
		        }

		        if (result.install_info->version != parsed_metadata.extension_version) {
		            throw IOException("Metadata mismatch detected when loading extension '%s'\nPlease try reinstalling
		   the " "extension using `FORCE INSTALL '%s'`", filename, extension);
		        }
		*/
	} else {
		result.install_info = make_uniq<ExtensionInstallInfo>();
		result.install_info->mode = ExtensionInstallMode::NOT_INSTALLED;
		result.install_info->full_path = filename;
		result.install_info->version = ""; // parsed_metadata.extension_version;
	}

	return true;
#endif
}

ExtensionInitResult ExtensionHelper::InitialLoad(DatabaseInstance &db, FileSystem &fs, const string &extension) {
	string error;
	ExtensionInitResult result;
	if (!TryInitialLoad(db, fs, extension, result, error)) {
		auto &config = DBConfig::GetConfig(db);
		if (!config.options.autoinstall_known_extensions || !ExtensionHelper::AllowAutoInstall(extension)) {
			throw IOException(error);
		}
		// the extension load failed - try installing the extension
		ExtensionInstallOptions options;
		ExtensionHelper::InstallExtension(db, fs, extension, options);
		// try loading again
		if (!TryInitialLoad(db, fs, extension, result, error)) {
			throw IOException(error);
		}
	}
	return result;
}

bool ExtensionHelper::IsFullPath(const string &extension) {
	return StringUtil::Contains(extension, ".") || StringUtil::Contains(extension, "/") ||
	       StringUtil::Contains(extension, "\\");
}

string ExtensionHelper::GetExtensionName(const string &original_name) {
	auto extension = StringUtil::Lower(original_name);
	if (!IsFullPath(extension)) {
		return ExtensionHelper::ApplyExtensionAlias(extension);
	}
	auto splits = StringUtil::Split(StringUtil::Replace(extension, "\\", "/"), '/');
	if (splits.empty()) {
		return ExtensionHelper::ApplyExtensionAlias(extension);
	}
	splits = StringUtil::Split(splits.back(), '.');
	if (splits.empty()) {
		return ExtensionHelper::ApplyExtensionAlias(extension);
	}
	return ExtensionHelper::ApplyExtensionAlias(splits.front());
}

void ExtensionHelper::LoadExternalExtension(DatabaseInstance &db, FileSystem &fs, const string &extension) {
	if (db.ExtensionIsLoaded(extension)) {
		return;
	}
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Loading external extensions is disabled through a compile time flag");
#else
	auto extension_init_result = InitialLoad(db, fs, extension);

	// C++ ABI
	if (extension_init_result.abi_type == ExtensionABIType::CPP) {
		auto init_fun_name = extension_init_result.filebase + "_init";
		ext_init_fun_t init_fun = TryLoadFunctionFromDLL<ext_init_fun_t>(extension_init_result.lib_hdl, init_fun_name,
		                                                                 extension_init_result.filename);
		if (!init_fun) {
			throw IOException("Extension '%s' did not contain the expected entrypoint function '%s'", extension,
			                  init_fun_name);
		}

		try {
			(*init_fun)(db);
		} catch (std::exception &e) {
			ErrorData error(e);
			throw InvalidInputException("Initialization function \"%s\" from file \"%s\" threw an exception: \"%s\"",
			                            init_fun_name, extension_init_result.filename, error.RawMessage());
		}

		D_ASSERT(extension_init_result.install_info);

		db.SetExtensionLoaded(extension, *extension_init_result.install_info);
		return;
	}

	// C ABI
	if (extension_init_result.abi_type == ExtensionABIType::C_STRUCT ||
	    extension_init_result.abi_type == ExtensionABIType::C_STRUCT_UNSTABLE) {
		auto init_fun_name = extension_init_result.filebase + "_init_c_api";
		ext_init_c_api_fun_t init_fun_capi = TryLoadFunctionFromDLL<ext_init_c_api_fun_t>(
		    extension_init_result.lib_hdl, init_fun_name, extension_init_result.filename);

		if (!init_fun_capi) {
			throw IOException("File \"%s\" did not contain function \"%s\": %s", extension_init_result.filename,
			                  init_fun_name, GetDLError());
		}
		// Create the load state
		DuckDBExtensionLoadState load_state(db, extension_init_result);

		auto access = ExtensionAccess::CreateAccessStruct();
		auto result = (*init_fun_capi)(load_state.ToCStruct(), &access);

		// Throw any error that the extension might have encountered
		if (load_state.has_error) {
			load_state.error_data.Throw("An error was thrown during initialization of the extension '" + extension +
			                            "': ");
		}

		// Extensions are expected to either set an error or return true indicating successful initialization
		if (result == false) {
			throw FatalException(
			    "Extension '%s' failed to initialize but did not return an error. This indicates an "
			    "error in the extension: C API extensions should return a boolean `true` to indicate succesful "
			    "initialization. "
			    "This means that the Extension may be partially initialized resulting in an inconsistent state of "
			    "DuckDB.",
			    extension);
		}

		D_ASSERT(extension_init_result.install_info);

		db.SetExtensionLoaded(extension, *extension_init_result.install_info);
		return;
	}

	throw IOException("Unknown ABI type of value '%s' for extension '%s'",
	                  static_cast<uint8_t>(extension_init_result.abi_type), extension);
#endif
}

void ExtensionHelper::LoadExternalExtension(ClientContext &context, const string &extension) {
	LoadExternalExtension(DatabaseInstance::GetDatabase(context), FileSystem::GetFileSystem(context), extension);
}

string ExtensionHelper::ExtractExtensionPrefixFromPath(const string &path) {
	auto first_colon = path.find(':');
	if (first_colon == string::npos || first_colon < 2) { // needs to be at least two characters because windows c: ...
		return "";
	}
	auto extension = path.substr(0, first_colon);

	if (path.substr(first_colon, 3) == "://") {
		// these are not extensions
		return "";
	}

	D_ASSERT(extension.size() > 1);
	// needs to be alphanumeric
	for (auto &ch : extension) {
		if (!isalnum(ch) && ch != '_') {
			return "";
		}
	}
	return extension;
}

} // namespace duckdb
