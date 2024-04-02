///
/// ================================
/// A header for quickly printing logs for debugging purposes in both the WebAssembly runtime
///   and the native environment
///
/// @author Liu Yue @hangxingliu
/// @version 2024-04-03
/// ================================
/// Example Usage:
///
///   init_console_log();
///   ....
///   console_log("%u", size);
///   ....
///   console_log("done");
///
/// Then you can search for the prefix '>>> WASM >>>' in the devTools console of your browser
///   to filter the log entries
///
#ifdef __EMSCRIPTEN__
#include "emscripten/console.h"
#endif

#ifndef DEBUG_CONSOLE_LOG_PREFIX
#ifdef __EMSCRIPTEN__
#define DEBUG_CONSOLE_LOG_PREFIX ">>> WASM >>> "
#else
#define DEBUG_CONSOLE_LOG_PREFIX ">>> D7NX >>> "
#endif
inline char *__debug_console_log_buff = nullptr;
inline char *__debug_console_log_start = nullptr;
inline size_t __debug_console_log_max = 0;
#endif

#ifndef console_log
#ifdef __EMSCRIPTEN__
#define _debug_console_log() emscripten_console_log(__debug_console_log_buff);
#else
#define _debug_console_log()                                                                                           \
	puts(__debug_console_log_buff);                                                                                    \
	fflush(stdout);
#endif

#define console_log(...)                                                                                               \
	{                                                                                                                  \
		size_t max = 16384;                                                                                            \
		if (!__debug_console_log_buff) {                                                                               \
			size_t prefix_len = strlen(DEBUG_CONSOLE_LOG_PREFIX);                                                      \
			__debug_console_log_buff = (char *)malloc(max);                                                            \
			__debug_console_log_start = __debug_console_log_buff + prefix_len;                                         \
			__debug_console_log_max = max - prefix_len - 1;                                                            \
			strcpy(__debug_console_log_buff, DEBUG_CONSOLE_LOG_PREFIX);                                                \
		}                                                                                                              \
		snprintf(__debug_console_log_start, __debug_console_log_max, __VA_ARGS__);                                     \
		_debug_console_log();                                                                                          \
	}
#endif
