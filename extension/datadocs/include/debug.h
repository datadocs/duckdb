///
/// ================================
/// A header for quickly printing logs for debugging purposes in both the WebAssembly runtime
///   and the native environment
///
/// @author Liu Yue @hangxingliu
/// @version 2024-04-01
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

#ifndef CONSOLE_LOG_MAX
#define CONSOLE_LOG_MAX 16384
#endif

#ifdef __EMSCRIPTEN__
#include "emscripten/console.h"
#ifndef CONSOLE_LOG_PREFIX
	#define CONSOLE_LOG_PREFIX ">>> WASM >>> "
#endif
#else
#ifndef CONSOLE_LOG_PREFIX
	#define CONSOLE_LOG_PREFIX ">>> D7NX >>> "
#endif
#endif

#ifndef init_console_log
#define init_console_log()                          \
    const char* _clog_pre = CONSOLE_LOG_PREFIX;     \
    const size_t _clog_pre_len = strlen(_clog_pre); \
    char _clog_buff[CONSOLE_LOG_MAX];               \
    strcpy(_clog_buff, _clog_pre);                  \
    char* _clog_buff_main = _clog_buff + _clog_pre_len;
#endif


#ifndef console_log

#ifdef __EMSCRIPTEN__
#define console_log(...)                                                         \
    snprintf(_clog_buff_main, CONSOLE_LOG_MAX - _clog_pre_len - 1, __VA_ARGS__); \
    emscripten_console_log(_clog_buff)
#else
#define console_log(...)                                                         \
    snprintf(_clog_buff_main, CONSOLE_LOG_MAX - _clog_pre_len - 1, __VA_ARGS__); \
    puts(_clog_buff)
#endif
#endif
