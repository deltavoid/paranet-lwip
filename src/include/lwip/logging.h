#ifndef LOGGING_H
#define LOGGING_H

#include <stdio.h>
#include <stdlib.h>
#include <execinfo.h>
// #include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>


#define LOG_LEVEL_DEBUG  0
#define LOG_LEVEL_INFO   1
#define LOG_LEVEL_WARN   2
#define LOG_LEVEL_ERROR  3
#define LOG_LEVEL_FATAL  4

#define OUTPUT_FILE stderr

#define CURRENT_LOG_LEVEL LOG_LEVEL_DEBUG

// #define ESC_START     "\033["
// #define ESC_END       "\033[0m"
// #define COLOR_FATAL   "31;40;5m"
// #define COLOR_ALERT   "31;40;1m"
// #define COLOR_CRIT    "31;40;1m"
// #define COLOR_ERROR   "31;40;1m"
// #define COLOR_WARN    "33;40;1m"
// #define COLOR_NOTICE  "34;40;1m"
// #define COLOR_INFO    "32;40;1m"
// #define COLOR_DEBUG   "36;40;1m"
// #define COLOR_TRACE   "37;40;1m"

#if (LOG_LEVEL_DEBUG >= CURRENT_LOG_LEVEL)
    // #define LOG_DEBUG(format, args...) (fprintf(OUTPUT_FILE,  ESC_START COLOR_INFO "[INFO]-[%s]-[%d]-[%s]:" format ESC_END, __FILE__, __LINE__, __FUNCTION__ , ##args))
    // #define LOG_DEBUG(format, args...) (printf( ESC_START COLOR_INFO "[INFO]-[%s]-[%s]-[%d]:" format ESC_END, __FILE__, __FUNCTION__ , __LINE__, ##args))
    // #define LOG_DEBUG(format, args...) (fprintf(OUTPUT_FILE, "DEBUG %s %d %s: " format , __FILE__, __LINE__, __FUNCTION__ , ##args))
    #define LOG_DEBUG(format, args...) (fprintf(OUTPUT_FILE, "DEBUG 00:00:00.000000 %ld %-24s %4d] " format , syscall(__NR_gettid) , __FILE__, __LINE__, ##args))
#else
    #define LOG_DEBUG(format, args...) do{}while(0)
#endif 
    

#if (LOG_LEVEL_INFO >= CURRENT_LOG_LEVEL)
    #define LOG_INFO(format, args...) (fprintf(OUTPUT_FILE, "INFO  %s %d %s] " format , __FILE__, __LINE__, __FUNCTION__ , ##args))
#else
    #define LOG_INFO(format, args...) do{}while(0)
#endif

#if (LOG_LEVEL_WARN >= CURRENT_LOG_LEVEL)
    #define LOG_WARN(format, args...) (fprintf(OUTPUT_FILE, "WARN  %s %d %s] " format , __FILE__, __LINE__, __FUNCTION__ , ##args))
#else
    #define LOG_WARN(format, args...) do{}while(0)
#endif

#if (LOG_LEVEL_ERROR >= CURRENT_LOG_LEVEL)
    #define LOG_ERROR(format, args...) (fprintf(OUTPUT_FILE, "ERROR %s %d %s] " format , __FILE__, __LINE__, __FUNCTION__ , ##args))
#else
    #define LOG_ERROR(format, args...) do{}while(0)
#endif

#if (LOG_LEVEL_FATAL >= CURRENT_LOG_LEVEL)
    #define LOG_FATAL(format, args...) (fprintf(OUTPUT_FILE, "FATAL %s %d %s] " format , __FILE__, __LINE__, __FUNCTION__ , ##args))
#else
    #define LOG_FATAL(format, args...) do{}while(0)
#endif


#if 0
#if (LOG_LEVEL_DEBUG >= CURRENT_LOG_LEVEL)

static inline void print_stack(void** addrs, int num)
{
    char** stack = backtrace_symbols(addrs, num);
    
    fprintf(OUTPUT_FILE, "----backtrace begin----\n");
    for (int i = 0; i < num; i++)
        fprintf(OUTPUT_FILE, "%d: %s\n", i, stack[i]);
    fprintf(OUTPUT_FILE, "----backtrace end----\n");

    free(stack);
}

#define MAX_STACK_DEPTH 64

#define BT_DEBUG() \
do \
{ \
    void* addrs[MAX_STACK_DEPTH]; \
    int num = backtrace(addrs, MAX_STACK_DEPTH); \
    print_stack(addrs, num); \
} while(0)

#else

#define BT_DEBUG() \
do {} while(0)

#endif
#endif // #if 0


#endif // LOGGING_H
