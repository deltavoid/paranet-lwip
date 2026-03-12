

#include "lwip/thread_framework.h"

#include "lwip/logging.h"


int tcp_thread_num, ip_thread_num; // global variable, init at process initialization, and should not be changed after that.




void thread_framework_init()
{
    LOG_DEBUG("thread_framework_init: 1\n");

}

void thread_framework_destroy()
{
    LOG_DEBUG("thread_framework_destroy: 1\n");
}