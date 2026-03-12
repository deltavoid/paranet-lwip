

#include "lwip/thread_framework.h"

#include "lwip/logging.h"


int g_tcp_thread_num, g_ip_thread_num; // global variable, init at process initialization, and should not be changed after that.

struct tcp_thread_cb {
    int id;


    // todo, dpdk ring for tcp pkt in process

};

struct ip_thread_cb {
    int id;

};

// todo, thread eventloop, base on epoll and eventfd.


void thread_framework_init(int ip_thread_num, int tcp_thread_num)
{
    LOG_DEBUG("thread_framework_init: 1\n");

}

void thread_framework_destroy()
{
    LOG_DEBUG("thread_framework_destroy: 1\n");
}