/*
 * thread framework
 * @author ZQY
*/

#include <rte_ring.h>
#include <rte_malloc.h>
#include <rte_eal.h>

#include "lwip/thread_framework.h"

#include "lwip/logging.h"
#include <pthread.h>
#include <unistd.h>



// tcp_thread class ----------------------------

struct tcp_thread_ctx {
    int id;
    volatile int running;
    pthread_t pthread_ctx;



    // todo, dpdk ring for tcp pkt in process

    // todo, tcp thread pkt output queue

};

void* tcp_thread_run(void* arg)
{
    struct tcp_thread_ctx* ctx = (struct tcp_thread_ctx*)arg;

    // todo, eventloop, while(epoll) { process event}
    // at first stage, just use eventfd as entry.

    while (ctx->running)
    {
        LOG_DEBUG("tcp_thread_run, id: %d\n", ctx->id);

        sleep(1);        
    }

    return NULL;
}

int tcp_thread_init(struct tcp_thread_ctx* ctx, int id)
{
    ctx->id = id;
    ctx->running = true;

    // create pthread
    int ret = pthread_create(&ctx->pthread_ctx, NULL, tcp_thread_run, ctx);
    if  (ret != 0)
    {   LOG_INFO("pthread create failed\n");
        return ret;
    }

    return ret;
}



void tcp_thread_destroy()
{
}


// ip thread class ---------------------------
struct ip_thread_cb {
    int id;

    // todo, pkt input entry.

};

void ip_thread_run()
{
    // death loop  poll pkt only wait process exit
    
}

// todo, thread eventloop, base on epoll and eventfd.


// thread framework object -------------------------

#define TCP_THREAD_MAX_NUM 32
#define IP_THREAD_MAX_NUM 8 


int g_tcp_thread_num, g_ip_thread_num; // global variable, init at process initialization, and should not be changed after that.

// todo, tcp_thread_cb list, ip_thread_cb_list


void thread_framework_init(int ip_thread_num, int tcp_thread_num)
{
    LOG_DEBUG("thread_framework_init: 1, ip_thread_num: %d, tcp_thread_num: %d\n",
        ip_thread_num, tcp_thread_num);

}

void thread_framework_destroy()
{
    LOG_DEBUG("thread_framework_destroy: 1\n");
}