

#include "lwip/thread_framework.h"

#include "lwip/logging.h"




// tcp_thread class ----------------------------

struct tcp_thread_cb {
    int id;


    // todo, dpdk ring for tcp pkt in process

    // todo, tcp thread pkt output queue

};

// void tcp_thread_init(struct tcp_thread_cb* cb)
// {

// }

// void tcp_thread_run(struct tcp_thread_cb* cb)
// {

//     // todo, eventloop, while(epoll) { process event}
//     // at first stage, just use eventfd as entry.


// }

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