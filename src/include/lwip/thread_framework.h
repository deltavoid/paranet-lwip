/*
 * thread framework
 * @author ZhangQianyu
*/

#ifndef THREAD_FRAMEWORK_H
#define THREAD_FRAMEWORK_H

#include <rte_ring.h>
#include <pthread.h>


#define TCP_THREAD_INPUT_RING_SIZE 128

struct tcp_thread_ctx {

    pthread_t pthread_ctx;

    int id;
    volatile int running;
    int input_event_fd;
    struct rte_ring* input_pkt_ring;



    // todo, dpdk ring for tcp pkt in process

    // todo, tcp thread pkt output queue

};

#define TCP_THREAD_MAX_NUM 32
#define IP_THREAD_MAX_NUM 8 


extern struct tcp_thread_ctx tcp_thread_ctxs[TCP_THREAD_MAX_NUM];
extern int g_tcp_thread_num, g_ip_thread_num; // global variable, init at process initialization, and should not be changed after that.



void thread_framework_init(int ip_thread_num, int tcp_thread_num);





#endif // THREAD_FRAMEWORK_H
