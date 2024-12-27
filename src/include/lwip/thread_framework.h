/*
 * thread framework
 * @author ZhangQianyu
*/

#ifndef THREAD_FRAMEWORK_H
#define THREAD_FRAMEWORK_H

#include <rte_ring.h>
#include <pthread.h>

#include "lwip/ip.h"

// tcp_thread_ctx -----------------------------------

struct tcp_thread_input_pkt_wrapper {

    struct ip_globals ip_data;

    struct pbuf *p;
};

#define TCP_THREAD_INPUT_RING_SIZE 512

struct tcp_thread_ctx {

    pthread_t pthread_ctx;
    struct rte_ring* input_pkt_ring;
    int id;
    volatile int running;
    int input_event_fd;
    int loop_state;

};

void tcp_thread_input_ring_notify(struct tcp_thread_ctx* ctx, uint64_t val);

extern _Thread_local volatile int thread_tx_queue_id; // default 0, tcp thread set it to sepcific id;

extern struct rte_mempool *pktmbuf_pool_tcp_tx;
struct rte_mempool* tcp_create_pktmbuf_pool_tcp_tx(int tcp_thread_num);

extern struct rte_mempool *pktmbuf_pool_rx;
struct rte_mempool* tcp_create_pktmbuf_pool_rx(int tcp_thread_num);

// thread framework -------------------

#define TCP_THREAD_MAX_NUM 32
#define IP_THREAD_MAX_NUM 32


extern struct tcp_thread_ctx tcp_thread_ctxs[TCP_THREAD_MAX_NUM];
extern int g_tcp_thread_num, g_ip_thread_num; // global variable, init at process initialization, and should not be changed after that.



static inline struct tcp_thread_ctx* get_tcp_thread_ctx_by_id(int id)
{
    if  (!(id >= 0 && id < g_tcp_thread_num)) return NULL;
    return &tcp_thread_ctxs[id];
}

// todo, get_tcp_thread_ctx_default() // by thread_tx_queue_id;

void thread_framework_init(int ip_thread_num, int tcp_thread_num, struct netif* nif);





#endif // THREAD_FRAMEWORK_H
