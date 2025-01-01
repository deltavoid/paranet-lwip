/*
 * thread framework
 * @author ZhangQianyu
*/

#ifndef THREAD_FRAMEWORK_H
#define THREAD_FRAMEWORK_H

#include <rte_ring.h>
#include <pthread.h>

#include "lwip/ip.h"

#define TCP_THREAD_MAX_NUM 32
#define IP_THREAD_MAX_NUM 16

// for epoll -----------------

typedef int (*epoll_handler_t)(void* data, uint32_t events);
typedef void (*destructor_t)(void* data);

struct epoll_handler_ops {
    epoll_handler_t handler;
    destructor_t destructor;
};

struct epoll_handler_trait {
    struct epoll_handler_ops * ops;
    void* data;
};

static inline int epoll_handle(struct epoll_handler_trait* trait, uint32_t events)
{
    return trait->ops->handler(trait->data, events);
}

static inline void epoll_handler_destruct(struct epoll_handler_trait* trait)
{
    trait->ops->destructor(trait->data);
}

// tcp_thread_ctx -----------------------------------

struct tcp_thread_input_pkt_wrapper {

    struct ip_globals ip_data;

    struct pbuf *p;
};

#define TCP_THREAD_INPUT_RING_SIZE 512

struct tcp_thread_ctx {
    int id;
    int loop_state;    
    volatile bool running;
    volatile bool ring_in_process;
    int input_event_fd;
    int epoll_fd;
    uint64_t input_pkt_num;
    uint64_t recv_pkt_num;
    uint64_t recv_pkt_bytes;
    uint64_t recv_pkt_rtt_us;
    struct timespec recv_time;

    
    pthread_t pthread_ctx;
    // struct rte_ring* input_pkt_ring;
    struct rte_mempool *pktmbuf_pool_tcp_tx;
    // struct epoll_handler_trait input_event_fd_handler;

    struct rte_ring* input_pkt_rings[IP_THREAD_MAX_NUM];
} __rte_cache_aligned;

//  void tcp_thread_input_ring_notify(struct tcp_thread_ctx *ctx);
int tcp_thread_input_ring_enqueue(int tcp_tid, int ip_tid, void* data);



// extern struct rte_mempool *pktmbuf_pool_tcp_tx;
// struct rte_mempool* tcp_create_pktmbuf_pool_tcp_tx(int tcp_thread_num);

// ip_thread_ctx ---------------------

struct ip_thread_ctx {
    pthread_t pthread_ctx;
    struct netif* _netif;
    struct rte_mempool *pktmbuf_pool_rx;
    int id;
    volatile int running;
    uint64_t input_num;
    uint64_t enqueue_num;

} __rte_cache_aligned;

// extern struct rte_mempool *pktmbuf_pool_rx;
// struct rte_mempool* tcp_create_pktmbuf_pool_rx(int tcp_thread_num);

// thread framework -------------------

// #define TCP_THREAD_MAX_NUM 32
// #define IP_THREAD_MAX_NUM 32


extern struct tcp_thread_ctx tcp_thread_ctxs[TCP_THREAD_MAX_NUM];
extern struct ip_thread_ctx ip_thread_ctxs[IP_THREAD_MAX_NUM];
extern int g_tcp_thread_num, g_ip_thread_num; // global variable, init at process initialization, and should not be changed after that.
// extern uint64_t tcp_input_frontend_pkt_cnt[IP_THREAD_MAX_NUM];


extern _Thread_local volatile int thread_tx_queue_id; // default 0, tcp thread set it to sepcific id;

static inline struct tcp_thread_ctx* get_tcp_thread_ctx_by_id(int id)
{
    if  (!(id >= 0 && id < g_tcp_thread_num)) return NULL;
    return &tcp_thread_ctxs[id];
}

// todo, get_tcp_thread_ctx_default() // by thread_tx_queue_id;
static inline struct tcp_thread_ctx* get_tcp_thread_ctx_default()
{
    if  (!(thread_tx_queue_id >= 1 && thread_tx_queue_id <= g_tcp_thread_num)) return NULL;
    return &tcp_thread_ctxs[thread_tx_queue_id - 1];
}

extern _Thread_local int64_t ip_thread_ts[10];


extern _Thread_local volatile int ip_thread_identify_id;

// struct ip_thread_ctx* get_ip_thread_ctx_default();


static inline struct ip_thread_ctx* get_ip_thread_ctx_by_id(int id)
{
    if  (!(id >= 0 && id < g_ip_thread_num)) return NULL;
    return &ip_thread_ctxs[id];
}

static inline struct ip_thread_ctx* get_ip_thread_ctx_default()
{
    if  (!(ip_thread_identify_id >= 1 && ip_thread_identify_id <= g_ip_thread_num)) return NULL;
    return &ip_thread_ctxs[ip_thread_identify_id - 1];
}

extern _Thread_local uint64_t input_enqueue_num, event_fd_notify_num;



void thread_framework_init(int ip_thread_num, int tcp_thread_num, struct netif* nif);




static inline long get_mono_tnesc()
{
    struct timespec ts; 
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000L + ts.tv_nsec;
}

extern _Thread_local int64_t tcp_thread_process_ts[];


// static inline void tcp_thread_ts_check(int i)
// {
//     tcp_thread_process_ts[i] = get_mono_tnesc();
// }
#define tcp_thread_ts_check(i) do {} while(0)

#endif // THREAD_FRAMEWORK_H
