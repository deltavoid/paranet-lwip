/*
 * thread framework
 * @author ZQY
*/

#include "lwip/thread_framework.h"

// #include <rte_ring.h>
#include <rte_malloc.h>
#include <rte_eal.h>



#include "lwip/logging.h"

// #include <pthread.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "lwip/pbuf.h"
#include "lwip/prot/tcp.h"
#include "lwip/def.h"
#include "lwip/timeouts.h"
#include "lwip/priv/tcp_priv.h"

#include <rte_mbuf.h>
#include <rte_errno.h>
#include <sys/epoll.h>

// tcp_thread class ----------------------------

static inline uint64_t get_now () 
{
    return ({ 
        struct timespec ts; 
        clock_gettime(CLOCK_REALTIME, &ts); 
        (ts.tv_sec * 1000000000UL + ts.tv_nsec); 
    });
}

void tcp_input_backend(struct pbuf *p);
void tx_flush(void);
// extern _Thread_local volatile int thread_tx_queue_id; // default 0, tcp thread set it to sepcific id;
_Thread_local volatile int thread_tx_queue_id = 0; // default 0, tcp thread set it to sepcific id;


void user_app_init();


int tcp_thread_input_ring_notify(struct tcp_thread_ctx *ctx)
{
    if  (ctx->ring_in_process)
        return 0;
    
    uint64_t val = 1;
    if (write(ctx->input_event_fd, &val, sizeof(val)) != sizeof(val))
    {
        perror("write eventfd error");
    }
    return 1;
}

int tcp_thread_input_ring_ack(struct tcp_thread_ctx *ctx, uint64_t* val_p)
{
    assert(val_p != NULL);
    if (read(ctx->input_event_fd, val_p, sizeof(*val_p)) != sizeof(*val_p))
    {
        perror("read eventfd error");
        return -1;
    }
    return 0;
}


_Thread_local uint64_t input_enqueue_num = 0, event_fd_notify_num = 0;

int tcp_thread_input_ring_enqueue(int tcp_tid, int ip_tid, void* data)
{
    assert(tcp_tid >= 0 && tcp_tid < g_tcp_thread_num);
    assert(ip_tid >= 0 && ip_tid < g_ip_thread_num);
    struct tcp_thread_ctx * ctx = get_tcp_thread_ctx_by_id(tcp_tid);

    struct rte_ring* ring = ctx->input_pkt_rings[ip_tid];
    int ret = rte_ring_enqueue(ring, data);

    input_enqueue_num++;
    // event_fd_notify_num += tcp_thread_input_ring_notify(ctx);
    return ret;
}


#define tcp_thread_process_ts_num 22
_Thread_local int64_t tcp_thread_process_ts[tcp_thread_process_ts_num + 1];


#define TCP_THREAD_RUN_MAX_PKT_ONCE 128

// uint64_t tcp_thread_poll_input_ring_once(struct tcp_thread_ctx* ctx, int ring_num)
uint64_t tcp_thread_poll_input_ring_once(struct tcp_thread_ctx* ctx, int ring_id, int64_t* pkt_num_cnt_p, int64_t* pkt_process_time_p)
{
    ctx->loop_state = 3;
    // uint64_t val = 0;
    // if (read(ctx->input_event_fd, &val, sizeof(val)) != sizeof(val))
    // {
    //     perror("read eventfd error");
    //     return 0;
    // }
    // LOG_DEBUG("tcp_thread_run: 3, ctx_id: %d\n", ctx->id);
    struct rte_ring* ring = ctx->input_pkt_rings[ring_id];
    int ring_num = rte_ring_count(ring);

    int process_num = ring_num > TCP_THREAD_RUN_MAX_PKT_ONCE ? 
            TCP_THREAD_RUN_MAX_PKT_ONCE : ring_num;

    for (int i = 0; i < process_num; i++)
    {
        void *obj_ptr;
        ctx->loop_state = 4;
        tcp_thread_process_ts[0] = get_mono_tnesc();
        if (rte_ring_dequeue(ring, &obj_ptr) == 0)
        {
            // get object
            // LOG_DEBUG("tcp_thread_run: 4, data: %d\n", *(int*)obj_ptr);

            // struct tcp_thread_input_pkt_wrapper *wrapper =
            //     (struct tcp_thread_input_pkt_wrapper *)obj_ptr;
            // struct pbuf *p = wrapper->p;
            tcp_thread_process_ts[1] = get_mono_tnesc();
            struct pbuf *p = (struct pbuf *)obj_ptr;
            // assert(p->type_internal == PBUF_RTE_MBUF_RX);
            // struct tcp_hdr *tcphdr = (struct tcp_hdr *)p->payload;
            // LOG_DEBUG("tcp_thread_run: tcphdr: src: %d, dest: %d\n",
            //     lwip_ntohs(tcphdr->src), lwip_ntohs(tcphdr->dest));

            assert(p->type_internal == PBUF_RTE_MBUF_RX);
            struct ip_globals* ip_data_p = rte_mbuf_to_priv(p->related_mbuf);

            // ip_data = wrapper->ip_data;
            // rte_free(wrapper);
            ip_data = * ip_data_p;

            ctx->loop_state = 5;
            tcp_thread_process_ts[2] = get_mono_tnesc();
            tcp_input_backend(p);
            // pbuf_free(p);
            tcp_thread_process_ts[22] = get_mono_tnesc();
            ctx->loop_state = 6;

            (*pkt_num_cnt_p)++;
            for (int j = 1; j<= tcp_thread_process_ts_num; j++)
            {
                if  (tcp_thread_process_ts[j] > tcp_thread_process_ts[j - 1])
                {
                    int64_t duration = tcp_thread_process_ts[j] - tcp_thread_process_ts[j - 1];
                    pkt_process_time_p[j] += duration;
                }
            }

        }
        else
        {
            LOG_DEBUG("tcp_thread_run: 5, no element\n");
            break;
        }
    }

    return process_num;
}



/* void*  */ int tcp_thread_run(void* arg)
{
    struct tcp_thread_ctx* ctx = (struct tcp_thread_ctx*)arg;
    // int cnt = 0;
    uint64_t pkt_cnt = 0;
    uint64_t prev_ts = 0;
    int last_poll_pkt_num = 0;
    uint64_t poll_cnt = 0;
    int64_t pkt_process_cnt = 0;
    int64_t pkt_process_time[tcp_thread_process_ts_num + 1];

#define MAX_EPOLL_EVENT_NUM 5
    // struct epoll_event events[MAX_EPOLL_EVENT_NUM];

    thread_tx_queue_id = 1 + ctx->id;    

    // todo, eventloop, while(epoll) { process event}
    // at first stage, just use eventfd as entry.
    ctx->loop_state = 1;

    if  (ctx->id == 0)
    {
        user_app_init();

        // tx_flush();
    }

    ctx->ring_in_process = true;

    while (ctx->running)
    {
        ctx->loop_state = 2;        
        // LOG_DEBUG("tcp_thread_run: 2, id: %d\n", ctx->id);


        // dead poll
        // usleep(1);
        // int ring_num = rte_ring_count(ctx->input_pkt_ring);
        // if (ring_num == 0)
        // if  (last_poll_pkt_num == 0)
        // {
        //     ctx->ring_in_process = false;

        //     // return at once. dead lock
        //     int num = epoll_wait(ctx->epoll_fd, events, MAX_EPOLL_EVENT_NUM, /* 250 */0);
        //     LOG_DEBUG("tcp_thread_run: 2, id: %d, epoll_wait return %d\n", ctx->id, num);
        //     if ((num < 0))
        //         perror("epoll_wait error");

        //     for (int i = 0; i < num; i++)
        //     {
        //         int fd = events[i].data.fd;
        //         if (fd == ctx->input_event_fd)
        //         {
        //             uint64_t val;
        //             tcp_thread_input_ring_ack(ctx, &val);

        //             // pkt_cnt += tcp_thread_poll_input_ring_once(ctx);
        //             // ring_num = rte_ring_count(ctx->input_pkt_ring);
        //             // if (ring_num > 0)
        //             //     ctx->ring_in_process = true;

        //             ctx->ring_in_process = true;
        //         }
        //         else
        //         {
        //             LOG_INFO("tcp_thread_run, unknown event\n");
        //         }
        //     }
        // }

        // if  (ctx->ring_in_process > 0)
        {
            last_poll_pkt_num = 0;
            for (int i = 0; i < g_ip_thread_num; i++)
            {
                int ret = tcp_thread_poll_input_ring_once(ctx, i, &pkt_process_cnt, pkt_process_time);
                last_poll_pkt_num += ret;
                pkt_cnt += ret;
            }
        }


        // uint64_t val = 0;
        // if  (read(ctx->input_event_fd, &val, sizeof(val)) != sizeof(val))
        // {   perror("read eventfd error");
        //     break;
        // }
        // LOG_DEBUG("tcp_thread_run: 3, ctx_id: %d, get val: %ld\n", ctx->id, val);
        
        // for (uint64_t i = 0; i < val; i++)
        // {
        //     void* obj_ptr;
        //     ctx->loop_state = 4;
        //     if (rte_ring_dequeue(ctx->input_pkt_ring, &obj_ptr) == 0)
        //     {
        //         // get object
        //         // LOG_DEBUG("tcp_thread_run: 4, data: %d\n", *(int*)obj_ptr);
                
        //         struct tcp_thread_input_pkt_wrapper*  wrapper  = 
        //             (struct tcp_thread_input_pkt_wrapper*)obj_ptr;
        //         struct pbuf *p = wrapper->p;
        //         // struct tcp_hdr *tcphdr = (struct tcp_hdr *)p->payload;
        //         // LOG_DEBUG("tcp_thread_run: tcphdr: src: %d, dest: %d\n", 
        //         //     lwip_ntohs(tcphdr->src), lwip_ntohs(tcphdr->dest));
                
        //         ip_data = wrapper->ip_data;
        //         rte_free(wrapper);

        //         ctx->loop_state = 5;
        //         tcp_input_backend(p);
        //         // pbuf_free(p);
        //         ctx->loop_state = 6;

        //     }
        //     else
        //     {
        //         LOG_DEBUG("tcp_thread_run: 5, no element\n");   
        //         break;
        //     }
        // }

        ctx->loop_state = 7;
        tcp_timer_needed();
        sys_check_timeouts();

        ctx->loop_state = 8;
        tx_flush();

        ctx->loop_state = 9;

        if (++poll_cnt % 1000 == 0)
        {
            uint64_t now = get_now();
            if (now - prev_ts > 1000000000UL)
            {

                // LOG_INFO("tcp_thread_run: 3: ctx_id: %d, pkt_cnt: %lu, input_ring num: %d\n",
                //         ctx->id, pkt_cnt, );
                LOG_INFO("tcp_thread_run: 3: ctx_id: %d, pkt_cnt: %lu, last_poll_pkt_num: %d\n",
                         ctx->id, pkt_cnt, last_poll_pkt_num);
                // for (int i = 0; i < g_ip_thread_num; i++)
                // {   LOG_INFO("input_ring_num, tcp_tid: %d, ip_tid: %d, num: %d\n",
                //             ctx->id, i, rte_ring_count(ctx->input_pkt_rings[i]));
                // }

                for (int j = 1; j <= tcp_thread_process_ts_num; j++)
                {
                    double duration = (double)pkt_process_time[j] / pkt_process_cnt;
                    LOG_INFO("tcp_thread_run: 3, ctx_id: %d, stage %d duration (ns): %lf\n",
                            ctx->id, j, duration);
                    pkt_process_time[j] = 0;
                }
                pkt_process_cnt = 0;

                prev_ts = now;
            }
        }

        ctx->loop_state = 10;

        // usleep(1);        
    }

    // return NULL;
    return 0;
}




// mem layout: struct rte_mbuf | (@priv) struct tcg_seg | (@data_room) struct pbuf + data
struct rte_mempool* tcp_thread_create_pktmbuf_pool_tcp_tx(int id)
{
    // LWIP_UNUSED_ARG(id);
    char pool_name[50];
    snprintf(pool_name, 50, "tcp_thread_tx_pool-%d", id);
    LOG_DEBUG("pool name: %s\n", pool_name);

    struct rte_mempool*  ret = rte_pktmbuf_pool_create(pool_name/* NULL *//* "pktmbuf_pool_tcp_tx_0" */,
			    8192 - 1, 512, LWIP_MEM_ALIGN_SIZE(sizeof(struct tcp_seg)), 
                LWIP_MEM_ALIGN_SIZE(sizeof(struct pbuf)) + RTE_MBUF_DEFAULT_BUF_SIZE,
                rte_socket_id());
    
    if  (ret == NULL)
        LOG_INFO("create pool failed: %s\n", rte_strerror(rte_errno));
    return ret;
}

int tcp_thread_init(struct tcp_thread_ctx* ctx, int id, int core_id)
{
    int ret = 0;;
    ctx->id = id;
    ctx->running = true;
    char ring_name[32];

    LOG_DEBUG("tcp_thread_init: 1, begin, id: %d\n", id);


    // epoll fd
    ctx->epoll_fd = epoll_create1(0);
    if  (ctx->epoll_fd < 0)
    {   perror("epoll fd created error");
    } 


    /* 2. 创建多生产者单消费者无锁队列 */
    // snprintf(ring_name, 32, "tcp_input_ring%d", id);
    // ctx->input_pkt_ring = rte_ring_create(ring_name, TCP_THREAD_INPUT_RING_SIZE,
    //         rte_socket_id(), RING_F_SC_DEQ);    /* 单消费者出队标志 */
    // if  (ctx->input_pkt_ring == NULL)
    // {   LOG_INFO("create input ring failed\n");
    //     return -1;
    // }

    for (int i = 0; i < g_ip_thread_num; i++)
    {
        snprintf(ring_name, 32, "tcp_input_ring%d-i%d", id, i);
        struct rte_ring *ring = rte_ring_create(ring_name, TCP_THREAD_INPUT_RING_SIZE,
                                                rte_socket_id(), RING_F_SC_DEQ | RING_F_SP_ENQ); /* 单消费者出队标志 */
        if (ring == NULL)
        {
            LOG_INFO("create input ring failed\n");
            return -1;
        }
        ctx->input_pkt_rings[i] = ring;
    }

    ret = eventfd(0, 0);
    if  (ret < 0)
    {   perror("eventfd create error");
        return ret;
    }
    ctx->input_event_fd = ret;


    // attach eventfd to epoll fd
    struct epoll_event event_fd_event;
    event_fd_event.events = EPOLLIN;
    event_fd_event.data.fd = ctx->input_event_fd;
    // listen_event.data.ptr = NULL;
    ret = epoll_ctl(ctx->epoll_fd, EPOLL_CTL_ADD, ctx->input_event_fd, &event_fd_event);
    if  (ret < 0)  
        perror("epoll_ctl event_fd error");


    ctx->pktmbuf_pool_tcp_tx = tcp_thread_create_pktmbuf_pool_tcp_tx(ctx->id);
    if  (ctx->pktmbuf_pool_tcp_tx == NULL)
    {   LOG_INFO("create tcp tx pool failed\n");
        while (1) sleep(1);
        return -1;
    }

    // create pthread
    // ret = pthread_create(&ctx->pthread_ctx, NULL, tcp_thread_run, ctx);
    ret = rte_eal_remote_launch(tcp_thread_run, ctx, core_id);
    if  (ret != 0)
    {   perror("pthread create failed\n");
        return ret;
    }

    return ret;
}



void tcp_thread_destroy(struct tcp_thread_ctx* ctx)
{
    LWIP_UNUSED_ARG(ctx);
    // rte_ring_free(ctx->input_pkt_ring);
}



#define MEMPOOL_CACHE_SIZE (256)


// struct rte_mempool *pktmbuf_pool_tcp_tx = NULL;

// mem layout: struct rte_mbuf | (@priv) struct tcg_seg | (@data_room) struct pbuf + data
struct rte_mempool* tcp_create_pktmbuf_pool_tcp_tx(int tcp_thread_num)
{
    return rte_pktmbuf_pool_create("pktmbuf_pool_tcp_tx",
			    tcp_thread_num * 128, MEMPOOL_CACHE_SIZE, LWIP_MEM_ALIGN_SIZE(sizeof(struct tcp_seg)), 
                LWIP_MEM_ALIGN_SIZE(sizeof(struct pbuf)) + RTE_MBUF_DEFAULT_BUF_SIZE,
                rte_socket_id());
}


// struct rte_mempool *pktmbuf_pool_rx = NULL;

struct rte_mempool* tcp_create_pktmbuf_pool_rx(int tcp_thread_num)
{
    return rte_pktmbuf_pool_create("pktmbuf_pool_rx",
			    tcp_thread_num * 512, MEMPOOL_CACHE_SIZE, 0, 
                LWIP_MEM_ALIGN_SIZE(sizeof(struct pbuf)) + RTE_MBUF_DEFAULT_BUF_SIZE,
                rte_socket_id());
}


// ip thread class ---------------------------
// struct ip_thread_ctx {
//     pthread_t pthread_ctx;
//     struct netif* _netif;
//     struct rte_mempool *pktmbuf_pool_rx;
//     int id;
//     volatile int running;

// };

_Thread_local volatile int ip_thread_identify_id = 0; // default 0, ip thread set it to sepcific id;


// unsigned short netif_poll_once(struct netif* _netif_p, int queue_id);
unsigned short netif_poll_once(struct netif* _netif_p, int queue_id, int64_t* process_cnt_p, int64_t* process_time_p);

void netif_rx_test_sleep(unsigned short nb_rx, uint16_t queue_id);


_Thread_local int64_t ip_thread_ts[10];


int 
/* void*  */ip_thread_run(void* arg)
{
    struct ip_thread_ctx* ctx = (struct ip_thread_ctx*)arg;
    uint64_t cnt = 0;
    uint64_t prev_ts = 0;
    uint64_t pkt_cnt = 0;
    // death loop  poll pkt only wait process exit
    int64_t process_cnt = 0;
    int64_t process_time[10];
    ip_thread_identify_id = ctx->id + 1;

    while (ctx->running)
    {
        // LOG_DEBUG("ip_thread_run: 2\n");


        unsigned short nb_rx = netif_poll_once(ctx->_netif, ctx->id, &process_cnt, process_time);
        pkt_cnt += nb_rx;

        tx_flush();


        // netif_rx_test_sleep(nb_rx, ctx->id);

        if  (++cnt % 1000 == 0)
        {
            // usleep(1);
            uint64_t now = get_now();
            if  (now  - prev_ts > 1000000000UL)
            {
                LOG_INFO("ip_thread_run: 3: ctx_id: %d, pkt_cnt: %lu, nb_rx: %d, enqueue_num: %ld, input_cnt: %ld\n", 
                        ctx->id, pkt_cnt, nb_rx, input_enqueue_num, process_cnt);
                
                for (int j = 1; j <= 6; j++)
                {
                    LOG_INFO("poll_once stage %d duration avg ns: %lf\n", 
                            j, (double)process_time[j] / process_cnt);
                    process_time[j] = 0;
                }

                input_enqueue_num = 0;
                process_cnt = 0;
                prev_ts = now;
            }
        }
    }
    

    // ret = rte_ring_enqueue(g_ring, obj);

    // return NULL;
    return 0;
}

struct rte_mempool* ip_thread_create_pktmbuf_pool_rx(int id)
{
    char pool_name[50];
    snprintf(pool_name, 50, "ip_thread_rx_pool-%d", id);
    LOG_DEBUG("pool name: %s\n", pool_name);
    
    struct rte_mempool* ret = rte_pktmbuf_pool_create(/* "pktmbuf_pool_rx" */pool_name,
			    /* tcp_thread_num * 512 */32768 - 1, 512, sizeof(struct ip_globals), 
                LWIP_MEM_ALIGN_SIZE(sizeof(struct pbuf)) + RTE_MBUF_DEFAULT_BUF_SIZE,
                rte_socket_id());

    return ret;
}

int ip_thread_init(struct ip_thread_ctx* ctx, int id, int core_id, struct netif* nif)
{
    int ret = 0;

    ctx->id = id;
    ctx->running = true;
    ctx->_netif = nif;


    ctx->pktmbuf_pool_rx = ip_thread_create_pktmbuf_pool_rx(ctx->id);
    if  (ctx->pktmbuf_pool_rx == NULL)
    {   LOG_INFO("create ip pool failed\n");
    }

        // create pthread
    // ret = pthread_create(&ctx->pthread_ctx, NULL, ip_thread_run, ctx);
    ret = rte_eal_remote_launch(ip_thread_run, ctx, core_id);
    if  (ret != 0)
    {   perror("pthread create failed\n");
        return ret;
    }

    return 0;
}




// thread framework object -------------------------


struct tcp_thread_ctx tcp_thread_ctxs[TCP_THREAD_MAX_NUM];
struct ip_thread_ctx ip_thread_ctxs[IP_THREAD_MAX_NUM];
int g_tcp_thread_num, g_ip_thread_num; // global variable, init at process initialization, and should not be changed after that.

uint64_t tcp_input_frontend_pkt_cnt[IP_THREAD_MAX_NUM];

// struct ip_thread_ctx* get_ip_thread_ctx_default()
// {
//     if  (!(ip_thread_identify_id >= 1 && ip_thread_identify_id <= g_ip_thread_num)) return NULL;
//     return &ip_thread_ctxs[ip_thread_identify_id - 1];
// }


void thread_framework_init(int ip_thread_num, int tcp_thread_num, struct netif* nif)
{
    LOG_DEBUG("thread_framework_init: 1, ip_thread_num: %d, tcp_thread_num: %d\n",
        ip_thread_num, tcp_thread_num);
    
    // LWIP_UNUSED_ARG(ip_thread_num);
    // LWIP_UNUSED_ARG(nif);

    
    g_ip_thread_num = ip_thread_num;
    g_tcp_thread_num = tcp_thread_num;

    for (int i = 0; i < tcp_thread_num; i++)
    {
        int ret = tcp_thread_init(&tcp_thread_ctxs[i], i, 1 + g_ip_thread_num + i);
        if  (ret != 0)
        {   LOG_INFO("tcp_thread_init failed\n");
        }
    }

    for (int i = 0; i < ip_thread_num; i++)
    {
        int ret = ip_thread_init(&ip_thread_ctxs[i], i, 1 + i, nif);
        if  (ret != 0)
        {   LOG_INFO("ip_thread_init failed\n");
        }
    }

}

void thread_framework_destroy()
{
    LOG_DEBUG("thread_framework_destroy: 1\n");
}