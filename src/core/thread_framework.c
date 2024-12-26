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


/* void*  */ int tcp_thread_run(void* arg)
{
    struct tcp_thread_ctx* ctx = (struct tcp_thread_ctx*)arg;
    // int cnt = 0;
    uint64_t pkt_cnt = 0;
    uint64_t prev_ts = 0;

    thread_tx_queue_id = 1 + ctx->id;    

    // todo, eventloop, while(epoll) { process event}
    // at first stage, just use eventfd as entry.
    ctx->loop_state = 1;

    while (ctx->running)
    {
        LOG_DEBUG("tcp_thread_run: 2, id: %d\n", ctx->id);
        ctx->loop_state = 2;

        uint64_t val = 0;
        if  (read(ctx->input_event_fd, &val, sizeof(val)) != sizeof(val))
        {   perror("read eventfd error");
            break;
        }
        LOG_DEBUG("tcp_thread_run: 3, ctx_id: %d, get val: %ld\n", ctx->id, val);
        pkt_cnt += val;

        ctx->loop_state = 3;
        for (uint64_t i = 0; i < val; i++)
        {
            void* obj_ptr;
            ctx->loop_state = 4;
            if (rte_ring_dequeue(ctx->input_pkt_ring, &obj_ptr) == 0)
            {
                // get object
                // LOG_DEBUG("tcp_thread_run: 4, data: %d\n", *(int*)obj_ptr);
                
                struct tcp_thread_input_pkt_wrapper*  wrapper  = 
                    (struct tcp_thread_input_pkt_wrapper*)obj_ptr;
                struct pbuf *p = wrapper->p;
                // struct tcp_hdr *tcphdr = (struct tcp_hdr *)p->payload;
                // LOG_DEBUG("tcp_thread_run: tcphdr: src: %d, dest: %d\n", 
                //     lwip_ntohs(tcphdr->src), lwip_ntohs(tcphdr->dest));
                
                ip_data = wrapper->ip_data;
                rte_free(wrapper);

                ctx->loop_state = 5;
                tcp_input_backend(p);
                // pbuf_free(p);
                ctx->loop_state = 6;

            }
            else
            {
                LOG_DEBUG("tcp_thread_run: 5, no element\n");   
                break;
            }
        }

        ctx->loop_state = 7;
        //todo, need to use epoll, and need to handle global lists
        sys_check_timeouts();

        ctx->loop_state = 8;
        tx_flush();

        ctx->loop_state = 9;
        uint64_t now = get_now();
        if (now - prev_ts > 1000000000UL)
        {

            LOG_INFO("tcp_thread_run: 3: ctx_id: %d, pkt_cnt: %lu, input_ring num: %d\n", 
                    ctx->id, pkt_cnt, rte_ring_count(ctx->input_pkt_ring));
            prev_ts = now;
        }

        ctx->loop_state = 10;

        // sleep(1);        
    }

    // return NULL;
    return 0;
}

void tcp_thread_input_ring_notify(struct tcp_thread_ctx *ctx, uint64_t val)
{
    if (write(ctx->input_event_fd, &val, sizeof(val)) != sizeof(val))
    {
        perror("write eventfd error");
    }
}

int tcp_thread_init(struct tcp_thread_ctx* ctx, int id, int core_id)
{
    int ret = 0;;
    ctx->id = id;
    ctx->running = true;
    char ring_name[32];

    LOG_DEBUG("tcp_thread_init: 1, begin, id: %d\n", id);


    ret = eventfd(0, 0);
    if  (ret < 0)
    {   perror("eventfd create error");
        return ret;
    }
    ctx->input_event_fd = ret;

    /* 2. 创建多生产者单消费者无锁队列 */
    snprintf(ring_name, 32, "tcp_input_ring%d", id);
    ctx->input_pkt_ring = rte_ring_create(ring_name, TCP_THREAD_INPUT_RING_SIZE,
            rte_socket_id(), RING_F_SC_DEQ);    /* 单消费者出队标志 */
    if  (ctx->input_pkt_ring == NULL)
    {   LOG_INFO("create input ring failed\n");
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
    rte_ring_free(ctx->input_pkt_ring);
}


// ip thread class ---------------------------
struct ip_thread_ctx {
    pthread_t pthread_ctx;
    struct netif* _netif;
    int id;
    volatile int running;

};


unsigned short netif_poll_once(struct netif* _netif_p, int queue_id);
void netif_rx_test_sleep(unsigned short nb_rx, uint16_t queue_id);



int 
/* void*  */ip_thread_run(void* arg)
{
    struct ip_thread_ctx* ctx = (struct ip_thread_ctx*)arg;
    uint64_t cnt = 0;
    uint64_t prev_ts = 0;
    uint64_t pkt_cnt = 0;
    // death loop  poll pkt only wait process exit

    while (ctx->running)
    {
        // LOG_DEBUG("ip_thread_run: 2\n");

        unsigned short nb_rx = netif_poll_once(ctx->_netif, ctx->id);
        pkt_cnt += nb_rx;

        tx_flush();


        // netif_rx_test_sleep(nb_rx, ctx->id);

        if  (++cnt % 10000 == 0)
        {
            // usleep(1);
            uint64_t now = get_now();
            if  (now  - prev_ts > 1000000000UL)
            {
                LOG_INFO("ip_thread_run: 3: ctx_id: %d, pkt_cnt: %lu\n", ctx->id, pkt_cnt);
                prev_ts = now;
            }
        }
    }
    

    // ret = rte_ring_enqueue(g_ring, obj);

    // return NULL;
    return 0;
}

int ip_thread_init(struct ip_thread_ctx* ctx, int id, int core_id, struct netif* nif)
{
    int ret = 0;

    ctx->id = id;
    ctx->running = true;
    ctx->_netif = nif;

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