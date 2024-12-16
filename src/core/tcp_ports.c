/*
 * for tcp ports management
 * @author ZhangQianyu
*/

#include "lwip/tcp_ports.h"



u16_t tcp_port_get_new()
{
    return 0;
}

bool tcp_port_check_used(u16_t port)
{
    LWIP_UNUSED_ARG(port);
    
    return false;
}

int tcp_port_ref_add(u16_t port, int num)
{
    LWIP_UNUSED_ARG(port);
    LWIP_UNUSED_ARG(num);

    return 0;
}

int tcp_port_ref_dec(u16_t port, int num)
{
    LWIP_UNUSED_ARG(port);
    LWIP_UNUSED_ARG(num);

    return 0;
}

