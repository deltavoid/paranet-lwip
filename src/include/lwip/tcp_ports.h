#ifndef TCP_PORTS_H
#define TCP_PORTS_H

#include "lwip/arch.h"
#include <stdbool.h>


u16_t tcp_port_get_new();

bool tcp_port_check_used(u16_t port);

int tcp_port_ref_add(u16_t port, int num);
int tcp_port_ref_dec(u16_t port, int num);





#endif  // TCP_PORTS_H
