//
// Created by 60458 on 2021/11/10.
//

#ifndef DDBSC_NETWORK_H
#define DDBSC_NETWORK_H

#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "utils/sds.h"

//#include <string.h>

struct network{
    sds server_ip;
    sds server_port;
    void* zmq_context;
    void* zmq_socket;
};

static struct network* server;

void init_network(char* ip,char* port);
void destroy_network();
int Bind();
int connect_server();
int send_msg(char* send_buf,int len);
int recv_msg(char* recv_buf,int* len);


#endif //DDBSC_NETWORK_H
