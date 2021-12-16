//
// Created by 60458 on 2021/11/12.
//
#include "network.h"

void init_network(char* ip,char* port){
    printf("init server,server addr %s,prot %s\n",ip,port);
    server = (struct network*)malloc(sizeof(struct network));
    server->server_ip = sdsnew(ip);
    server->server_port = sdsnew(port);
    server->zmq_context = zmq_ctx_new();
    server->zmq_socket = zmq_socket(server->zmq_context,ZMQ_DEALER);
}
void destroy_network(){
    zmq_close(server->zmq_socket);
    zmq_ctx_destroy(server->zmq_context);
    sdsfree(server->server_ip);
    sdsfree(server->server_port);
    free(server);
}


int Bind(){
    puts("====server binding====");
    sds local_addr = sdsnew("tcp://");
    sdscat(local_addr,server->server_ip);
    sdscat(local_addr,":");
    sdscat(local_addr,server->server_port);
    printf("server bind to %s\n",local_addr);
    if(zmq_bind(server->zmq_socket,local_addr) != 0) {
        return -1;
    }
    return 1;
}

int connect_server(){
    puts("=====connecting to server...======");
    sds remote_addr = sdsnew("tcp://");
    printf("server:%s,port:%s\n",server->server_ip,server->server_port);
    sdscat(remote_addr,server->server_ip);
    sdscat(remote_addr,":");
    sdscat(remote_addr,server->server_port);
    printf("server address: %s\n",remote_addr);
    if(zmq_connect(server->zmq_socket,remote_addr)!=0){
        return -1;
    }
    return 1;
}

int send_msg(char* send_buf,int len){
    zmq_msg_t msg;

    //send_bytes += len;

    if(zmq_msg_init_size(&msg,len) != 0){
        return -1;
    }
    memcpy(zmq_msg_data(&msg),send_buf,len);

    if(zmq_msg_send(&msg,server->zmq_socket,0) < 0) {
        return -2;
    }
    if(zmq_msg_close(&msg) != 0)
        return -3;
    return 1;
}

int recv_msg(char* recv_buf,int* len){
    zmq_msg_t msg;
    if(zmq_msg_init(&msg) != 0) {
        printf("init msg error\n");
        return -1;
    }
    if(zmq_msg_recv(&msg,server->zmq_socket,0) < 0){
        printf("recv msg error\n");
        return -2;
    }
//    char *p = (char*) ;
    int msg_len = zmq_msg_size(&msg);
    memcpy(recv_buf,zmq_msg_data(&msg),msg_len);
    *len = msg_len;

//    zmq_getsockopt (server->zmq_socket, ZMQ_RCVMORE, &more, &more_size);

    if(zmq_msg_close(&msg) != 0)
        return -3;
    return 1;
}


