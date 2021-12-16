//
// Created by 60458 on 2021/11/11.
//
#include "destor.h"
#include "backup.h"
#include "utils/serial.h"
#include "net/network.h"
#include "jcr.h"

#define NET_BUFFSIZE 1048576

static pthread_t net_t;
static char recv_buf[NET_BUFFSIZE];
static int msg_len = 0;

void* net_thread(void *arg){
    /**
     * 接收所有的chunk
     */
    struct chunk *c = NULL;
    while (1){
        recv_msg(recv_buf,&msg_len);
        if(recv_buf[0] == BACKUP_END_REQ ){
            sync_queue_term(net_queue);
            //发送结束标志
            break;
        }
        int size;
        unser_declare;
        unser_begin(recv_buf+1,NET_BUFFSIZE);
        unser_int32(size);
        c = new_chunk(size);
        unser_int32(c->flag);
        unser_int64(c->id);
        unser_bytes(&c->fp, sizeof(fingerprint));
        if(c->size > 0){
            unser_bytes(c->data,c->size);
        }
        unser_end(recv_buf+1,NET_BUFFSIZE);
        sync_queue_push(net_queue,c);
    }
    return NULL;
}

void start_net_phase(){
    /* running job */
    jcr.status = JCR_STATUS_RUNNING;
    net_queue = sync_queue_new(100);
    pthread_create(&net_t,NULL,net_thread,NULL);
    printf("Net phase:start net pipeline\n");
}

void stop_net_phase(){
    pthread_join(net_t,NULL);
//    sync_queue_free(net_queue,free_chunk);
    WARNING("Net Phase: stop net pipeline successfully!");
}