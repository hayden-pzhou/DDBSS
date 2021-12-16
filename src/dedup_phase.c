//
// Created by 60458 on 2021/11/11.
//

#include "destor.h"
#include "utils/sync_queue.h"
#include "index/index.h"
#include "backup.h"

static pthread_t dedup_t;
static int64_t chunk_num;
static int64_t segment_num;

struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;  //index buffer is not full
    // index buffer is full,waitting
    // if threshold < 0,it indicates no threshold
    int wait_threshold;
}index_lock;

void send_segment(struct segment* s){
    /*
     * CHUNK_SEGMENT_START and _END are used for
     * reconstructing the segment in filter phase.
    */
    struct chunk* ss = new_chunk(0);
    SET_CHUNK(ss,CHUNK_SEGMENT_START);
    /*
    *if rate less grater value,set update flag.
    */
    /*
    if( rate >= destor.update_rate) {
        NOTICE("segment's fingerprint not need update! rate is %d update_rate %d",rate,destor.update_rate);
        SET_CHUNK(ss, CHUNK_SEGMENT_FLAG);
    }
     */
    sync_queue_push(dedup_queue,ss);

    GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
    GSequenceIter *begin = g_sequence_get_begin_iter(s->chunks);
    while (begin != end){
        struct chunk* c = g_sequence_get(begin);
        if (!CHECK_CHUNK(c, CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)) {
            if (CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
                if (c->id == TEMPORARY_ID) {
                    DEBUG("Dedup phase: %ldth chunk is identical to a unique chunk",
                          chunk_num++);
                } else {
                    DEBUG("Dedup phase: %ldth chunk is duplicate in container %lld",
                          chunk_num++, c->id);
                }
            } else {
                DEBUG("Dedup phase: %ldth chunk is unique", chunk_num++);
            }
        }
        sync_queue_push(dedup_queue,c);
        g_sequence_remove(begin);
        begin = g_sequence_get_begin_iter(s->chunks);
    }

    struct chunk* se = new_chunk(0);
    SET_CHUNK(se, CHUNK_SEGMENT_END);
    sync_queue_push(dedup_queue, se);

    s->chunk_num=0;

}

void *dedup_thread(void *arg) {
    struct segment* s = NULL;
    int rate = 1;
    while (1){
        struct chunk *c = NULL;
        c= sync_queue_pop(net_queue);
        /* Add the chunk to the segment.*/
        s = segmenting(c);
        if(!s)
            continue;
        /* segmenting success */
        if(s->chunk_num > 0) {
            VERBOSE("Dedup phase: the %lldth segment of %lld chunks",segment_num++,s->chunk_num);
            /* Each duplicate chunk will be marked. */
            pthread_mutex_lock(&index_lock.mutex);
            while (index_lookup(s)==0){
                pthread_cond_wait(&index_lock.cond, &index_lock.mutex);
            }
            pthread_mutex_unlock(&index_lock.mutex);
        }else{
            VERBOSE("Dedup phase: an empty segment");
        }
        /* Send chunks in the sengment to the next phase.
         * The segment will be cleared. */
        send_segment(s);
        free_segment(s);
        s=NULL;

        if(c == NULL)
            break;
    }
    sync_queue_term(dedup_queue);
    return NULL;
}

void start_dedup_phase(){
    if(destor.index_segment_algorithm[0] == INDEX_SEGMENT_CONTENT_DEFINED)
        index_lock.wait_threshold = destor.rewrite_algorithm[1] + destor.index_segment_max - 1;
    else if(destor.index_segment_algorithm[0] == INDEX_SEGMENT_FIXED)
        index_lock.wait_threshold = destor.rewrite_algorithm[1] + destor.index_segment_algorithm[1] - 1;
    else
        index_lock.wait_threshold = -1; // file-defined segmenting has no threshold.
    printf("before init mutex\n");
    pthread_mutex_init(&index_lock.mutex, NULL);
    pthread_cond_init(&index_lock.cond, NULL);
    printf("after init mutex\n");
    dedup_queue = sync_queue_new(1000);
    printf("Dedup Phase:starting dedup pipeline\n");
    pthread_create(&dedup_t,NULL,dedup_thread,NULL);
}


void stop_dedup_phase() {

    pthread_join(dedup_t, NULL);
    pthread_mutex_destroy(&index_lock.mutex);
    pthread_cond_destroy(&index_lock.cond);
//    sync_queue_free(dedup_queue,free_chunk);
    WARNING("Dedup Phase: stop dedup pipeline successfully: %d segments of %d chunks on average",
           segment_num, segment_num ? chunk_num / segment_num : 0);
}