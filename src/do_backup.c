//
// Created by 60458 on 2021/11/12.
//

#include "destor.h"
#include "jcr.h"
#include "utils/sync_queue.h"
#include "index/index.h"
#include "backup.h"
#include "storage/containerstore.h"

/*
 * defined in index.c
 */

extern struct {
    /* Requests to the key-value store */
    int lookup_requests;
    int update_requests;
    int lookup_requests_for_unique;
    /* Overheads of prefetching module */
    int read_prefetching_units;
    int cache_hits;
}index_overhead;

void do_backup(char *path){

    init_recipe_store(); //初始化recipe
    init_container_store();//初始化容器存储
    init_index();//初始化索引模块

    init_backup_jcr(path);
    puts("==== backup begin====");
    TIMER_DECLARE(1);
    TIMER_BEGIN(1);

    time_t start = time(NULL);

    start_net_phase();
    start_dedup_phase();
    start_rewrite_phase();
    start_filter_phase();

    do{
        usleep(100);
        /*time_t now = time(NULL);*/
//        fprintf(stderr,"job %" PRId32 ", %" PRId64 " bytes, %" PRId32 " chunks, %d files processed\r",
//                jcr.id, jcr.data_size, jcr.chunk_num, jcr.file_num);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
//    fprintf(stderr,"job %" PRId32 ", %" PRId64 " bytes, %" PRId32 " chunks, %d files processed\n",
//            jcr.id, jcr.data_size, jcr.chunk_num, jcr.file_num);

    stop_net_phase();
    stop_dedup_phase();
    stop_rewrite_phase();
    stop_filter_phase();

    TIMER_END(1, jcr.total_time);

    close_index();
    close_container_store();
    close_recipe_store();
    update_backup_version(jcr.bv);
    free_backup_version(jcr.bv);

    puts("==== backup end ====");

    char logfile[] = "backup.log";
    FILE *fp = fopen(logfile, "a");
    /*
 * job id,
 * the size of backup
 * accumulative consumed capacity,
 * deduplication rate,
 * rewritten rate,
 * total container number,
 * sparse container number,
 * inherited container number,
 * 4 * index overhead (4 * int)
 * throughput,
 */
    fprintf(fp, "%" PRId32 " %" PRId64 " %" PRId64 " %.4f %.4f %" PRId32 " %" PRId32
                " %" PRId32 " %" PRId32 " %" PRId32 " %" PRId32" %" PRId32 " %" PRId32" %.2f\n",
            jcr.id,
            jcr.data_size,
            destor.stored_data_size,
            jcr.data_size != 0 ?
            (jcr.data_size - jcr.rewritten_chunk_size - jcr.unique_data_size)/(double) (jcr.data_size)
                               : 0,
            jcr.data_size != 0 ? (double) (jcr.rewritten_chunk_size) / (double) (jcr.data_size) : 0,
            jcr.total_container_num,
            jcr.sparse_container_num,
            jcr.inherited_sparse_num,
            index_overhead.lookup_requests,
            index_overhead.lookup_requests_for_unique,
            index_overhead.update_requests,
            index_overhead.read_prefetching_units,
            index_overhead.cache_hits,
            (double) jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));

    fclose(fp);

}