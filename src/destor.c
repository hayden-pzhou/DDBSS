//
// Created by 60458 on 2021/11/14.
//

#include "destor.h"
#include "index/index.h"
#include "jcr.h"
#include "net/network.h"
#include "utils/serial.h"

extern void do_backup(char *path);
extern void do_restore(int revision, char *path);
extern int load_config();
extern void load_config_from_string(sds config);

/* : means argument is required.
 * :: means argument is required and no space.
 */
const char * const short_options = "sr::t::p::h";
const char * const restore_tmp_dir = "/home/data/working/restore/";
static char buf[512];
//static char send_buf[DEFAULT_BLOCK_SIZE];
static int32_t msg_len = 0;

struct option long_options[] = {
        { "state", 0, NULL, 's' },
        { "help", 0, NULL, 'h' },
        { NULL, 0, NULL, 0 }
};

void destor_log(int level, const char *fmt, ...) {
    va_list ap;
    char msg[DESTOR_MAX_LOGMSG_LEN];

    if ((level & 0xff) < destor.verbosity)
        return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    fprintf(stdout, "%s\n", msg);
}

void check_simulation_level(int last_level, int current_level) {
    if ((last_level <= SIMULATION_RESTORE && current_level >= SIMULATION_APPEND)
        || (last_level >= SIMULATION_APPEND
            && current_level <= SIMULATION_RESTORE)) {
        fprintf(stderr, "FATAL ERROR: Conflicting simualtion level!\n");
        exit(1);
    }
}

void destor_start(){
    /* Init*/
    destor.working_directory = sdsnew("/home/data/working/");
    destor.verbosity = DESTOR_WARNING;
    destor.simulation_level = SIMULATION_NO;
    destor.restore_cache[0] = RESTORE_CACHE_LRU;
    destor.restore_cache[1] = 1024;
    destor.restore_opt_window_size = 1000000;

    destor.index_category[0] = INDEX_CATEGORY_NEAR_EXACT;
    destor.index_category[1] = INDEX_CATEGORY_PHYSICAL_LOCALITY;
    destor.index_specific = INDEX_SPECIFIC_NO;
    destor.index_key_value_store = INDEX_KEY_VALUE_HTABLE;
    destor.index_key_size = 20;
    destor.index_value_length = 1;

    destor.index_cache_size = 4096;

    destor.update_rate=100;

    destor.index_segment_algorithm[0] = INDEX_SEGMENT_FIXED;
    destor.index_segment_algorithm[1] = 1024;
    destor.index_segment_min = 128;
    destor.index_segment_max = 10240;
    destor.index_sampling_method[0] = INDEX_SAMPLING_UNIFORM;
    destor.index_sampling_method[1] = 1;
    destor.index_value_length = 1;
    destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_TOP;
    destor.index_segment_selection_method[1] = 1;
    destor.index_segment_prefech = 0;

    destor.rewrite_algorithm[0] = REWRITE_NO;
    destor.rewrite_algorithm[1] = 1024;

    /* for History-Aware Rewriting (HAR) */
    destor.rewrite_enable_har = 0;
    destor.rewrite_har_utilization_threshold = 0.5;
    destor.rewrite_har_rewrite_limit = 0.05;


    /* for Cache-Aware Filter */
    destor.rewrite_enable_cache_aware = 0;

    /*
     * Specify how many backups are retained.
     * A negative value indicates all backups are retained.
     */
    destor.backup_retention_time = -1;

    load_config();

    sds stat_file = sdsdup(destor.working_directory);
    stat_file = sdscat(stat_file, "/destor.stat");

    FILE *fp;
    if((fp = fopen(stat_file,"r"))){
        fread(&destor.chunk_num, 8, 1, fp);
        fread(&destor.stored_chunk_num, 8, 1, fp);

        fread(&destor.data_size, 8, 1, fp);
        fread(&destor.stored_data_size, 8, 1, fp);

        fread(&destor.zero_chunk_num, 8, 1, fp);
        fread(&destor.zero_chunk_size, 8, 1, fp);

        fread(&destor.rewritten_chunk_num, 8, 1, fp);
        fread(&destor.rewritten_chunk_size, 8, 1, fp);

        fread(&destor.index_memory_footprint, 4, 1, fp);

        fread(&destor.live_container_num, 4, 1, fp);

        int last_retention_time;
        fread(&last_retention_time, 4, 1, fp);
        assert(last_retention_time == destor.backup_retention_time);

        int last_level;
        fread(&last_level, 4, 1, fp);
        check_simulation_level(last_level, destor.simulation_level);

        fclose(fp);
    } else{
        destor.chunk_num = 0;
        destor.stored_chunk_num = 0;
        destor.data_size = 0;
        destor.stored_data_size = 0;
        destor.zero_chunk_num = 0;
        destor.zero_chunk_size = 0;
        destor.rewritten_chunk_num = 0;
        destor.rewritten_chunk_size = 0;
        destor.index_memory_footprint = 0;
        destor.live_container_num = 0;
    }
    sdsfree(stat_file);
}

void destor_shutdown() {
    sds stat_file = sdsdup(destor.working_directory);
    stat_file = sdscat(stat_file, "/destor.stat");

    FILE *fp;
    if ((fp = fopen(stat_file, "w")) == 0) {
        destor_log(DESTOR_WARNING, "Fatal error, can not open destor.stat!");
        exit(1);
    }

    fwrite(&destor.chunk_num, 8, 1, fp);
    fwrite(&destor.stored_chunk_num, 8, 1, fp);

    fwrite(&destor.data_size, 8, 1, fp);
    fwrite(&destor.stored_data_size, 8, 1, fp);

    fwrite(&destor.zero_chunk_num, 8, 1, fp);
    fwrite(&destor.zero_chunk_size, 8, 1, fp);

    fwrite(&destor.rewritten_chunk_num, 8, 1, fp);
    fwrite(&destor.rewritten_chunk_size, 8, 1, fp);

    fwrite(&destor.index_memory_footprint, 4, 1, fp);

    fwrite(&destor.live_container_num, 4, 1, fp);

    fwrite(&destor.backup_retention_time, 4, 1, fp);

    fwrite(&destor.simulation_level, 4, 1, fp);

    fclose(fp);
    sdsfree(stat_file);
}

int handle_msg(){
    destor_start();//启动
    char msg_type = buf[0];
    int ret = 1;
    switch (msg_type) {
        case BACKUP_START_REQ: {
            sds path = sdsnewlen(NULL,msg_len);
            memcpy(path, buf + 1, msg_len - 1);
            struct  timeval t0,t1;
            gettimeofday(&t0, NULL);
            printf("Backup path is %s\n",path);
            char buf[8];
            buf[0] = BACKUP_START_REP;
            buf[1] = 1;
            send_msg(buf,2);
            printf("notify backup!\n");
            do_backup(path);
            gettimeofday(&t1, NULL);
            sdsfree(path);
            break;
        }
        case BACKUP_END_REQ:
            break;
        case RESTORE_START_REQ: {
            int revision = -1;
            char bv[128];
            unser_declare;
            unser_begin(buf+1,128);
            unser_int32(revision);
            unser_end(buf+1,128);
            char rsp[8];
            rsp[0] = RESTORE_START_REP;
            rsp[1] = 1;
            send_msg(rsp,2);
            printf("restore vision: %d\n",revision);
            do_restore(revision,restore_tmp_dir);
            break;
        }
    }
    destor_shutdown();//结束
    return ret;
}

int main(int argc,char **argv){
    int ret = 0;
    init_network("*","8899");//启动网络服务
    if((ret = Bind()) < 0){
        fprintf(stderr,"Network bind failed\n");
    }
    while (1) {
        if((ret = recv_msg(buf,&msg_len)) < 0){
            fprintf(stderr, "Network Recv failed: %d\n", ret);
            return -1;
        }
        if((ret = handle_msg()) < 0)
        {
            fprintf(stderr, "HandleMsg failed: %d\n", ret);
            return -1;
        }
    }
}

struct chunk* new_chunk(int32_t size) {
    struct chunk* ck = (struct chunk*) malloc(sizeof(struct chunk));

    ck->flag = CHUNK_UNIQUE;
    ck->id = TEMPORARY_ID;
    memset(&ck->fp, 0x0, sizeof(fingerprint));
    ck->size = size;

    if (size > 0)
        ck->data = malloc(size);
    else
        ck->data = NULL;

    return ck;
}

void free_chunk(struct chunk* ck) {
    if (ck->data) {
        free(ck->data);
        ck->data = NULL;
    }
    free(ck);
}

struct segment* new_segment() {
    struct segment * s = (struct segment*) malloc(sizeof(struct segment));
    s->id = TEMPORARY_ID;
    s->chunk_num = 0;
    s->chunks = g_sequence_new(NULL);
    s->features = NULL;
    return s;
}

struct segment* new_segment_full(){
    struct segment* s = new_segment();
    s->features = g_hash_table_new_full(g_feature_hash, g_feature_equal, free, NULL);
    return s;
}

void free_segment(struct segment* s) {
    GSequenceIter *begin = g_sequence_get_begin_iter(s->chunks);
    GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
    for(; begin != end; begin = g_sequence_get_begin_iter(s->chunks)){
        free_chunk(g_sequence_get(begin));
        g_sequence_remove(begin);
    }
    g_sequence_free(s->chunks);

    if (s->features)
        g_hash_table_destroy(s->features);

    free(s);
}

gboolean g_fingerprint_equal(fingerprint* fp1, fingerprint* fp2) {
    return !memcmp(fp1, fp2, sizeof(fingerprint));
}

gint g_fingerprint_cmp(fingerprint* fp1, fingerprint* fp2, gpointer user_data) {
    return memcmp(fp1, fp2, sizeof(fingerprint));
}

gint g_chunk_cmp(struct chunk* a, struct chunk* b, gpointer user_data){
    return memcmp(&a->fp, b->fp, sizeof(fingerprint));
}

void hash2code(unsigned char hash[20], char code[40]) {
    int i, j, b;
    unsigned char a, c;
    i = 0;
    for (i = 0; i < 20; i++) {
        a = hash[i];
        for (j = 0; j < 2; j++) {
            b = a / 16;
            switch (b) {
                case 10:
                    c = 'A';
                    break;
                case 11:
                    c = 'B';
                    break;
                case 12:
                    c = 'C';
                    break;
                case 13:
                    c = 'D';
                    break;
                case 14:
                    c = 'E';
                    break;
                case 15:
                    c = 'F';
                    break;
                default:
                    c = b + 48;
                    break;

            }
            code[2 * i + j] = c;
            a = a << 4;
        }
    }
}

void code2hash(unsigned char code[40], unsigned char hash[20]) {
    bzero(hash, 20);
    int i, j;
    unsigned char a, b;
    for (i = 0; i < 20; i++) {
        for (j = 0; j < 2; j++) {
            a = code[2 * i + j];
            switch (a) { //A is equal to a
                case 'A':
                    b = 10;
                    break;
                case 'a':
                    b = 10;
                    break;
                case 'B':
                    b = 11;
                    break;
                case 'b':
                    b = 11;
                    break;
                case 'C':
                    b = 12;
                    break;
                case 'c':
                    b = 12;
                    break;
                case 'D':
                    b = 13;
                    break;
                case 'd':
                    b = 13;
                    break;
                case 'E':
                    b = 14;
                    break;
                case 'e':
                    b = 14;
                    break;
                case 'F':
                    b = 15;
                    break;
                case 'f':
                    b = 15;
                    break;
                default:
                    b = a - 48;
                    break;
            }
            hash[i] = hash[i] * 16 + b;
        }
    }
}