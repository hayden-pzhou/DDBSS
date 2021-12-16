cmake_minimum_required(VERSION 3.16)
project(DDBSS C)

set(CMAKE_C_STANDARD 11)

include_directories(src)
include_directories(src/index)
include_directories(src/net)
include_directories(src/recipe)
include_directories(src/storage)
include_directories(src/utils)
include_directories(/usr/local/include/glib-2.0/)
include_directories(/usr/local/lib/glib-2.0/include/)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2 -g -Wall")

add_executable(DDBS_server
        src/destor.h
        src/destor.c
        src/net/network.h
        src/net/network.c
        src/index/fingerprint_cache.h
        src/index/fingerprint_cache.c
        src/index/index.h
        src/index/index.c
        src/index/index_buffer.h
        src/index/kvstore.h
        src/index/kvstore.c
        src/index/kvstore_htable.c
        src/index/sampling_method.c
        src/index/segmenting_method.c
        src/index/similarity_detection.c
        src/recipe/recipestore.h
        src/recipe/recipestore.c
        src/storage/containerstore.h
        src/storage/containerstore.c
        src/backup.h
        src/net_phase.c
        src/utils/bloom_filter.h
        src/utils/bloom_filter.c
        src/utils/lru_cache.h
        src/utils/lru_cache.c
        src/utils/serial.h
        src/utils/serial.c
        src/utils/sds.h
        src/utils/sds.c
        src/utils/queue.h
        src/utils/queue.c
        src/utils/sync_queue.h
        src/utils/sync_queue.c
        src/config.c
        src/dedup_phase.c
        src/filter_phase.c
        src/do_backup.c
        src/cap_rewrite.c
        src/cbr_rewrite.c
        src/cfl_rewrite.c
        src/rewrite_phase.h
        src/rewrite_phase.c
        src/restore_aware.c
        src/har_rewrite.c
        src/cma.h
        src/cma.c
        src/jcr.h
        src/jcr.c
        src/do_restore.c
        src/restore.h
        src/optimal_restore.c
        src/assembly_restore.c)

target_link_libraries(DDBS_server glib-2.0 pthread zmq)
