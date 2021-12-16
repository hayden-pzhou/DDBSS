//
// Created by 60458 on 2021/11/18.
//

#ifndef DDBSS_RESTORE_H
#define DDBSS_RESTORE_H

#include "utils/sync_queue.h"

SyncQueue *restore_chunk_queue;
SyncQueue *restore_recipe_queue;

void* assembly_restore_thread(void *arg);
void* optimal_restore_thread(void *arg);

#endif //DDBSS_RESTORE_H
