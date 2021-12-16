//
// Created by 60458 on 2021/11/11.
//

#ifndef DDBSS_BACKUP_H
#define DDBSS_BACKUP_H

#include "destor.h"
#include "utils/sync_queue.h"

void start_net_phase();
void stop_net_phase();

void start_dedup_phase();
void stop_dedup_phase();

void start_rewrite_phase();
void stop_rewrite_phase();

void start_filter_phase();
void stop_filter_phase();

SyncQueue* net_queue;
SyncQueue* dedup_queue;
SyncQueue* rewrite_queue;



#endif //DDBSS_BACKUP_H
