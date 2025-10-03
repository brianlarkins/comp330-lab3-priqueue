
#pragma once

#include <pthread.h>
#include <stdio.h>

typedef struct {
    int item;
    int priority;
} pq_item_t;

typedef struct {
    pq_item_t *data;        // heap array
    int capacity;           // max size
    int size;               // current size
    pthread_mutex_t lock;   // mutex for critical section
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} priority_queue_t;

void pq_init(priority_queue_t *pq, int capacity);
void pq_destroy(priority_queue_t *pq);

void pq_enqueue(priority_queue_t *pq, int item, int priority, int tid);
int  pq_dequeue(priority_queue_t *pq, int *priority_out, int tid);
