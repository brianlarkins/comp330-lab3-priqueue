#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>

#include "priority_queue.h"
#include "util.h"

/**
* get_usec_timestamp - returns current time in microseconds
* @return current time in microseconds
*/
static long get_usec_timestamp() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (tv.tv_sec * 1000000L) + tv.tv_usec;
}




/**
 * swap - swaps two heap items
 * @param a first item
 * @param b second item
 */
static void swap(pq_item_t *a, pq_item_t *b) {
  pq_item_t tmp = *a;
  *a = *b;
  *b = tmp;
}



/**
 * heapify_up - restores heap property going up from idx
 * @param heap the heap array
 * @param idx the starting index
 */
static void heapify_up(pq_item_t *heap, int idx) {
  while (idx > 0) {
    int parent = (idx - 1) / 2;
    if (heap[parent].priority >= heap[idx].priority)
      break;
    swap(&heap[parent], &heap[idx]);
    idx = parent;
  }
}



/**
 * heapify_down - restores heap property going down from idx
 * @param heap the heap array
 * @param size the current size of the heap
 * @param idx the starting index
 */
static void heapify_down(pq_item_t *heap, int size, int idx) {
  while (1) {
    int left = 2 * idx + 1;
    int right = 2 * idx + 2;
    int largest = idx;

    if (left < size && heap[left].priority > heap[largest].priority)
      largest = left;
    if (right < size && heap[right].priority > heap[largest].priority)
      largest = right;

    if (largest == idx) break;

    swap(&heap[idx], &heap[largest]);
    idx = largest;
  }
}



/**
 * pq_init - initializes the priority queue
 * @param pq the priority queue
 * @param capacity the maximum capacity
 */
void pq_init(priority_queue_t *pq, int capacity) {
  pq->data = malloc(sizeof(pq_item_t) * capacity);
  pq->capacity = capacity;
  pq->size = 0;
  pthread_mutex_init(&pq->lock, NULL);
  pthread_cond_init(&pq->not_full, NULL);
  pthread_cond_init(&pq->not_empty, NULL);
}



/**
 * pq_destroy - cleans up the priority queue
 * @param pq the priority queue
 */
void pq_destroy(priority_queue_t *pq) {
  free(pq->data);
  pthread_mutex_destroy(&pq->lock);
  pthread_cond_destroy(&pq->not_full);
  pthread_cond_destroy(&pq->not_empty);
}



/**
 * pq_enqueue - adds an item with priority to the queue
 * @param pq the priority queue
 * @param item the item to add
 * @param priority the priority of the item
 * @param logfile the log file (can be NULL)
 * @param tid the thread id for logging
 */
void pq_enqueue(priority_queue_t *pq, int item, int priority, int tid) {
  printf("enqueue %d locking\n", tid);
  pthread_mutex_lock(&pq->lock);

  printf("enqueue %d size %d cap %d\n", tid, pq->size, pq->capacity);
  while (pq->size == pq->capacity) {
    printf("enqueue %d waiting\n", tid);
    pthread_cond_wait(&pq->not_full, &pq->lock);
    printf("enqueue %d waking\n", tid);
  }

  pq->data[pq->size].item = item;
  pq->data[pq->size].priority = priority;
  heapify_up(pq->data, pq->size);
  pq->size++;

  if (g.logfile) {
    fprintf(g.logfile, "%ld,Producer-%d,enqueue,%d,priority=%d,queue_size=%d\n",
        get_usec_timestamp(), tid, item, priority, pq->size);
    fflush(g.logfile);
  }

  printf("enqueue %d signaling consumer\n", tid);
  pthread_cond_signal(&pq->not_empty);
  pthread_mutex_unlock(&pq->lock);
  printf("enqueue %d yielding\n", tid);
}



/**
 * pq_dequeue - removes and returns the highest priority item
 * @param pq the priority queue
 * @param priority_out pointer to store the priority of the dequeued item
 * @param logfile the log file (can be NULL)
 * @param tid the thread id for logging
 * @return the dequeued item
 */
int pq_dequeue(priority_queue_t *pq, int *priority_out, int tid) {
  printf("dequeue %d locking\n", tid);
  pthread_mutex_lock(&pq->lock);

  printf("dequeue %d size %d cap %d\n", tid, pq->size, pq->capacity);
  while (pq->size == 0) {
    printf("dequeue %d waiting\n", tid);
    pthread_cond_wait(&pq->not_empty, &pq->lock);
    printf("dequeue %d waking\n", tid);
  }

  int item = pq->data[0].item;
  *priority_out = pq->data[0].priority;

  pq->size--;
  pq->data[0] = pq->data[pq->size];
  heapify_down(pq->data, pq->size, 0);

  if (g.logfile) {
    fprintf(g.logfile, "%ld,Consumer-%d,dequeue,%d,priority=%d,queue_size=%d\n",
        get_usec_timestamp(), tid, item, *priority_out, pq->size);
    fflush(g.logfile);
  }

  printf("dequeue %d signaling producer\n", tid);
  pthread_cond_signal(&pq->not_full);
  pthread_mutex_unlock(&pq->lock);
  printf("dequeue %d yielding\n", tid);

  return item;
}
