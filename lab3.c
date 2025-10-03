#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "priority_queue.h"
#include "util.h"

#define MAX_PRODUCERS 10 
#define MAX_CONSUMERS 10
#define QUEUE_CAPACITY 10
#define NUM_ITEMS 20


global_t g;


/**
 * producer - producer thread function
 * @param arg thread id
 * @return NULL
 */
void *producer(void* arg) {
  int id = *((int*)arg);
  for (int i = 0; i < g.npitems; i++) {
    int item = rand() % 100;
    int priority = rand() % 10;
    pq_enqueue(&g.pq, item, priority, id);
    printf("  [Producer %d] Enqueued %d (priority %d)\n", id, item, priority);
    usleep(rand() % 50000);
  }
  return NULL;
}




/**
 * consumer - consumer thread function
 * @param arg thread id
 * @return NULL
 */
void *consumer(void* arg) {
  int id = *((int*)arg);
  for (int i = 0; i < g.ncitems; i++) {
    int priority;
    int item = pq_dequeue(&g.pq, &priority, id);
    printf("  [Consumer %d] Dequeued %d (priority %d)\n", id, item, priority);
    usleep(rand() % 70000);
  }
  return NULL;
}



int main(int argc, char **argv) {
  int nitems = NUM_ITEMS, qsize = QUEUE_CAPACITY, opt;
  pthread_t producers[MAX_PRODUCERS], consumers[MAX_CONSUMERS];
  int ids[MAX_PRODUCERS > MAX_CONSUMERS ? MAX_PRODUCERS : MAX_CONSUMERS];

  memset(&g, 0, sizeof(g));

  while ((opt = getopt(argc, argv, "c:dlp:n:q:")) != -1) {
    switch (opt) {
      case 'c':
        g.nconsumers = atoi(optarg);
        break;
      case 'd':
        g.debug = 1;
        break;
      case 'l':
        g.logging = 1;
      case 'p':
        g.nproducers = atoi(optarg);
        break;
      case 'n':
        nitems = atoi(optarg);
        break;
      case 'q':
        qsize = atoi(optarg);
        break;
      default:
        fprintf(stderr, "Usage: %s [-d] [-l] [-c num_consumers] [-p num_producers] [-n num_items] [-q queue_size]\n", argv[0]);
        exit(EXIT_FAILURE);
    }
  }


  // seed random number generator
  srand(time(NULL));

  pq_init(&g.pq, qsize);

  if (g.logging) {
    g.logfile = fopen("queue_log.csv", "w");
    perror("fopen");
    goto error;
  }

  // make sure total item counts are divisible
  nitems = g.nproducers * g.nconsumers * nitems;
  g.npitems = nitems / g.nproducers;
  g.ncitems = nitems / g.nconsumers;

  for (int i = 0; i < g.nproducers; i++) {
    ids[i] = i;
    pthread_create(&producers[i], NULL, producer, &ids[i]);
  }
  for (int i = 0; i < g.nconsumers; i++) {
    ids[i] = i;
    pthread_create(&consumers[i], NULL, consumer, &ids[i]);
  }

  for (int i = 0; i < g.nproducers; i++) {
    pthread_join(producers[i], NULL);
  }
  for (int i = 0; i < g.nconsumers; i++) {
    pthread_join(consumers[i], NULL);
  }

  if (g.logging)
    fclose(g.logfile);

error:
  pq_destroy(&g.pq);

  return 0;
}

