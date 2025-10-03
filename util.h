#pragma once

#include <stdio.h>
#include <stdarg.h>

#include "priority_queue.h"

struct global_s {
  priority_queue_t pq;
  FILE *logfile;
  int debug;
  int logging;
  int nproducers;
  int nconsumers;
  int npitems;
  int ncitems;
};
typedef struct global_s global_t;

extern global_t g;

int dprint(const char *str, ...);
