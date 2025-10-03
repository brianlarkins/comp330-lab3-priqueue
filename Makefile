CC=clang
CFLAGS=-g -Wall -Werror -Wno-unused -pthread

all: lab3

lab3: lab3.o priority_queue.o
	$(CC) $(CFLAGS) -o lab3 lab3.o priority_queue.o

lab3.o: lab3.c priority_queue.h
	$(CC) $(CFLAGS) -c lab3.c

priority_queue.o: priority_queue.c priority_queue.h
	$(CC) $(CFLAGS) -c priority_queue.c

clean:
	rm -f *.o *~ lab3 queue_log.csv

