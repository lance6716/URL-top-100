CFLAGS = -O3 -Wall

.PHONY: all clean

all: top100
top100: top100.o murmur3.o
	$(CC) $^ -o $@

clean:
	rm -rf top100 *.o buckets/
