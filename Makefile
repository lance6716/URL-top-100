CXXFLAGS = -O3 -Wall

.PHONY: all clean

all: top100
top100: top100.o takeapart.o murmur3.o
	$(CXX) $^ -o $@

clean:
	rm -rf top100 *.o buckets/
