CXXFLAGS = -O3 -Wall -pthread

.PHONY: all clean

all: top100
top100: top100.o takeapart.o murmur3.o
	$(CXX) $(CXXFLAGS) $^ -o $@

clean:
	rm -rf top100 *.o buckets/
