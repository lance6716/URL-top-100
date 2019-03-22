#include <iostream>
#include <fstream>
#include <map>
#include <queue>
#include <vector>
#include <string>

typedef std::map<std::string, int> StrIntMap;

struct Urlfreq {
    int count;
    std::string url;
    Urlfreq(int c, std::string u) : count(c), url(std::move(u)) {}
};

struct Comp {
    bool operator()(const Urlfreq &a, const Urlfreq &b) {
        return a.count > b.count;
    }
};

void countWords(std::istream& in, StrIntMap& words) {
   std::string s;

   while (in >> s) {
      ++words[s];
   }
}

int main(int argc, char** argv) {
    char pathbuffer[100];
    int c;
    std::string s;

    std::priority_queue<Urlfreq, std::vector<Urlfreq>, Comp> pq;
    std::vector<Urlfreq> result;

    const char *FOLDER = "buckets";
    for (uint32_t i = 0; i < 256; i++) {
        sprintf(pathbuffer, "%s/%08x", FOLDER, i);
        std::ifstream in(pathbuffer);
        sprintf(pathbuffer, "%s/%08x.out", FOLDER, i);
        std::ofstream out(pathbuffer);

        if (!in)
            exit(EXIT_FAILURE);

        StrIntMap w;
        countWords(in, w);

        for (StrIntMap::iterator p = w.begin();
            p != w.end(); ++p) {
            out << p->second << " " << p->first << "\n";
        }
    }

    for (uint32_t i = 0; i < 256; i++) {
        sprintf(pathbuffer, "%s/%08x.out", FOLDER, i);
        std::ifstream in(pathbuffer);

        while(in >> c >> s) {
            Urlfreq uf = Urlfreq(c, s);
            pq.push(uf);
            if (pq.size() > 100) {
                pq.pop();
            }
        }
    }

    while (pq.size() > 0) {
        result.push_back(pq.top());
        pq.pop();
    }

    for (auto it = result.crbegin(); it != result.crend(); it++) {
        std::cout << it->count << " " << it->url << "\n";
    }
}
