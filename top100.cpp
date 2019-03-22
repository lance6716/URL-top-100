#include <iostream>
#include <fstream>
#include <unordered_map>
#include <queue>
#include <vector>
#include <cstring>
#include <string>
#include "takeapart.h"
#include "utils.h" 

struct Url {
    size_t hash;
    char *url_with_prefix;
    Url(uint32_t a, uint32_t b, uint32_t c, uint32_t d, const char *url) {
        hash = d;
        url_with_prefix = (char *)malloc((strlen(url) + 1 + 3 * 8) * sizeof(char));
        //TODO: save len, avoid strlen

        sprintf(url_with_prefix, "%08x%08x%08x%s", a, b, c, url);
    }
    ~Url() {
        // std::cout << "Url destruction" << std::endl;
        free(url_with_prefix);
    }
};

struct UrlHash {
    size_t operator()(const Url &url) const {
        return url.hash;
    }
};

struct UrlEqual {
    bool operator()(const Url &a, const Url &b) const {
        return strcmp(a.url_with_prefix, b.url_with_prefix) == 0;
    }
};

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

typedef std::unordered_map<Url, int, UrlHash, UrlEqual> CounterMap;

void countWords(std::istream& in, CounterMap& words) {
    std::string s;
    uint32_t a, b, c, d;

    //TODO: need serialization and deserialization
    while (in >> a >> b >> c >> d >> s) { 
        Url *tmp = new Url(a, b, c, d, s.c_str());
        ++words[*tmp];
    }
}

int main(int argc, char** argv) {
    if (take_apart(INFILE) != 0) { //TODO: argv[1]
        exit(EXIT_FAILURE);
    }

    char pathbuffer[PATH_MAXLEN];
    int c;
    std::string s;
    std::priority_queue<Urlfreq, std::vector<Urlfreq>, Comp> pq;
    std::vector<Urlfreq> result;

    for (auto i = 0; i < BUCKET_NUM; i++) {
        sprintf(pathbuffer, "%s/%08x", BUCKET_FOLDER, i);
        std::ifstream in(pathbuffer);
        sprintf(pathbuffer, "%s/%08x.out", BUCKET_FOLDER, i);
        std::ofstream out(pathbuffer);

        if (!in)
            exit(EXIT_FAILURE);

        CounterMap w;
        countWords(in, w);

        for (CounterMap::iterator p = w.begin();
            p != w.end(); ++p) {
            out << p->second << " " << p->first.url_with_prefix + 3 * 8 << "\n";
        }
        in.close();
        out.close();
        // std::cout << "map is going to destruction" << std::endl;
    }

    for (auto i = 0; i < BUCKET_NUM; i++) {
        sprintf(pathbuffer, "%s/%08x.out", BUCKET_FOLDER, i);
        std::ifstream in(pathbuffer);

        while(in >> c >> s) {
            Urlfreq uf = Urlfreq(c, s);
            pq.push(uf);
            if (pq.size() > TOPK) {
                pq.pop();
            }
        }
        in.close();
    }

    while (pq.size() > 0) {
        result.push_back(pq.top());
        pq.pop();
    }

    for (auto it = result.crbegin(); it != result.crend(); it++) {
        std::cout << it->count << " " << it->url << "\n";
    }
}
