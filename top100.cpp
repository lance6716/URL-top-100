#include <iostream>
#include <fstream>
#include <unordered_map>
#include <queue>
#include <vector>
#include <cstring>
#include <inttypes.h>
#include <stdio.h>

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
    char *url;
    Urlfreq(int c, char *u) : count(c) {
        url = (char *)malloc((strlen(u) + 1) * sizeof(char));
        sprintf(url, "%s", u);
    }
    ~Urlfreq() {
        free(url);
    }
};

struct Comp {
    bool operator()(const Urlfreq *a, const Urlfreq *b) {
        return a->count > b->count;
    }
};

typedef std::unordered_map<Url, int, UrlHash, UrlEqual> CounterMap;

void countWords(FILE *in, CounterMap& words) {
    uint32_t a, b, c, d;
    char urlbuffer[URL_MAXLEN];

    //TODO: need serialization and deserialization
    while (fscanf(in, "%" PRIu32 " %" PRIu32 " %" PRIu32 " %" PRIu32 " %s",
                  &a, &b, &c, &d, urlbuffer) != EOF) { 
        Url *tmp = new Url(a, b, c, d, urlbuffer);
        ++words[*tmp]; //TODO: copy-construction or move or pass reference?
    }
}

int main(int argc, char** argv) {
    if (take_apart(INFILE) != 0) { //TODO: argv[1]
        exit(EXIT_FAILURE);
    }

    char pathbuffer[PATH_MAXLEN];
    char urlbuffer[URL_MAXLEN];
    int c;
    std::priority_queue<Urlfreq *, std::vector<Urlfreq *>, Comp> pq;
    std::vector<Urlfreq *> result;
    FILE *infilefp;

    for (auto i = 0; i < BUCKET_NUM; i++) {
        sprintf(pathbuffer, "%s/%08x", BUCKET_FOLDER, i);
        if ((infilefp = fopen(pathbuffer, "r")) == NULL) {
            printf("error when open file, errno = %d\n", errno);
            exit(EXIT_FAILURE);
        }
        sprintf(pathbuffer, "%s/%08x.out", BUCKET_FOLDER, i);
        std::ofstream out(pathbuffer);
            
        CounterMap w;
        countWords(infilefp, w);

        for (CounterMap::iterator p = w.begin(); p != w.end(); ++p) {
            out << p->second << " " << p->first.url_with_prefix + 3 * 8 << "\n";
        }
        fclose(infilefp);
        out.close();
        // std::cout << "map is going to destruction" << std::endl;
    }

    for (auto i = 0; i < BUCKET_NUM; i++) {
        sprintf(pathbuffer, "%s/%08x.out", BUCKET_FOLDER, i);
        if ((infilefp = fopen(pathbuffer, "r")) == NULL) {
            printf("error when open file, errno = %d\n", errno);
            exit(EXIT_FAILURE);
        }

        while(fscanf(infilefp, "%d %s", &c, urlbuffer) != EOF) {
            Urlfreq *uf = new Urlfreq(c, urlbuffer);
            pq.push(uf);
            if (pq.size() > TOPK) {
                delete pq.top();
                pq.pop();
            }
        }
        fclose(infilefp);
    }

    while (pq.size() > 0) {
        result.push_back(pq.top());
        pq.pop();
    }

    for (auto it = result.crbegin(); it != result.crend(); it++) {
        std::cout << (*it)->count << " " << (*it)->url << "\n";
    }
}
