#include <iostream>
#include <unordered_map>
#include <queue>
#include <chrono>
#include <vector>
#include <cstring>
#include <inttypes.h>
#include <pthread.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/resource.h>

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
    Url(const Url &other) {
        hash = other.hash;
        size_t len = strlen(other.url_with_prefix) + 1;
        url_with_prefix = (char *)malloc(len * sizeof(char));
        memcpy(url_with_prefix, other.url_with_prefix, len);
    }
    ~Url() {
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
    uint32_t count;
    char *url;
    Urlfreq(uint32_t c, char *u) : count(c) {
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

typedef std::unordered_map<Url, uint32_t, UrlHash, UrlEqual> CounterMap;

void countWords(FILE *in, CounterMap& counter) {
    uint32_t a, b, c, d;
    char urlbuffer[URL_MAXLEN];

    //TODO: need serialization and deserialization
    while (fscanf(in, "%" PRIu32 " %" PRIu32 " %" PRIu32 " %" PRIu32 " %s",
                  &a, &b, &c, &d, urlbuffer) != EOF) { 
        Url *tmp = new Url(a, b, c, d, urlbuffer);
        ++counter[*tmp];
        delete tmp;
    }
}

long getrss() {
    FILE *fp = NULL;
    long rss = 0;
    char *line = NULL;
    size_t len = 0;
    if ((fp = fopen("/proc/self/status", "r")) == NULL) {
        printf("error when get memory\n");
        exit(EXIT_FAILURE);
    }
    while (getline(&line, &len, fp) != -1) {
        if (strncmp(line, "VmRSS", 5) == 0) {
            sscanf(line, "%*s%ld", &rss);
            break;
        }
    }
    fclose(fp);
    free(line);
    // std::cout << "rss " << rss << std::endl;
    return rss;
}

pthread_mutex_t numlock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t ready = PTHREAD_COND_INITIALIZER;
int threadnum = 0;
long rss = 0;
pthread_t thid;

void *bucket_work(void *arg) {
    char pathbuffer[PATH_MAXLEN];
    FILE *infilefp;
    FILE *outfilefp;
    int i = *((int *)arg);

    sprintf(pathbuffer, "%s/%08x", BUCKET_FOLDER, i);
    if ((infilefp = fopen(pathbuffer, "r")) == NULL) {
        printf("error when open file, errno = %d\n", errno);
        exit(EXIT_FAILURE);
    }
    sprintf(pathbuffer, "%s/%08x.out", BUCKET_FOLDER, i);
    if ((outfilefp = fopen(pathbuffer, "w")) == NULL) {
        printf("error when open file, errno = %d\n", errno);
        exit(EXIT_FAILURE);
    }

    CounterMap w;
    countWords(infilefp, w);

    for (CounterMap::iterator p = w.begin(); p != w.end(); ++p) {
        fprintf(outfilefp, "%" PRIu32 " %s\n", 
                p->second, p->first.url_with_prefix + 3 * 8);
    }
    fclose(infilefp);
    fclose(outfilefp);

    pthread_mutex_lock(&numlock);
    --threadnum;
    rss = getrss();
    pthread_cond_signal(&ready);
    pthread_mutex_unlock(&numlock);
    free(arg);
    pthread_exit(0);
}

void assign_jobs(int &i) {
    int *arg = (int *)malloc(sizeof(int));
    *arg = i;
    int err = pthread_create(&thid, NULL, bucket_work, arg);
    if (err != 0) {
        printf("error when pthread_create, err = %d\n", err);
        exit(EXIT_FAILURE);
    }
    ++threadnum;
    ++i;
}

int main(int argc, char** argv) {
    auto start = std::chrono::high_resolution_clock::now();

    if (take_apart(INFILE) != 0) { //TODO: argv[1]
        exit(EXIT_FAILURE);
    }

    char pathbuffer[PATH_MAXLEN];

    auto finish = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = finish - start;
    std::cout << "1 Elapsed time: " << elapsed.count() << " s\n";
    start = finish;

    /* Stage 1 complete */ 

    /* Use RSS increment of one thread to estimate thread limits */
    int i = 0;
    long beforerss = getrss();

    pthread_mutex_lock(&numlock);
    assign_jobs(i);
    pthread_cond_wait(&ready, &numlock);
    long rss_per_thread = rss - beforerss;
    rss_per_thread = rss_per_thread < 1 ? 1 : rss_per_thread;
    int maxthread = 500 * 1000 / rss_per_thread; /* use totally 500M */
    maxthread = maxthread < 12 ? maxthread : 12;
    pthread_mutex_unlock(&numlock);

    while (i < BUCKET_NUM) {
        pthread_mutex_lock(&numlock);
        if (threadnum < maxthread) {
            assign_jobs(i);
        } else {
            pthread_cond_wait(&ready, &numlock);
        }
        pthread_mutex_unlock(&numlock);
    }

    pthread_mutex_lock(&numlock);
    while (threadnum > 0) {
        pthread_cond_wait(&ready, &numlock);
    }
    pthread_mutex_unlock(&numlock);

    finish = std::chrono::high_resolution_clock::now();
    elapsed = finish - start;
    std::cout << "2 Elapsed time: " << elapsed.count() << " s\n";
    start = finish;

    /* Stage 2 complete */

    char urlbuffer[URL_MAXLEN];
    FILE *infilefp;
    uint32_t count;
    std::priority_queue<Urlfreq *, std::vector<Urlfreq *>, Comp> pq;
    std::vector<Urlfreq *> result;

    for (auto i = 0; i < BUCKET_NUM; i++) {
        sprintf(pathbuffer, "%s/%08x.out", BUCKET_FOLDER, i);
        if ((infilefp = fopen(pathbuffer, "r")) == NULL) {
            printf("error when open file, errno = %d\n", errno);
            exit(EXIT_FAILURE);
        }

        while(fscanf(infilefp, "%" PRIu32 " %s", &count, urlbuffer) != EOF) {
            Urlfreq *uf = new Urlfreq(count, urlbuffer);
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
    } // ! need free elements in result vector if append code
    finish = std::chrono::high_resolution_clock::now();
    elapsed = finish - start;
    std::cout << "3 Elapsed time: " << elapsed.count() << " s\n";

    /* Stage 3 complete */
}
