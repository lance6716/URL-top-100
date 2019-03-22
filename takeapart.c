#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/stat.h>
#include "murmur3.h"
#include "utils.h"

/* A randomly generated URL file shows that 10k URLs has a size of
 * 334KB, we could estimate 100GB file has about 3e9 URLs. Samples from
 * real world may have longer URL thus less amount.
 *
 * We use a uint32 to count URL, overflow will happen when all URL are 
 * same and less than 25 Bytes. TODO: deal with this.
 *
 * First consider no two URLs are same.
 *
 * Assuming we have 250 buckets, and each URL has same probability goes
 * into buckets, thus the number of URL of a bucket follows Binomial
 * distribution with paramter N=3e9, p=1/250. And this distribution
 * could be approximated by Poisson distribution with lambda=N*p=1.2e7 
 * 
 */

int take_apart(const char *infile) {
    FILE *infilefp;
    FILE *bucketfp[BUCKET_NUM];
    size_t linelen = URL_MAXLEN;
    ssize_t nread;
    char *line = (char *)malloc(linelen * sizeof(char));
    char pathbuffer[PATH_MAXLEN];

    uint32_t hash[4];
    uint32_t seed = 6716;

    if ((infilefp = fopen(infile, "r")) == NULL) {
        printf("error when open file, errno = %d\n", errno);
        return -1;
    }

    if (mkdir(BUCKET_FOLDER, S_IRWXU) == -1 && errno != EEXIST) {
        printf("error when create folder, errno = %d\n", errno);
        return -1;
    }

    for (uint32_t i = 0; i < BUCKET_NUM; i++) {
        sprintf(pathbuffer, "%s/%08x", BUCKET_FOLDER, i);
        if ((bucketfp[i] = fopen(pathbuffer, "w")) == NULL) {
            printf("error when open file, errno = %d\n", errno);
            return -1;
        }
    }

    while ((nread = getline(&line, &linelen, infilefp)) != -1) {
        // printf("%s", line);
        MurmurHash3_x64_128(line, nread, seed, hash);
        // printf("x64_128: %08x %08x %08x %08x\n",
        //        hash[0], hash[1], hash[2], hash[3]);
        uint32_t i = hash[0] & (BUCKET_NUM - 1);
        if (fprintf(bucketfp[i], "%"PRIu32" %"PRIu32" %"PRIu32" %"PRIu32" %s", 
            hash[0], hash[1], hash[2], hash[3], line) < 0) {
            printf("error when fprintf to bucket files, errno = %d\n", errno);
        };
    }


    for (uint32_t i = 0; i < BUCKET_NUM; i++) {
        fclose(bucketfp[i]);
    }
    fclose(infilefp);
    if (line) {
        free(line);
    }
    return 0;
}
