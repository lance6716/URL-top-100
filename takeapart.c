#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include "murmur3.h"

/* A randomly generated URL file shows that 10k URLs has a size of
 * 334KB, we could estimate 100GB file has about 3e9 URLs. Samples from
 * real world may have longer URL thus less amount.
 *
 * We use a uint32 to count URL, overflow will happen when all URL are 
 * same and less than 25 Bytes. TODO: deal with this.
 *
 * Assuming we have 250 buckets, and each URL has same probability goes
 * into buckets, thus the number of URL of a bucket follows Binomial
 * distribution with paramter N=3e9, p=1/250. And this distribution
 * could be approximated by Poisson distribution with lambda=N*p=1.2e7 
 *
 * 
 */

int take_apart() {
    const char *INFILE = "infile.txt";
    const char *BUCKETFOLDER = "buckets";
    const size_t URLMAXLEN = 2084;
    FILE *infilefp;
    FILE *bucketfp[256];
    size_t linelen = URLMAXLEN;
    ssize_t nread;
    char *line = (char *)malloc(linelen * sizeof(char));
    char pathbuffer[100]; //TODO: magic number

    uint32_t hash[4];
    uint32_t seed = 6716;

    if ((infilefp = fopen(INFILE, "r")) == NULL) {
        printf("error when open file, errno = %d\n", errno);
        exit(EXIT_FAILURE);
    }

    if (mkdir(BUCKETFOLDER, S_IRWXU) == -1 && errno != EEXIST) {
        printf("error when create folder, errno = %d\n", errno);
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < 256; i++) {
        sprintf(pathbuffer, "%s/%08x", BUCKETFOLDER, i); //TODO: overflow
        bucketfp[i] = fopen(pathbuffer, "w"); //TODO: if fail
    }

    //TODO: URL may exceed? notify it
    while ((nread = getline(&line, &linelen, infilefp)) != -1) {
        printf("%s", line);
        MurmurHash3_x64_128(line, nread, seed, hash);
        printf("x64_128: %08x %08x %08x %08x\n",
               hash[0], hash[1], hash[2], hash[3]);
        uint32_t i = hash[0] & 0xff;
        fprintf(bucketfp[i], "%s", line);
//        fprintf(bucketfp[i], "hash: %08x %08x %08x %08x, URL: %s",
//                hash[0], hash[1], hash[2], hash[3], line); //TODO: fail
    }


    for (uint32_t i = 0; i < 256; i++) {
        fclose(bucketfp[i]);
    }
    fclose(infilefp);
    if (line) {
        free(line);
    }
    return 0;
}
