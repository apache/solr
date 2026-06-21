#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>

//  gcc drop-caches.c -o drop-caches
//  sudo chown root:root drop-caches
//  sudo chmod 4755 drop-caches

int main(void) {
    printf("Dropping caches...\n");
    if (geteuid() != 0) {
        fprintf(stderr, "Not root\n");
        exit(EXIT_FAILURE);
    }
    if (system("sync") != 0) {
        fprintf(stderr, "sync failed\n");
        exit(EXIT_FAILURE);
    }

    FILE* f;
    f = fopen("/proc/sys/vm/drop_caches", "w");
    if (f == NULL) {
        fprintf(stderr, "Could not open /proc/sys/vm/drop_caches\n");
        exit(EXIT_FAILURE);
    }
    if (fprintf(f, "3\n") != 2) {
        fprintf(stderr, "Could not write 3 to /proc/sys/vm/drop_caches\n");
        exit(EXIT_FAILURE);
    }
    fclose(f);
    printf("Caches dropped.\n");

    return 0;
}
