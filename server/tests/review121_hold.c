// LD_PRELOAD shim for the ACC3-ready-fence review test (PR #121 review).
// When the process writes R121_MARK to stderr, the writing thread reports
// R121_HOLD.reached and waits for R121_HOLD.release. Product code is unchanged.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

static void hold(const void *buffer, size_t length) {
    const char *path = getenv("R121_HOLD"), *mark = getenv("R121_MARK");
    if (!path || !mark || !memmem(buffer, length, mark, strlen(mark))) return;
    char name[4096];
    snprintf(name, sizeof name, "%s.reached", path);
    if (access(name, F_OK) == 0) return;
    FILE *file = fopen(name, "w");
    if (file) fclose(file);
    snprintf(name, sizeof name, "%s.release", path);
    while (access(name, F_OK) != 0) usleep(2000);
}

ssize_t write(int fd, const void *buffer, size_t length) {
    static ssize_t (*real)(int, const void *, size_t);
    if (!real) real = dlsym(RTLD_NEXT, "write");
    ssize_t done = real(fd, buffer, length);
    if (fd == 2 && done > 0) hold(buffer, (size_t)done);
    return done;
}

ssize_t writev(int fd, const struct iovec *parts, int count) {
    static ssize_t (*real)(int, const struct iovec *, int);
    if (!real) real = dlsym(RTLD_NEXT, "writev");
    ssize_t done = real(fd, parts, count);
    if (fd == 2 && done > 0)
        for (int index = 0; index < count; index++) hold(parts[index].iov_base, parts[index].iov_len);
    return done;
}
