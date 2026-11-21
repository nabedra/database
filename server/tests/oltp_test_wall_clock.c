// Test-only process clock jumps. Keep CLOCK_MONOTONIC on the real clock.
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

int clock_gettime(clockid_t clock, struct timespec *value)
{
    int result = syscall(SYS_clock_gettime, clock, value);
    if (result != 0 || clock != CLOCK_REALTIME)
        return result;

    int saved_errno = errno;
    const char *directory = getenv("NABEDRA_OLTP_TEST_BARRIER_DIR");
    if (directory != NULL) {
        char path[PATH_MAX];
        int size = snprintf(path, sizeof path, "%s/clock-offset-seconds", directory);
        if (size > 0 && (size_t)size < sizeof path) {
            int fd = open(path, O_RDONLY | O_CLOEXEC);
            if (fd >= 0) {
                char offset[16];
                ssize_t read_size = read(fd, offset, sizeof offset - 1);
                close(fd);
                if (read_size > 0) {
                    offset[read_size] = '\0';
                    value->tv_sec += strtol(offset, NULL, 10);
                }
            }
        }
    }
    errno = saved_errno;
    return result;
}
