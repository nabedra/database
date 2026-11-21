FROM nabedra-oltp-perf-builder:1.94.1
RUN apt-get update && apt-get install -y --no-install-recommends strace && rm -rf /var/lib/apt/lists/*
