> Resource-aware scheduling of database queries and mutations across a worker cluster

## Worker configuration and resource limits

- Each worker/node should handle multiple partitions. We should be able to configure the database such that we can allocate a max number of workers per request, query or whatever, and a max number of threads per worker.
- The developer should be able to specify how many threads to run per worker, although that should also be specified by each worker. Whenever we add a worker to the cluster, we might have to specify the max number of concurrent threads it can run.
- The goal of the max partition size should also be to limit the amount of RAM a thread can consume.

## Concurrent requests and worker utilization

- We might have two queries coming in at the same time, so the server should be able to allocate/distribute the right amount of queries through the workers, again to maximize utilization.
- Check whether our distribution algorithm is able to handle multiple incoming and running requests at the same time, and if we are able to efficiently max out the capacity of the workers. The goal is to maximize the CPU and RAM usage of the workers.
- No matter how weird the request, query or whatever, the distribution algorithm should work well with the queue and when maxing out the capacity.

## Queue retention and restarts

- We should have a queue of sorts to not lose any request that comes in but that finds the resources of the database maxed out. Check whether requests are lost even when having a queue.
- The queue should survive if at least one worker is running and has processed it. If all workers, the entire server, are shut down, then the query/request should be ignored.

## Failure isolation and transactional mutations

- If something fails in some thread, it should not affect anything at any of the other threads!
- It should also be fast and efficient by not using try/catches but something else, like smart ifs, checks and type checks and so on.
- We need to make changes transactional in some way as to be fault-tolerant, or keep track of a mutation somewhere temporarily, such that when the server is restarted it goes back to normal. The simplest way is to make everything transactional for mutations, such that everything mutates accordingly or nothing does. This can be stored in memory in the cluster, perhaps the most efficient and cleaner way versus having to make changes to a temp file in the S3 bucket.

## Deployment topology and autoscaling

- There shouldn't be a difference between `serve` and `api` + `worker`. We create a cluster of machines/workers, which receive requests by some separate API, perhaps another Kubernetes pod, another AWS EC2 instance or something. You could say that one serves and the other joins, but the act of joining the cluster is also serving it, in some sense.
- Autoscaling will of course come in, but only if we have Kubernetes or AWS integrated.