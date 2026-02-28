# Harder-to-Reproduce Index Recovery Performance Results

I tested recovery of 1 and 12 shards of ~20 Gigs each. The size makes it a bit challenging to package nicely in a reproducible benchmark, although I am sure it can be done.
I am confident you can reproduce this behavior with a comparable amount of data and cloud structure. I can share the scripts I used to achieve these results if it is helpful.

## Results Summary

| Scenario | Shards | Configuration | Result | Time |
|----------|--------|---------------|--------|------|
| HTTP/2 | 1 | default | Fast | ~40s |
| HTTP/1 | 1 | default | Fast | ~50s |
| HTTP/1 | 12 | default | Fast | ~90s |
| HTTP/2 | 12 | default | Slowest | ~320s |
| HTTP/2 | 12 | `maxConcurrentStreams=1`| Slower | ~180s |

