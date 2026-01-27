# Reproducible /Stream Performance Results

## Scenario: HTTP/2 with 1 shard

**Result: Slow**

```bash
./jmh.sh -p useHttp1=false -p nodeCount=2 -p numShards=1 -p numReplicas=2 -p docCount=10000 -p indexThreads=14 -p batchSize=500 -p docSizeBytes=10024 -p numTextFields=25 StreamingSearch
```

| Benchmark | batchSize | docCount | docSizeBytes | indexThreads | nodeCount | numReplicas | numShards | numTextFields | useHttp1 | Mode | Cnt | Score | Error | Units |
|-----------|-----------|----------|--------------|--------------|-----------|-------------|-----------|---------------|----------|------|-----|-------|-------|-------|
| StreamingSearch.stream | 500 | 10000 | 10024 | 14 | 2 | 2 | 1 | 25 | false | thrpt | 4 | 0.257 | ± 0.308 | ops/s |

---

## Scenario: HTTP/2 with 12 shards

**Result: Hangs indefinitely**

```bash
./jmh.sh -p useHttp1=false -p nodeCount=2 -p numShards=12 -p numReplicas=2 -p docCount=10000 -p indexThreads=14 -p batchSize=500 -p docSizeBytes=10024 -p numTextFields=25 StreamingSearch
```

It appears server's flow control forcibly sets recvWindow to 0 on the offending client. There appear to be too many concurrent streams fighting for the same piece of "session window". I'm attaching flow-control-stall.log which gives a more detailed view of this response stall.

---

## Scenario: HTTP/2 with 12 shards and LOWER initialStreamRecvWindow

**Result: Slow**

```bash
./jmh.sh -p useHttp1=false -p nodeCount=2 -p numShards=12 -p numReplicas=2 -p docCount=10000 -p indexThreads=14 -p batchSize=500 -p docSizeBytes=10024 -p numTextFields=25 -jvmArgs -Dsolr.http2.initialStreamRecvWindow=500000 StreamingSearch
```

| Benchmark | batchSize | docCount | docSizeBytes | indexThreads | nodeCount | numReplicas | numShards | numTextFields | useHttp1 | Mode | Cnt | Score | Error | Units |
|-----------|-----------|----------|--------------|--------------|-----------|-------------|-----------|---------------|----------|------|-----|-------|-------|-------|
| StreamingSearch.stream | 500 | 10000 | 10024 | 14 | 2 | 2 | 12 | 25 | false | thrpt | 4 | 0.344 | ± 0.027 | ops/s |

The reason setting initialStreamRecvWindow lower helps avoid the stall is because each response gets sequestered to a smaller portion of the total session window pool and so each shard progresses consistently, albeit slowly. This result is still poor.

---

## Scenario: HTTP/2 with 12 shards and HIGHER initialSessionRecvWindow

**Result: Slow**

```bash
./jmh.sh -p useHttp1=false -p nodeCount=2 -p numShards=12 -p numReplicas=2 -p docCount=10000 -p indexThreads=14 -p batchSize=500 -p docSizeBytes=10024 -p numTextFields=25 -jvmArgs -Dsolr.http2.initialStreamRecvWindow=8000000 -jvmArgs -Dsolr.http2.initialSessionRecvWindow=96000000 StreamingSearch
```

| Benchmark | batchSize | docCount | docSizeBytes | indexThreads | nodeCount | numReplicas | numShards | numTextFields | useHttp1 | Mode | Cnt | Score | Error | Units |
|-----------|-----------|----------|--------------|--------------|-----------|-------------|-----------|---------------|----------|------|-----|-------|-------|-------|
| StreamingSearch.stream | 500 | 10000 | 10024 | 14 | 2 | 2 | 12 | 25 | false | thrpt | 4 | 0.332 | ± 0.050 | ops/s |

In other words, increasing the client-side response window doesn't help.

---

## Scenario: HTTP/1 with 12 shards

**Result: Fast**

```bash
./jmh.sh -p useHttp1=true -p nodeCount=2 -p numShards=12 -p numReplicas=2 -p docCount=10000 -p indexThreads=14 -p batchSize=500 -p docSizeBytes=10024 -p numTextFields=25 -jvmArgs -Dsolr.http1=true StreamingSearch
```

| Benchmark | batchSize | docCount | docSizeBytes | indexThreads | nodeCount | numReplicas | numShards | numTextFields | useHttp1 | Mode | Cnt | Score | Error | Units |
|-----------|-----------|----------|--------------|--------------|-----------|-------------|-----------|---------------|----------|------|-----|-------|-------|-------|
| StreamingSearch.stream | 500 | 10000 | 10024 | 14 | 2 | 2 | 12 | 25 | true | thrpt | 4 | 2.301 | ± 0.676 | ops/s |
