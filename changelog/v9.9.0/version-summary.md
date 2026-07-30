<!-- @formatter:off -->
<!-- noinspection -->
<!-- Prevents auto format, for JetBrains IDE File > Settings > Editor > Code Style (Formatter Tab) > Turn formatter on/off with markers in code comments  -->

<!-- This file is automatically generate by logchange tool 🌳 🪓 => 🪵 -->
<!-- Visit https://github.com/logchange/logchange and leave a star 🌟 -->
<!-- !!! ⚠️ DO NOT MODIFY THIS FILE, YOUR CHANGES WILL BE LOST ⚠️ !!! -->


[9.9.0] - 2025-07-24
--------------------

### Added (9 changes)

- Certificate based authentication plugin now has richer flexible cert principal resolution. [SOLR-17309](https://issues.apache.org/jira/browse/SOLR-17309) (Lamine Idjeraoui) (Eric Pugh)
- Support terminating a search early based on maxHitsAllowed per shard. [SOLR-17447](https://issues.apache.org/jira/browse/SOLR-17447) (Siju Varghese) (Houston Putman) (David Smiley) (Gus Heck)
- Add RawTFSimilarityFactory class. [SOLR-17626](https://issues.apache.org/jira/browse/SOLR-17626) (Christine Poerschke)
- Added update request processor to encode text to vector at indexing time through external LLM services. [SOLR-17632](https://issues.apache.org/jira/browse/SOLR-17632) (Alessandro Benedetti)
- New 'skipLeaderRecovery' replica property allows PULL replicas with existing indexes to immediately become ACTIVE [SOLR-17656](https://issues.apache.org/jira/browse/SOLR-17656) (hossman)
- ReRank queries can now return the originalScore (original score) in addition to the re-ranked score. [SOLR-17678](https://issues.apache.org/jira/browse/SOLR-17678) (Siju Varghese) (Houston Putman)
- Added a FuzzyQParser to enable all FuzzyQuery customizations. [SOLR-17714](https://issues.apache.org/jira/browse/SOLR-17714) (Houston Putman) (Siju Varghese)
- Added linear function support for RankField via RankQParserPlugin. [SOLR-17749](https://issues.apache.org/jira/browse/SOLR-17749) (Christine Poerschke)
- New ExpressionValueSourceParser that allows custom function queries / VSPs to be defined in a subset of JavaScript, pre-compiled, and that which can access the score and fields. It's powered by the Lucene Expressions module. [SOLR-5707](https://issues.apache.org/jira/browse/SOLR-5707) (hossman) (David Smiley) (Ryan Ernst) (Kevin Risden)

### Changed (21 changes)

- v2 APIs now obey the "Accept" request header for content-negotiation if 'wt' is unspecified. JSON is still used as a default when neither 'Accept' or 'wt' are specified. [SOLR-10998](https://issues.apache.org/jira/browse/SOLR-10998) (Jason Gerlowski)
- The v2 API now has parity with the v1 "COLSTATUS" and "segments" APIs, which can be used to fetch detailed information about specific collections or cores. Collection information can be fetched by a call to `GET /api/collections/collectionName`, and core information with a call to `GET /api/cores/coreName/segments`. [SOLR-15751](https://issues.apache.org/jira/browse/SOLR-15751) (Jason Gerlowski)
- All v2 configset APIs have been moved to the slightly different path: `/api/configsets`, to better align with the design of other v2 APIs. SolrJ now offers (experimental) SolrRequest implementations for all v2 configset APIs in `org.apache.solr.client.solrj.request.ConfigsetsApi`. [SOLR-16396](https://issues.apache.org/jira/browse/SOLR-16396) (Jason Gerlowski)
- Add PKI Auth Caching for both generation of the PKI Auth Tokens and validation of received tokens. The default PKI Auth validity TTL has been increased from 5 seconds to 10 seconds. [SOLR-16951](https://issues.apache.org/jira/browse/SOLR-16951) (Houston Putman)
- edismax MatchAllDocsQuery (*:*) Optimization [SOLR-17130](https://issues.apache.org/jira/browse/SOLR-17130) (Kevin Risden)
- The polling interval for PULL and TLOG replicas can now be overridden using the `commitPollInterval` setting, which takes a String formatted as "HH:mm:ss" [SOLR-17187](https://issues.apache.org/jira/browse/SOLR-17187) (Torsten Koster) (Christine Poerschke) (Jason Gerlowski)
- Solr's filestore "get-file" API has been decomposed into several separate endpoints. Traditional file-fetching is now available at `GET /api/cluster/filestore/files/some/path.txt` and metadata fetching (and directory listing) is now available at `GET /api/cluster/filestore/metadata/some/path.txt`. SolrJ now offers request and response bindings for these APIs in `org.apache.solr.client.solrj.request.FileStoreApi`. The older form of this endpoint (`GET /api/node/files`) is deprecated and will be removed. [SOLR-17351](https://issues.apache.org/jira/browse/SOLR-17351) (Jason Gerlowski)
- Remove ZkController internal core supplier, for slightly faster reconnection after Zookeeper session loss. [SOLR-17578](https://issues.apache.org/jira/browse/SOLR-17578) (Pierre Salagnac)
- The CLUSTERSTATUS API will now stream each collection's status to the response, fetching and computing it on the fly. To avoid a backwards compatibility concern, this won't work for wt=javabin. [SOLR-17582](https://issues.apache.org/jira/browse/SOLR-17582) (Matthew Biscocho) (David Smiley)
- SolrJ CloudSolrClient configured with HTTP URLs will no longer eagerly connect to anything. [SOLR-17607](https://issues.apache.org/jira/browse/SOLR-17607) (David Smiley)
- Export metric timers via `wt=prometheus` as Prometheus summaries. [SOLR-17628](https://issues.apache.org/jira/browse/SOLR-17628) (Jude Muriithi) (Matthew Biscocho)
- Reduced memory usage in SolrJ getBeans() method when handling dynamic fields with wildcards. [SOLR-17669](https://issues.apache.org/jira/browse/SOLR-17669) (Martin Anzinger)
- Score-based return fields other than "score" can now be returned in distributed queries. [SOLR-17732](https://issues.apache.org/jira/browse/SOLR-17732) (Houston Putman)
- Solr now supports Jetty's Graceful Shutdown module (via SOLR_JETTY_GRACEFUL=true) to prevent client connections from being abruptly terminated on orderly shutdown [SOLR-17744](https://issues.apache.org/jira/browse/SOLR-17744) (hossman)
- Provide long form --jettyconfig option to go with -j in bin/solr scripts. [SOLR-17746](https://issues.apache.org/jira/browse/SOLR-17746) (Eric Pugh) (Rahul Goswami)
- S3 File downloads now handle connection issues more gracefully [SOLR-17750](https://issues.apache.org/jira/browse/SOLR-17750) (Houston Putman) (Mark Miller)
- Parallelize index fingerprint computation across segments via a dedicated thread pool [SOLR-17756](https://issues.apache.org/jira/browse/SOLR-17756) (Matthew Biscocho) (Luke Kot-Zaniewski)
- Speed up function queries in 'fl' param. [SOLR-17775](https://issues.apache.org/jira/browse/SOLR-17775) (Yura Korolov)
- SolrJ: If Http2SolrClient.Builder.withHttpClient is used, (and it's used more in 9.8, shifted from deprecated Apache HttpClient based clients), settings affecting HttpClient construction cannot be customized; an IllegalStateException will now be thrown if you try. The connection timeout cannot be customized, but idle & request timeouts can be. Callers (various places in Solr) no longer customize the connection timeout. [SOLR-17776](https://issues.apache.org/jira/browse/SOLR-17776) (David Smiley) (Luke Kot-Zaniewski)
- The HttpJdkSolrClient wasn't setting the connection timeout as per the builder configuration. [SOLR-17776](https://issues.apache.org/jira/browse/SOLR-17776) (David Smiley)
- Use TotalHitCountCollector to collect count when no rows needed [SOLR-17801](https://issues.apache.org/jira/browse/SOLR-17801) (Kevin Risden)

### Fixed (26 changes)

- Improving check in TextToVectorUpdateProcessorFactory, which breaks update for new dynamic fields. [GITHUB#3426](https://github.com/apache/solr/pull/3426) (Renato Haeberli) (Alessandro Benedetti)
- Clean up shard metadata in ZooKeeper nodes after shard deletion is invoked. This makes sure Zookeeper nodes for leader election and terms are not left behind [SOLR-12831](https://issues.apache.org/jira/browse/SOLR-12831) (Andy Vuong) (Pierre Salagnac)
- SolrJ CloudSolrClient configured with Solr URLs can fail to request cluster state if its current live nodes list is stale. CloudSolrClient now retains the initial configured list of passed URLs as backup used for fetching cluster state when all live nodes have failed. [SOLR-17519](https://issues.apache.org/jira/browse/SOLR-17519) (Matthew Biscocho) (David Smiley) (Houston Putman)
- If SQLHandler failed to open the underlying stream (e.g. Solr returns an error; could be user/syntax problem), it needs to close the stream to cleanup resources but wasn't. [SOLR-17629](https://issues.apache.org/jira/browse/SOLR-17629) (David Smiley)
- Some CLI errors not logged when starting prometheus exporter [SOLR-17638](https://issues.apache.org/jira/browse/SOLR-17638) (Alex Deparvu)
- Core unload/deletion now preempts all forms of ongoing "recovery", rather than inadvertently waiting for completion in some cases. [SOLR-17692](https://issues.apache.org/jira/browse/SOLR-17692) (Jason Gerlowski)
- Fixed performance regression since 9.0 with "frange" queries embedded in other queries. May also affect some numeric range queries when the minimum is negative. [SOLR-17699](https://issues.apache.org/jira/browse/SOLR-17699) (David Smiley)
- Use Core Operation lock when unloading cores. This fixes errors that occur when a collection deletion and reload occur at the same time. [SOLR-17700](https://issues.apache.org/jira/browse/SOLR-17700) (Houston Putman)
- Fix race condition when checking distrib async cmd status [SOLR-17709](https://issues.apache.org/jira/browse/SOLR-17709) (Houston Putman)
- Remove total request timeout during recovery that was inadvertently added. [SOLR-17711](https://issues.apache.org/jira/browse/SOLR-17711) (Luke Kot-Zaniewski)
- Fix rare deadlock in CollectionProperties internals. [SOLR-17720](https://issues.apache.org/jira/browse/SOLR-17720) (Aparna Suresh) (Houston Putman)
- MoreLikeThis to support copy-fields [SOLR-17726](https://issues.apache.org/jira/browse/SOLR-17726) (Ilaria Petreti) (Alessandro Benedetti)
- When the V2 API is receiving raw files, it could sometimes skip the first byte. [SOLR-17740](https://issues.apache.org/jira/browse/SOLR-17740) (David Smiley)
- Cancel leader election was being skipped on core container shutdown due to incorrect zkClient check [SOLR-17745](https://issues.apache.org/jira/browse/SOLR-17745) (Matthew Biscocho) (Luke Kot-Zaniewski)
- Fix rare bug in overseer main loop in case of high load, that may cause the overseer be fully stuck until server restart. [SOLR-17754](https://issues.apache.org/jira/browse/SOLR-17754) (Pierre Salagnac)
- The NumFieldLimitingUpdateRequestProcessor's "warnOnly" mode has been fixed, and now processes documents even when the limit has been exceeded. [SOLR-17758](https://issues.apache.org/jira/browse/SOLR-17758) (Jason Gerlowski) (Rahul Goswami)
- Global circuit breakers can no longer trigger ConcurrentModificationException's on Solr startup [SOLR-17761](https://issues.apache.org/jira/browse/SOLR-17761) (Jason Gerlowski)
- Use S3 RetryStrategy instead of RetryPolicy. This fixes the error caused by using the "Adaptive" retry mode. [SOLR-17769](https://issues.apache.org/jira/browse/SOLR-17769) (Houston Putman)
- Fix a very minor memory leak in metrics reporting code when a core is deleted. [SOLR-17777](https://issues.apache.org/jira/browse/SOLR-17777) (Pierre Salagnac)
- Allow the -j or --jettyconfig option to start with a dash (-). [SOLR-17790](https://issues.apache.org/jira/browse/SOLR-17790) (Houston Putman)
- Fix deadlocks in ParallelHttpShardHandler, re-implement synchronization in HttpShardHandler [SOLR-17792](https://issues.apache.org/jira/browse/SOLR-17792) (Houston Putman)
- Security Manager should handle symlink on /tmp [SOLR-17800](https://issues.apache.org/jira/browse/SOLR-17800) (Kevin Risden)
- Exception in TransactionLog constructor deletes the file and does not block subsequent updates. [SOLR-17805](https://issues.apache.org/jira/browse/SOLR-17805) (Bruno Roustant)
- Disable http2 request cancellation for Jetty 10, the cancellation can bleed across to other requests. [SOLR-17819](https://issues.apache.org/jira/browse/SOLR-17819) (Houston Putman)
- Fix various methods of invoking "local" solr requests to re-use the same searcher as the original request. This notably fixes the use of spellcheck.maxCollationTries in firstSearcher warming queries to prevent deadlock [SOLR-5386](https://issues.apache.org/jira/browse/SOLR-5386) (hossman)
- Passing additional arguments to solr.cmd using "--jvm-opts" (formerly "-a") in conjunction with "-e" (examples like 'techproducts') wouldn't reflect on Windows [SOLR-7962](https://issues.apache.org/jira/browse/SOLR-7962) (Rahul Goswami) (Eric Pugh)

### Dependency Upgrades (6 changes)

- Update apache.zookeeper to v3.9.3 [PR#3061](https://github.com/apache/solr/pull/3061) (solrbot)
- chore(deps): update apache.kafka to v3.9.0 [PR#3078](https://github.com/apache/solr/pull/3078) (solrbot)
- Update amazon.awssdk to v2.31.77 [PR#3228](https://github.com/apache/solr/pull/3228) (solrbot)
- Update mockito to v5.16.1 [PR#3234](https://github.com/apache/solr/pull/3234) (solrbot)
- Upgrade Lucene to 9.12.2. [SOLR-17795](https://issues.apache.org/jira/browse/SOLR-17795) (Pierre Salagnac) (Christine Poerschke) (Houston Putman)
- Upgrade forbiddenapis to 3.9. (Uwe Schindler)

### Other (19 changes)

- Improve reliablity of NpmTasks finding needed files/commands. [GITHUB#2680](https://github.com/apache/solr/pull/2680) (Tyler Bertrand) (Eric Pugh)
- SolrTestCase now supports @LogLevel annotations (as SolrTestCaseJ4 has). Added LogLevelTestRule for encapsulation and reuse. [GITHUB#2869](https://github.com/apache/solr/pull/2869) (David Smiley)
- Most remaining usages of Apache HttpClient in Solr switched to Jetty HttpClient (HTTP 2). [SOLR-16503](https://issues.apache.org/jira/browse/SOLR-16503) (Sanjay Dutt) (David Smiley)
- Deprecate UpdateRequest.getXml(). Usage of XMLRequestWriter is now preferred. [SOLR-17518](https://issues.apache.org/jira/browse/SOLR-17518) (Pierre Salagnac)
- Remove unused code and other refactorings in ReplicationHandler and tests. Removed unused public LOCAL_ACTIVITY_DURING_REPLICATION variable. [SOLR-17579](https://issues.apache.org/jira/browse/SOLR-17579) (Eric Pugh)
- Introduce new test variant of waitForState(), that does not wait on live node changes when we're only interested in the collection state. [SOLR-17581](https://issues.apache.org/jira/browse/SOLR-17581) (Pierre Salagnac)
- Prevent error log entry on solr server due to initial HEAD request from HttpJdkSolrClient. [SOLR-17589](https://issues.apache.org/jira/browse/SOLR-17589) (Paul Blanchaert) (James Dyer)
- SolrJ's User-Agent header now uses the version of SolrJ. There's a corresponding HttpSolrCall.getUserAgentSolrVersion to parse it. [SOLR-17611](https://issues.apache.org/jira/browse/SOLR-17611) (David Smiley)
- SimpleOrderedMap (a NamedList) now implements java.util.Map. [SOLR-17623](https://issues.apache.org/jira/browse/SOLR-17623) (Renato Haeberli) (David Smiley)
- Replaced NamedList.findRecursive usages with _get, which can do Map traversal, and thus makes it easier to transition intermediate NamedLists to Maps. [SOLR-17625](https://issues.apache.org/jira/browse/SOLR-17625) (Gaurav Tuli)
- Added a node-wide CloudSolrClient to ZkController, and accessible via SolrCloudManager too. Uses Jetty HttpClient. Deprecated CloudSolrClient.getSolrClientCache. Redirected some callers. [SOLR-17630](https://issues.apache.org/jira/browse/SOLR-17630) (David Smiley)
- javabin format: provide option to decode maps as SimpleOrderedMap (a NamedList & Map) instead of LinkedHashMap. Intended to aid in 10.x compatibility. Enable via sys prop: solr.solrj.javabin.readMapAsNamedList=true [SOLR-17635](https://issues.apache.org/jira/browse/SOLR-17635) (Renato Haeberli) (David Smiley)
- multiThreaded=true: changed queue implementation from unlimited to 1000 max, after which the caller thread will execute. [SOLR-17648](https://issues.apache.org/jira/browse/SOLR-17648) (David Smiley)
- Add System.exit() in forbidden APIs, and make sure CLI unit tests never call it. [SOLR-17651](https://issues.apache.org/jira/browse/SOLR-17651) (Pierre Salagnac)
- Simplify zombie server logic in LBSolrClient [SOLR-17667](https://issues.apache.org/jira/browse/SOLR-17667) (Houston Putman)
- Replication and backup have their DirectoryFactory.DirContext so the directory they use is unwrapped when copying files. [SOLR-17671](https://issues.apache.org/jira/browse/SOLR-17671) (Bruno Roustant) (David Smiley)
- Handle interrupted exception in SolrCores.waitAddPendingCoreOps. [SOLR-17716](https://issues.apache.org/jira/browse/SOLR-17716) (Bruno Roustant)
- Deprecated 'addHttpRequestToContext', removed in 10. [SOLR-17741](https://issues.apache.org/jira/browse/SOLR-17741) (David Smiley)
- Fix native access warning when using MemorySegmentIndexInput [SOLR-17803](https://issues.apache.org/jira/browse/SOLR-17803) (Kevin Risden)


