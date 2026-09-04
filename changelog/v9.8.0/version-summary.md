<!-- @formatter:off -->
<!-- noinspection -->
<!-- Prevents auto format, for JetBrains IDE File > Settings > Editor > Code Style (Formatter Tab) > Turn formatter on/off with markers in code comments  -->

<!-- This file is automatically generate by logchange tool 🌳 🪓 => 🪵 -->
<!-- Visit https://github.com/logchange/logchange and leave a star 🌟 -->
<!-- !!! ⚠️ DO NOT MODIFY THIS FILE, YOUR CHANGES WILL BE LOST ⚠️ !!! -->


[9.8.0] - 2025-01-23
--------------------

### Added (3 changes)

- The Solr Cross-DC Project has graduated from the sandbox repository. It will now be released as a fully supported Solr feature. This feature closes SIP-13: Cross Data Center Replication. [SOLR-17065](https://issues.apache.org/jira/browse/SOLR-17065) (Mark Miller) (Anshum Gupta) (Andrzej Bialecki) (Jason Gerlowski) (Houston Putman)
- Implement `memAllowed` parameter to limit per-thread memory allocations during request processing. [SOLR-17150](https://issues.apache.org/jira/browse/SOLR-17150) (Andrzej Bialecki) (Gus Heck)
- Added knn_text_to_vector query parser to encode text to vector at query time through external LLM services. [SOLR-17525](https://issues.apache.org/jira/browse/SOLR-17525) (Alessandro Benedetti)

### Changed (21 changes)

- Solrj CloudSolrClient with Solr URLs had serious performance regressions (since the beginning?) in which its collection state cache was not being used, resulting in many extra requests to Solr for cluster information. [SOLR-14985](https://issues.apache.org/jira/browse/SOLR-14985) (Aparna Suresh) (shalin) (David Smiley)
- v2 "cluster prop" APIs have been updated to be more REST-ful. Cluster prop creation/update are now available at `PUT /api/cluster/properties/somePropName`. Deletion is now available at `DELETE /api/cluster/properties/somePropName`. New APIs for listing-all and fetching-single cluster props are also now available at `GET /api/cluster/properties` and `GET /api/cluster/properties/somePropName`, respectively. [SOLR-16390](https://issues.apache.org/jira/browse/SOLR-16390) (Carlos Ugarte) (Jason Gerlowski)
- Replication "fetch file" API now has a v2 equivalent, available at `GET /api/cores/coreName/replication/files/fileName` [SOLR-16470](https://issues.apache.org/jira/browse/SOLR-16470) (Matthew Biscocho) (Jason Gerlowski)
- The VersionBucket indexing lock mechanism was replaced with something just as fast yet that which consumes almost no memory, saving 1MB of memory per SolrCore. [SOLR-17102](https://issues.apache.org/jira/browse/SOLR-17102) (David Smiley)
- Users using query limits (timeAllowed, cpuTimeAllowed) for whom partial results are uninteresting may set partialResults=false. This parameter has been enhanced to reduce time spent processing partial results and omit partialResults from the response. Since this is requested behavior, no exception is thrown and the partialResults response header will always exist if the result was short circuited. [SOLR-17158](https://issues.apache.org/jira/browse/SOLR-17158) (Gus Heck) (Andrzej Bialecki) (hossman)
- Deprecate snapshotscli.sh in favour of bin/solr snapshot sub commands. Now able to manage Snapshots from the CLI. HDFS module specific snapshot script now ships as part of that module in the modules/hdfs/bin directory. [SOLR-17180](https://issues.apache.org/jira/browse/SOLR-17180) (Eric Pugh)
- Deprecate SolrRequest `setBasePath` and `getBasePath` methods. SolrJ users wishing to temporarily override an HTTP client's base URL may use `Http2SolrClient.requestWithBaseUrl` instead. [SOLR-17256](https://issues.apache.org/jira/browse/SOLR-17256) (Jason Gerlowski) (Sanjay Dutt) (David Smiley)
- Make CLUSTERSTATUS request configurable to improve performance by allowing retrieval of specific information, reducing unnecessary data fetching. Enhanced CloudSolrClient's HTTP ClusterStateProvider to use it, and to scale to more collections better as well. [SOLR-17381](https://issues.apache.org/jira/browse/SOLR-17381) (Aparna Suresh) (David Smiley)
- Resolved overlapping arguments in the Solr CLI. Removed duplicative but differing arguments, consolidated use of short form arguments -v to not have differing meanings based on tool. Provide deprecation warning in command line when deprecated arguments are used. [SOLR-17383](https://issues.apache.org/jira/browse/SOLR-17383) (Eric Pugh) (Christos Malliaridis)
- EmbeddedSolrServer now considers the ResponseParser [SOLR-17390](https://issues.apache.org/jira/browse/SOLR-17390) (David Smiley)
- Reduce thread contention in ZkStateReader.getCollectionProperties(). [SOLR-17396](https://issues.apache.org/jira/browse/SOLR-17396) (Aparna Suresh) (David Smiley) (Paul McArthur)
- SkipExistingDocumentsProcessor now functions correctly with child documents. [SOLR-17397](https://issues.apache.org/jira/browse/SOLR-17397) (Tim Owens) (Eric Pugh)
- COLSTATUS command was fetching details remotely from replicas when this information wasn't asked for. [SOLR-17408](https://issues.apache.org/jira/browse/SOLR-17408) (Mathieu Marie)
- When searching with multiThreaded=true, the internal tasks may now block instead of enqueuing with a risk of rejection. Solr will use less resources under stress but to get the most of your machine, you may want to increase the thread pool. [SOLR-17414](https://issues.apache.org/jira/browse/SOLR-17414) (David Smiley)
- An alternate ShardHandlerFactory is now available, ParallelHttpShardHandlerFactory, which may help reduce distributed-search latency in collections with many shards, especially when PKI is used between nodes. [SOLR-17419](https://issues.apache.org/jira/browse/SOLR-17419) (Jason Gerlowski)
- Improve system metrics collection by skipping unreadable MXBean properties, making /admin/info/system calls faster [SOLR-17441](https://issues.apache.org/jira/browse/SOLR-17441) (Haythem Khiri)
- Leverage waitForState() instead of busy waiting in CREATE, MIGRATE, REINDEXCOLLECTION, MOVEREPLICA commands, and in some tests. [SOLR-17453](https://issues.apache.org/jira/browse/SOLR-17453) (Pierre Salagnac)
- Introduce -y short option to bin/solr start --no-prompt option. Aligns with bin/solr package tool. [SOLR-17528](https://issues.apache.org/jira/browse/SOLR-17528) (Eric Pugh)
- Suppress printing out of password to console when using auth CLI command. [SOLR-17554](https://issues.apache.org/jira/browse/SOLR-17554) (Christos Malliaridis) (Eric Pugh)
- Switched from HTTP1 to HTTP2 in SolrCloudManager via HttpClient change from Apache to Jetty. [SOLR-17592](https://issues.apache.org/jira/browse/SOLR-17592) (Sanjay Dutt) (David Smiley)
- Optimize PostTool to call just optimize when both commit and optimize requested. [SOLR-3913](https://issues.apache.org/jira/browse/SOLR-3913) (Eric Pugh)

### Fixed (16 changes)

- Splitting shards now routes child-docs with their _root_ field when available so they maintain parent relationship. [SOLR-11191](https://issues.apache.org/jira/browse/SOLR-11191) (Zack Kendall)
- Uploading a configset with a symbolic link produces a IOException. Now a error message to user generated instead. [SOLR-12429](https://issues.apache.org/jira/browse/SOLR-12429) (Eric Pugh)
- Clarify when a bin/solr create needs to be run on the same server as Solr. [SOLR-16254](https://issues.apache.org/jira/browse/SOLR-16254) (Eric Pugh)
- Remove log4j-jul jar and use slf4j bridge for JUL to prevent exception from being logged when remote JMX is enabled [SOLR-16976](https://issues.apache.org/jira/browse/SOLR-16976) (Shawn Heisey) (Stephen Zhou) (Eric Pugh) (Christine Poerschke) (David Smiley)
- fix replication problem on follower restart [SOLR-17306](https://issues.apache.org/jira/browse/SOLR-17306) (Martin Anzinger and Peter Kroiss) (Eric Pugh)
- Fix race condition where Zookeeper session could be re-established by multiple threads concurrently in case of frequent session expirations. [SOLR-17405](https://issues.apache.org/jira/browse/SOLR-17405) (Pierre Salagnac)
- Fixed UpdateLog replay bug that shared thread-unsafe SolrQueryRequest objects across threads [SOLR-17413](https://issues.apache.org/jira/browse/SOLR-17413) (Jason Gerlowski) (David Smiley) (Houston Putman)
- Fixed ExportHandler bug that silently suppressed errors and returned partial results in some situations [SOLR-17416](https://issues.apache.org/jira/browse/SOLR-17416) (hossman)
- Fixed a rare case where overseer was stuck after a failure when changing overseer to honor the node role for preferred overseer. [SOLR-17421](https://issues.apache.org/jira/browse/SOLR-17421) (Pierre Salagnac)
- Fixed Http2SolrClient bug in that 'requestAsync' triggered NPE when using a shared Jetty client [SOLR-17464](https://issues.apache.org/jira/browse/SOLR-17464) (Jason Gerlowski) (James Dyer)
- Clean up error message in PostTool when you attempt to post to a basic auth secured Solr. [SOLR-17529](https://issues.apache.org/jira/browse/SOLR-17529) (Eric Pugh)
- Fix AllowListUrlChecker when liveNodes changes. Remove ClusterState.getHostAllowList [SOLR-17574](https://issues.apache.org/jira/browse/SOLR-17574) (Bruno Roustant) (David Smiley)
- Fixed broken backwards compatibility with the legacy "langid.whitelist" config in Solr Langid. [SOLR-17575](https://issues.apache.org/jira/browse/SOLR-17575) (Jan Høydahl) (Alexander Zagniotov)
- Print zkcli.sh deprecation msg to stderr, which fixes Solr Operator upload of security.json to zookeeper [SOLR-17586](https://issues.apache.org/jira/browse/SOLR-17586) (Jan Høydahl)
- Fix two issues in Solr CLI that prevent Solr from starting with the techproducts example and from correctly parsing arguments on Windows that start with -D and have multiple values separated by "," or spaces. [SOLR-17595](https://issues.apache.org/jira/browse/SOLR-17595) (Christos Malliaridis)
- bin/solr stop/start/restart should complain about missing value for options that expect a value. [SOLR-6962](https://issues.apache.org/jira/browse/SOLR-6962) (Eric Pugh) (Rahul Goswami)

### Dependency Upgrades (6 changes)

- chore(deps): update dependency com.github.spotbugs:spotbugs-annotations to v4.8.6 [PR#2129](https://github.com/apache/solr/pull/2129) (solrbot)
- Update org.glassfish.hk2*:* to v3.1.1 [PR#2268](https://github.com/apache/solr/pull/2268) (solrbot)
- Update org.glassfish.jersey*:* to v3.1.9 [PR#2396](https://github.com/apache/solr/pull/2396) (solrbot)
- Update dependency commons-cli:commons-cli to v1.9.0 [PR#2650](https://github.com/apache/solr/pull/2650) (solrbot)
- chore(deps): update io.netty:* to v4.1.114.final [PR#2702](https://github.com/apache/solr/pull/2702) (solrbot)
- Update dependency com.fasterxml.jackson:jackson-bom to v2.18.0 [PR#2769](https://github.com/apache/solr/pull/2769) (solrbot)

### Other (15 changes)

- Introduce unit testing for AssertTool. [SOLR-11318](https://issues.apache.org/jira/browse/SOLR-11318) (Eric Pugh) (Jason Gerlowski)
- NamedList: deprecating methods: forEachEntry, forEachKey, abortableForEachKey, abortableForEach, asMap (no-arg only), get(key, default). Added getOrDefault. Deprecated the SimpleMap interface as well as the entirety of the SolrJ package org.apache.solr.cluster.api, which wasn't used except for SimpleMap. [SOLR-14680](https://issues.apache.org/jira/browse/SOLR-14680) (David Smiley)
- "&lt;lib/&gt;" tags in solrconfig.xml are now quietly ignored by default unless explicitly enabled with the `SOLR_CONFIG_LIB_ENABLED=true` enviroment variable (or corresponding sysprop). These tags are now considered deprecated and will be removed in Solr 10. [SOLR-16781](https://issues.apache.org/jira/browse/SOLR-16781) 
- Fix Gradle build sometimes gives spurious "unreferenced license file" warnings. [SOLR-17142](https://issues.apache.org/jira/browse/SOLR-17142) (Uwe Schindler)
- Remove Deprecated URL and replace it with URI in Preparation for Java 21 [SOLR-17321](https://issues.apache.org/jira/browse/SOLR-17321) (Sanjay Dutt) (David Smiley) (Uwe Schindler)
- Move Zk Arg parsing into Java Code from bin/solr scripts. [SOLR-17359](https://issues.apache.org/jira/browse/SOLR-17359) (Eric Pugh) (Rahul Goswami)
- Replace the use of the deprecated java.util.Locale constructor with Locale Builder API. [SOLR-17399](https://issues.apache.org/jira/browse/SOLR-17399) (Sanjay Dutt)
- Fixed inadvertent suppression of exceptions in the background tasks across the codebase. For certain tasks that were scheduled via ExecutorService#submit, the results of the task execution were never examined which led to the suppression of exceptions. [SOLR-17448](https://issues.apache.org/jira/browse/SOLR-17448) (Andrey Bozhko)
- Document deprecation status of language specific writer types (wt=python,ruby,php,phps). [SOLR-17494](https://issues.apache.org/jira/browse/SOLR-17494) (Eric Pugh)
- CoreContainer calls UpdateHandler.commit when closing a read-only core [SOLR-17504](https://issues.apache.org/jira/browse/SOLR-17504) (Bruno Roustant)
- Introduce ClusterState.getCollectionNames, a convenience method [SOLR-17534](https://issues.apache.org/jira/browse/SOLR-17534) (David Smiley)
- Introduce ClusterState.collectionStream to replace getCollectionStates, getCollectionsMap, and forEachCollection, which are now deprecated. [SOLR-17535](https://issues.apache.org/jira/browse/SOLR-17535) (David Smiley)
- Upgrade to Gradle 8.10 [SOLR-17545](https://issues.apache.org/jira/browse/SOLR-17545) (Houston Putman)
- "home" and "data" directories used by Solr examples have been updated to align with documented best practices. [SOLR-17556](https://issues.apache.org/jira/browse/SOLR-17556) (Eric Pugh) (Houston Putman)
- Remove "solr.indexfetcher.sotimeout" system property that was for optimizing replication tests. It was disabled, but not removed. [SOLR-17577](https://issues.apache.org/jira/browse/SOLR-17577) (Eric Pugh)


