<!-- @formatter:off -->
<!-- noinspection -->
<!-- Prevents auto format, for JetBrains IDE File > Settings > Editor > Code Style (Formatter Tab) > Turn formatter on/off with markers in code comments  -->

<!-- This file is automatically generate by logchange tool 🌳 🪓 => 🪵 -->
<!-- Visit https://github.com/logchange/logchange and leave a star 🌟 -->
<!-- !!! ⚠️ DO NOT MODIFY THIS FILE, YOUR CHANGES WILL BE LOST ⚠️ !!! -->


[9.11.0]
--------

### Important notes

- Per-core `/solr/{core}/replication` commands are authorized individually. `backup`, `restore`, `deletebackup`, `disablereplication`, `enablereplication`, `abortfetch`, `disablepoll`, `enablepoll`, and `fetchindex` require the `update` permission; `indexversion`, `filelist`, `filecontent`, `details`, `commits`, and `restorestatus` require `read`. A command not listed here requires `update`. Grant `update` (or `all`) to roles that invoke the state-changing commands.
- A `filecontent` request to `/solr/{core}/replication` with `dirType=cf` or `dirType=tlogFile` must name a file that resolves inside the core's config or tlog directory; otherwise it is rejected.
- In SolrCloud, a request served by a core on the receiving node is authorized against that core's collection, regardless of the `collection` request parameter. Requests the core sends on to other collections, such as a distributed search over several collections or an alias, are authorized by the nodes that receive them, against their own collections. For example, a search sent to a core of collection `A` with `collection=B` needs read permission on both `A` and `B`.

### Added (29 changes)

- New SolrJ CollectionScopedSolrClient [PR#4418](https://github.com/apache/solr/pull/4418) (David Smiley)
- Create new v2 APIs for listing and reading collection properties ("collprops") [SOLR-12224](https://issues.apache.org/jira/browse/SOLR-12224) (Jason Gerlowski)
- Introduce new `DoubleRangeField` field type for storing and querying double-based ranges [SOLR-13309](https://issues.apache.org/jira/browse/SOLR-13309) (Jason Gerlowski)
- Introduce new `FloatRangeField` field type for storing and querying float-based ranges [SOLR-13309](https://issues.apache.org/jira/browse/SOLR-13309) (Jason Gerlowski)
- Introduce new `IntRangeField` field type and (experimental) `{!numericRange}` query parser for storing and querying integer ranges. [SOLR-13309](https://issues.apache.org/jira/browse/SOLR-13309) (Jason Gerlowski)
- Introduce new `LongRangeField` field type and (experimental) `{!numericRange}` query parser for storing and querying long ranges [SOLR-13309](https://issues.apache.org/jira/browse/SOLR-13309) (Jason Gerlowski)
- The {!parent} and {!child} query parsers now support a parentPath local param that automatically derives the correct parent filter using the _nest_path_ field, making nested document queries easier to write correctly. childPath is also added. [SOLR-14687](https://issues.apache.org/jira/browse/SOLR-14687) (David Smiley) (hossman)
- Support block / nested-docs with index sorting [SOLR-17170](https://issues.apache.org/jira/browse/SOLR-17170) (David Smiley)
- New CombinedQuerySearchHandler etc. for implementing hybrid search with reciprocal rank fusion (RRF). [SOLR-17319](https://issues.apache.org/jira/browse/SOLR-17319) [SOLR-18290](https://issues.apache.org/jira/browse/SOLR-18290) (Sonu Sharma) (David Smiley)
- New LatestVersionMergePolicyFactory to upgrade index for compatibility with future Solr version [SOLR-17725](https://issues.apache.org/jira/browse/SOLR-17725) (Rahul Goswami)
- Add alwaysStopwords option to edismax so its "all stopwords" behaviour can be controlled [SOLR-17959](https://issues.apache.org/jira/browse/SOLR-17959) (Andy Webb)
- Enable MergeOnFlushMergePolicy in Solr [SOLR-17984](https://issues.apache.org/jira/browse/SOLR-17984) ([Houston Putman](https://home.apache.org/phonebook.html?uid=houston) @HoustonPutman)
- Support including stored fields in Export Writer output. [SOLR-18071](https://issues.apache.org/jira/browse/SOLR-18071) (Luke Kot-Zaniewski)
- Introducing support for multi valued dense vector representation in documents through nested vectors [SOLR-18074](https://issues.apache.org/jira/browse/SOLR-18074) (Alessandro Benedetti)
- Add top-level "queries" support to JsonQueryRequest in SolrJ [SOLR-18093](https://issues.apache.org/jira/browse/SOLR-18093) (Sonu Sharma @ercsonusharma)
- CoreAdmin API (/admin/cores?action=UPGRADECOREINDEX) to upgrade an index in-place [SOLR-18096](https://issues.apache.org/jira/browse/SOLR-18096) (Rahul Goswami)
- New ContentHashVersionProcessor to avoid index churn when adding same-content documents. [SOLR-18189](https://issues.apache.org/jira/browse/SOLR-18189) (Francois Huaulme) (David Smiley)
- Support for using {!collapse} with CombinedQueryComponent (RRF) [SOLR-18195](https://issues.apache.org/jira/browse/SOLR-18195) (Sonu Sharma @ercsonusharma)
- Support 'missing' stats count in rollup function for streaming expressions [SOLR-18198](https://issues.apache.org/jira/browse/SOLR-18198) (khushjain)
- Range field types (Int/Long/Float/DoubleRangeField) now support docValues="true" as a query-time filter optimization. [SOLR-18201](https://issues.apache.org/jira/browse/SOLR-18201) (Sonu Sharma @ercsonusharma)
- Support 'countDist' (count distinct) metric in rollup for streaming expressions [SOLR-18220](https://issues.apache.org/jira/browse/SOLR-18220) (khushjain)
- Support 'percentile' (per) metric in rollup for streaming expressions [SOLR-18221](https://issues.apache.org/jira/browse/SOLR-18221) (khushjain)
- Add `name` local parameter support and `MatchedQueriesComponent` to identify which named sub-queries matched each document. [SOLR-18227](https://issues.apache.org/jira/browse/SOLR-18227) (Dmitrii Tikhonov)
- Support for using Query Elevation with CombinedQueryComponent (RRF) [SOLR-18271](https://issues.apache.org/jira/browse/SOLR-18271) (Sonu Sharma @ercsonusharma)
- Support 'std' (standard deviation) metric in rollup for streaming expressions [SOLR-18328](https://issues.apache.org/jira/browse/SOLR-18328) (khushjain)
- DenseVectorField now supports existence queries (field:* / [* TO *]) [SOLR-18329](https://issues.apache.org/jira/browse/SOLR-18329) (Li Enlai @linslee75)
- SPLITSHARD has expanded support for collection router.field, not only supporting indexed fields but also docValues. Note: "Point" numeric fields only work in this case with docValues. [SOLR-18335](https://issues.apache.org/jira/browse/SOLR-18335) (Olivier Boudet) (David Smiley)
- Luke handler now aggregates results across multiple shards and does this by default in SolrCloud mode. [SOLR-8127](https://issues.apache.org/jira/browse/SOLR-8127) (Luke Kot-Zaniewski)
- Add root document query shortcut support to NestPathField [SOLR-18197](https://issues.apache.org/jira/browse/SOLR-18197) (Abhishek Umarjikar @abumarjikar) (David Smiley @dsmiley)

### Changed (24 changes)

- Dropdowns for collection/core, for fields on the schema-page and for field-types on the analyze page are now using a contains-filtering [GITHUB#4121](https://github.com/apache/solr/pull/4121) (Renato Haeberli)
- add percentage and threshold based minimum match functionality, as we know it from ExtendedDismaxQParser, to BoolQParserPlugin [PR#4406](https://github.com/apache/solr/pull/4406) (Renato Haeberli)
- The /sql handler request params forwarded to Calcite is now configurable [PR#4607](https://github.com/apache/solr/pull/4607) (Jan Høydahl) ([William Wallace](https://fjord.ai/) @phyr3wall)
- Distributed tracing and audit logging now see request parameters sent in a url-encoded POST body, not only those in the URL query string. [PR#4870](https://github.com/apache/solr/pull/4870) (David Smiley) (Xinyao Zhang)
- HttpSolrClient impls now sends certain interesting request parameters in the URL query string when a POST of parameters is submitted. In other words, withTheseParamNamesInTheUrl now has a default set. This improves observability, particularly for distributed search & admin commands. [PR#4871](https://github.com/apache/solr/pull/4871) (David Smiley)
- Parallelize Backup and Restore File Operations [SOLR-1092](https://issues.apache.org/jira/browse/SOLR-1092) (Samuel Verstraete @elangelo) (David Smiley @dsmiley)
- QueryRequest.java in SolrJ no longer sends the 'qt' parameter to the server. [SOLR-17715](https://issues.apache.org/jira/browse/SOLR-17715) [PR#4397](https://github.com/apache/solr/pull/4397) (r4mercur @r4mercur)
- HttpJdkSolrClient & HttpJettySolrClient now send headers that enable Solr's rate limiting to work. [SOLR-17810](https://issues.apache.org/jira/browse/SOLR-17810) (David Smiley) (Gaurav Tuli)
- Improved multiThreaded=true performance when a docset is needed (e.g. faceting). [SOLR-17841](https://issues.apache.org/jira/browse/SOLR-17841) (Puneet Ahuja @punAhuja)
- Add solr.cloud.delete.unknown.cores.enabled setting for removing unknown but existing core data when a core is created in SolrCloud mode. [SOLR-18008](https://issues.apache.org/jira/browse/SOLR-18008) (Eric Pugh) (David Smiley)
- PropertiesInputStream overrides bulk read method, and rename it to IndexInputInputStream to match symmetrical class IndexOutputOutputStream. [SOLR-18029](https://issues.apache.org/jira/browse/SOLR-18029) (Pierre Salagnac)
- Improved CloudSolrClient's urlScheme detection by using the scheme of provided Solr URLs, or looking at "solr.ssl.enabled". [SOLR-18056](https://issues.apache.org/jira/browse/SOLR-18056) (Vishnu Priya Chandra Sekar)
- Optimize the size of the internal buffer of JavaBin codec to reduce the number of allocations. This reduces GC pressure on SolrJ client under high indexing load. [SOLR-18157](https://issues.apache.org/jira/browse/SOLR-18157) (Pierre Salagnac)
- Increased query throughput by removing a call to ZooKeeper for cluster state that should have been cached. Happens when Solr does distributed search over multiple collections, and when the coordinator has no local replica for some of them. [SOLR-18176](https://issues.apache.org/jira/browse/SOLR-18176) [SOLR-15352](https://issues.apache.org/jira/browse/SOLR-15352) (Matthew Biscocho)
- JWT Authentication `blockUnknown` now defaults to `true`, blocking unauthenticated requests by default. Previously the code defaulted to `false` despite the reference guide documenting `true`. Users relying on pass-through must explicitly set `blockUnknown` to `false` in their security.json. [SOLR-18215](https://issues.apache.org/jira/browse/SOLR-18215) ([Jan Høydahl](https://home.apache.org/phonebook.html?uid=janhoy))
- New solr.xml setting allowZkHosts limits which ZooKeeper connection strings a cross-collection join or streaming expression may use (zkHost). The local SolrCloud ensemble is always allowed. zkHost and solrUrl are now mutually exclusive in a single cross-collection join clause. The local ensemble is matched verbatim, including any chroot: a node started with `-DzkHost=zk1:2181/solr` allows `zkHost="zk1:2181/solr"` but not `zkHost="zk1:2181"` unless listed in `allowZkHosts`. [SOLR-18224](https://issues.apache.org/jira/browse/SOLR-18224) [SOLR-18229](https://issues.apache.org/jira/browse/SOLR-18229) (Mark Robert Miller)
- The per-core /solr/{core}/replication endpoint is now authorized per command: state-changing commands require the "update" permission, while read-only commands continue to require "read". [SOLR-18225](https://issues.apache.org/jira/browse/SOLR-18225) (Mark Robert Miller)
- Replication file fetching treats both "/" and "\" as path separators when validating a file name, and config/tlog file names must resolve within the core's config or tlog directory. [SOLR-18226](https://issues.apache.org/jira/browse/SOLR-18226) (Mark Robert Miller)
- /update/extract (ExtractingRequestHandler) now requires the "update" permission rather than "read", since it indexes documents. [SOLR-18228](https://issues.apache.org/jira/browse/SOLR-18228) (Mark Robert Miller)
- In SolrCloud, requests are authorized against the collection of the core that serves them; the "collection" request parameter is not used for authorization. [SOLR-18230](https://issues.apache.org/jira/browse/SOLR-18230) (David Smiley) (Mark Robert Miller)
- Change JettySolrRunner's use of GracefulHandler to be opt-in [SOLR-18285](https://issues.apache.org/jira/browse/SOLR-18285) (hossman)
- Optimize collapse performance for String fields in Solr 9.x and later [SOLR-18304](https://issues.apache.org/jira/browse/SOLR-18304) (Bartosz Fidrysiak)
- Leader/follower replication now restarts with the latest index generation when the selected generation expires during download, avoiding retries against an unavailable generation. [SOLR-18406](https://issues.apache.org/jira/browse/SOLR-18406) (ZhenyuLi @JHSUYU)
- Simplify PingRequestHandler shard handling [SOLR-18419](https://issues.apache.org/jira/browse/SOLR-18419) (Jan Høydahl)

### Fixed (38 changes)

- HttpJettySolrClient could throw IllegalStateException on connection lost, which foiled LBSolrClient's attempts to classify a request as retry-able. [PR#4490](https://github.com/apache/solr/pull/4490) (David Smiley)
- PKIAuthenticationPlugin now rejects a SolrAuthV2 header with a malformed signature using a 401 response, instead of returning a 500 [PR#4553](https://github.com/apache/solr/pull/4553) (Jan Høydahl)
- Fixed HttpJdkSolrClient leaking an executor thread when an async request's connection failed while its body was still being written; enough such failures could exhaust the client's thread pool. [SOLR-17707](https://issues.apache.org/jira/browse/SOLR-17707) (Serhiy Bzhezytskyy)
- SpellCheckCollator now returns the partial (mutable) list of collations it had gathered so far, instead of throwing an UnsupportedOperationException, when the query time limit is exceeded while collating results. [SOLR-17870](https://issues.apache.org/jira/browse/SOLR-17870) (Puneet Sharma)
- SOLR-17973: Fix `shards.preference` not respected for cross-collection join queries [SOLR-17973](https://issues.apache.org/jira/browse/SOLR-17973) (khushjain)
- Improve HttpJettySolrClient.requestAsync (used in sharded/distributed-search and more) to increase throughput and prevent a rare deadlock. [SOLR-18051](https://issues.apache.org/jira/browse/SOLR-18051) (James Vanneman)
- JWT Authentication plugin now supports matching non-string claims such as boolean [SOLR-18073](https://issues.apache.org/jira/browse/SOLR-18073) ([Jan Høydahl](https://home.apache.org/phonebook.html?uid=janhoy)) (Tony Panza)
- Fix replication failure for files with exact MB sizes [SOLR-18098](https://issues.apache.org/jira/browse/SOLR-18098) (Shubham Ranjan)
- The /admin/info/logging endpoint (or just a tests) could yield partial logging hierarchies after log4j was upgraded. It should now be robust. [SOLR-18107](https://issues.apache.org/jira/browse/SOLR-18107) (David Smiley)
- Fixed CloudSolrClient deleteById failure when routing info is not passed with compositeId router, router.field, and directUpdatesToLeadersOnly enabled [SOLR-18114](https://issues.apache.org/jira/browse/SOLR-18114) (Matthew Biscocho)
- Fix ArrayStoreException when combining rerank with sort under multi-threaded segment-parallel search [SOLR-18136](https://issues.apache.org/jira/browse/SOLR-18136) (Shiming Li)
- CloudSolrClient- fixed state refresh race; didn't refresh. Regression from 9.10.1/10.0. [SOLR-18142](https://issues.apache.org/jira/browse/SOLR-18142) (David Smiley)
- Fixed schema designer to create a missing .system collection. This is a regression specific to 9.x. [SOLR-18144](https://issues.apache.org/jira/browse/SOLR-18144) (David Smiley) (Eric Pugh) (Jan Høydahl)
- Fix race conditions in "global" CircuitBreaker registration [SOLR-18146](https://issues.apache.org/jira/browse/SOLR-18146) (Jason Gerlowski)
- Abort shard leader election if container shutdown sequence has started, so we don't have leaders elected very late and not properly closed. [SOLR-18155](https://issues.apache.org/jira/browse/SOLR-18155) (Pierre Salagnac)
- Fix semaphore permit leaks in Http2SolrClient's AsyncTracker. Avoid IO-thread deadlock on connection failure retries. Add a new metric gauge solr_client_request_async_permits [SOLR-18174](https://issues.apache.org/jira/browse/SOLR-18174) ([Jan Høydahl](https://home.apache.org/phonebook.html?uid=janhoy))
- Use of function queries in "fl" would fail if rows exceeds 1000 and scores requested. A regression since v9.9. [SOLR-18181](https://issues.apache.org/jira/browse/SOLR-18181) (Matthew Biscocho)
- Fixed Admin UI to use max heap (-Xmx) value instead of committed heap to compute heap used percentage. [SOLR-18186](https://issues.apache.org/jira/browse/SOLR-18186) (Ravi Ranjan Jha)
- Strengthen Basic Authentication password policy (password must differ from username) and harden template users created by bin/solr auth enable. The check can be temporarily disabled with -Dsolr.security.auth.basicauth.allowuseraspassword=true (env SOLR_SECURITY_AUTH_BASICAUTH_ALLOWUSERASPASSWORD) as an upgrade escape hatch. [SOLR-18233](https://issues.apache.org/jira/browse/SOLR-18233) (Jan Høydahl)
- Avoid OOMs when deserializing collection states by not copying full data for UTF8 to Java string conversion. [SOLR-18237](https://issues.apache.org/jira/browse/SOLR-18237) (Pierre Salagnac)
- Fixed NPE in size estimator for null valued fields [SOLR-18239](https://issues.apache.org/jira/browse/SOLR-18239) (Jalaz Kumar)
- Fix several concurrency bugs in HttpShardHandler / ParallelHttpShardHandler that could cause search threads to hang in take() or return HTTP 500 instead of honoring shards.tolerant under thread-pool saturation [SOLR-18244](https://issues.apache.org/jira/browse/SOLR-18244) (Mark Miller)
- LoadAverageCircuitBreaker now caches its sampled value for a short TTL, so it stops re-polling the OS load average per request at high RPS. [SOLR-18284](https://issues.apache.org/jira/browse/SOLR-18284) (Mark Robert Miller)
- MemoryCircuitBreaker now measures post-GC live heap data, so it no longer trips when the heap is full of collectible garbage. Earlier versions sampled MemoryMXBean.getHeapMemoryUsage().getUsed() on a 30-second moving average (6 samples), which trends toward max between collections during normal operation; if you tuned a threshold against that behavior you may need to revisit it. This breaks subclasses of MemoryCircuitBreaker: the deprecated MemoryCircuitBreaker(int, int) constructor is removed, and the protected getAvgMemoryUsage() hook is renamed to getCurrentMemoryUsage() (it no longer averages anything). The now-unused public AveragingMetricProvider class is also removed. [SOLR-18284](https://issues.apache.org/jira/browse/SOLR-18284) (Mark Robert Miller)
- Fix Thread Pool Starvation in HttpJdkSolrClient. HttpJdkSolrClient defaults would often create 32 threads, likely under-utilizing them. and it would cap threads to 256. Now it does neither by default but Executor customization (and other saturation controls) remain. [SOLR-18312](https://issues.apache.org/jira/browse/SOLR-18312) (Renato Haeberli) (David Smiley)
- PRS based collection creation returns too early; can cause restore failure. [SOLR-18334](https://issues.apache.org/jira/browse/SOLR-18334) (David Smiley)
- /replication?command=details now reports backup details while the backup is still running, including file counts, instead of the previous backup's status [SOLR-18344](https://issues.apache.org/jira/browse/SOLR-18344) (Idan Tepper @idantepper)
- SolrJ ClientUtils.encodeLocalParamVal() can produce lossy/invalid encodings with a backslash or leading quotes. Affects faceting with a custom facet response key. Affects the SQL module for LIKE queries. [SOLR-18345](https://issues.apache.org/jira/browse/SOLR-18345) (David Smiley)
- Fix complement() and intersect() streaming expressions silently returning wrong results when on= maps two differently-named fields [SOLR-18418](https://issues.apache.org/jira/browse/SOLR-18418) ([David Smiley](https://home.apache.org/phonebook.html?uid=dsmiley))
- SlowCompositeReaderWrapper no longer misreports the index sort and hasBlocks of a multi-segment composite view; both were previously taken from the first segment only, which is wrong whenever there is more than one segment. #4825 (David Smiley)
- AllowListUrlChecker now detects URL schemes with URLUtil#hasScheme, the same helper used when building shard URLs, parsing scheme prefixes consistently and rejecting invalid schemes. [SOLR-18293](https://issues.apache.org/jira/browse/SOLR-18293) (Rajat Raghav @Xclow3n) (Jan Høydahl)
- Balance replicas API now enforces the rule of at most one replica per shard on the same node [SOLR-18327](https://issues.apache.org/jira/browse/SOLR-18327) (Jan Høydahl)
- A transient connection failure from a shard leader to one of its replicas is now retried when the failure arrives wrapped inside another exception, instead of sending the replica into recovery. Previously whether the retry happened depended on which exception the client reported outermost. [SOLR-18346](https://issues.apache.org/jira/browse/SOLR-18346) (Serhiy Bzhezytskyy)
- Prevent partial writes when a collection referenced by a MIGRATE routing rule has been deleted [SOLR-18413](https://issues.apache.org/jira/browse/SOLR-18413) (ZhenyuLi @JHSUYU)
- Support repeated parents.preFilter values in nested vector knn queries [SOLR-18039](https://issues.apache.org/jira/browse/SOLR-18039) (Arup Chauhan @arup-chauhan)
- Fixed --cloud option is not honored when running example (-e) on Windows in Solr CLI #4075 (Rahul Goswami)
- Don't buffer updates on replicas without update log (e.g. PULL). [SOLR-17231](https://issues.apache.org/jira/browse/SOLR-17231) (Andrzej Bialecki)
- Keep request processing and searches on a stable schema snapshot when the schema changes concurrently [SOLR-18350](https://issues.apache.org/jira/browse/SOLR-18350) (Shrey Narayan @NextbrickInc)

### Deprecated (1 change)

- Un-deprecate 'qt' for certain use-cases where there's no alternative. [SOLR-17715](https://issues.apache.org/jira/browse/SOLR-17715) (David Smiley @dsmiley)

### Removed (2 changes)

- Remove gosu from the Docker image. The Solr Docker image no longer installs the gosu binary. [SOLR-17353](https://issues.apache.org/jira/browse/SOLR-17353) (Jan Høydahl)
- Removed LocalTikaExtractionBackend from the extraction module (SolrCell). Extraction using a remote Tika Server is now the only and default option. Tika-core is upgraded to v3.2.3 and still used for some SAX parsing [SOLR-18037](https://issues.apache.org/jira/browse/SOLR-18037) (Jan Høydahl)

### Other (4 changes)

- Centralize Maven repository declarations for the build. Can customize with SOLR_MAVEN_REPO_URL. [PR#4677](https://github.com/apache/solr/pull/4677) (David Smiley)
- CborResponseWriter should use content-typ- application/cbor [SOLR-17787](https://issues.apache.org/jira/browse/SOLR-17787) (Sanjay Kumar Yadav)
- Provide an internal API to force distributed search (even when one shard). Also, refactor/reorganize SearchHandler for clarity & extensibility. [SOLR-17982](https://issues.apache.org/jira/browse/SOLR-17982) (David Smiley) (Sonu Sharma)
- New SolrJ SolrRequest.processWithBaseUrl, new HttpSolrClientBase.requestWithBaseUrl. HttpJdkSolrClient.requestWithBaseUrl ported from 10x. [SOLR-17996](https://issues.apache.org/jira/browse/SOLR-17996) (David Smiley)


