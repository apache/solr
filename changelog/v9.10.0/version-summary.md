<!-- @formatter:off -->
<!-- noinspection -->
<!-- Prevents auto format, for JetBrains IDE File > Settings > Editor > Code Style (Formatter Tab) > Turn formatter on/off with markers in code comments  -->

<!-- This file is automatically generate by logchange tool 🌳 🪓 => 🪵 -->
<!-- Visit https://github.com/logchange/logchange and leave a star 🌟 -->
<!-- !!! ⚠️ DO NOT MODIFY THIS FILE, YOUR CHANGES WILL BE LOST ⚠️ !!! -->


[9.10.0]
--------

### Added (4 changes)

- Add Amazon Linux as known distro for installing Solr as a service. #3778 (Eric Pugh) (Andreas Rütten)
- shards.preference=replica.location now supports the "host" option for routing to replicas on the same host. [SOLR-17915](https://issues.apache.org/jira/browse/SOLR-17915) (Houston Putman)
- Add fullOuterJoin stream function [SOLR-17923](https://issues.apache.org/jira/browse/SOLR-17923) (Andy Webb)
- The Extraction Request Handler, aka Solr Cell, now supports delegating the parsing of rich documents to an external Tika Server. This allows for a more stable Solr server, and easier to configure and scale parsing independently. The local in-process Tika parser is now deprecated. [SOLR-7632](https://issues.apache.org/jira/browse/SOLR-7632) (Jan Høydahl) (Eric Pugh)

### Changed (6 changes)

- Solr is now able to start on Java 24 and later, but with Security Manager disabled [SOLR-17641](https://issues.apache.org/jira/browse/SOLR-17641) (Houston Putman) (Jan Høydahl)
- DocBasedVersionConstraintsProcessorFactory now supports PULL replicas. [SOLR-17860](https://issues.apache.org/jira/browse/SOLR-17860) (Houston Putman)
- SolrJ users not using deprecated SolrClients can safely exclude Apache HttpClient dependencies. [SOLR-17884](https://issues.apache.org/jira/browse/SOLR-17884) (David Smiley)
- Speed up Remote Proxy for high QPS, utilizing ClusterState caching. [SOLR-17893](https://issues.apache.org/jira/browse/SOLR-17893) (Houston Putman)
- CloudSolrClient now recognizes UnknownHostException as a case to refetch the cluster state and retry. [SOLR-17897](https://issues.apache.org/jira/browse/SOLR-17897) (David Smiley)
- SolrJ CloudSolrClient configured with a Solr URL (not ZK) now refreshes liveNodes in the background. This will reduce spikes in request latency when the cached liveNodes have expired. [SOLR-17921](https://issues.apache.org/jira/browse/SOLR-17921) (Houston Putman) (David Smiley)

### Fixed (16 changes)

- Make solr bin/solr zk CLI tools read ZK_HOST environment as they did pre Solr 9.8. With this regression fixed it is no longer necessary to pass the --zk-host option to the CLI tools if ZK_HOST is set. [SOLR-17690](https://issues.apache.org/jira/browse/SOLR-17690) ([Jan Høydahl](https://home.apache.org/phonebook.html?uid=janhoy) @janhoy)
- Starting solr on newer Windows 11 Home complained about missing wmic [SOLR-17717](https://issues.apache.org/jira/browse/SOLR-17717) (Jan Høydahl)
- NPE can occur when doing Atomic Update using Add Distinct on documents with a null field value. [SOLR-17721](https://issues.apache.org/jira/browse/SOLR-17721) (puneetSharma) (Eric Pugh)
- Fixed dense/sparse representation in LTR module. [SOLR-17760](https://issues.apache.org/jira/browse/SOLR-17760) (Anna Ruggero) (Alessandro Benedetti)
- When Solr forwards/proxies requests to another node that can service the request, it needs to pass authorization headers. [SOLR-17789](https://issues.apache.org/jira/browse/SOLR-17789) (Timo Crabbé)
- RecoveryStrategy.pingLeader could NPE when there's no shard leader [SOLR-17824](https://issues.apache.org/jira/browse/SOLR-17824) (David Smiley)
- v1 Restore API no longer conflates backup-name and collection-name during validation. [SOLR-17830](https://issues.apache.org/jira/browse/SOLR-17830) (Abhishek Umarjikar) (Jason Gerlowski)
- ExitableDirectoryReader always initialized with QueryLimits.NONE [SOLR-17831](https://issues.apache.org/jira/browse/SOLR-17831) (Andrzej Białecki)
- Fixed a bug preventing Config API set properties (aka: configoverlay.json) from being used in config file property substitution [SOLR-17834](https://issues.apache.org/jira/browse/SOLR-17834) (hossman)
- PULL replica nodes could be marked as "preferredLeader" by BALANCESHARDUNIQUE despite never being able to be elected leader [SOLR-17837](https://issues.apache.org/jira/browse/SOLR-17837) (Kevin Liang) (Houston Putman)
- Fix race condition in SolrCore's fingerprint cache which caused leader election to hang. [SOLR-17863](https://issues.apache.org/jira/browse/SOLR-17863) (Luke Kot-Zaniewski) (Matthew Biscocho)
- Avoid creating grouping shard requests when timeAllowed has already run out. [SOLR-17869](https://issues.apache.org/jira/browse/SOLR-17869) (Andrzej Bialecki) (hossman)
- Http2SolrClient wasn't honoring idle timeout configuration above 30 seconds -- a regression. [SOLR-17871](https://issues.apache.org/jira/browse/SOLR-17871) (Thomas Wöckinger) (David Smiley)
- Http2SolrClient: followRedirects=true: if Http2SolrClient was created with followRedirects=true, and then was used to create future clients (via builder.withHttpClient), then redirect processing was wrongly disabled on the shared instance. [SOLR-17876](https://issues.apache.org/jira/browse/SOLR-17876) (David Smiley)
- SolrCLI tools such as "bin/solr zk" can now access jars located in `&lt;install_dir&gt;/lib`. [SOLR-17883](https://issues.apache.org/jira/browse/SOLR-17883) 
- Fix for LB/Cloud SolrClients that could leak on close() if concurrent request failed and triggered zombie server logic. [SOLR-3696](https://issues.apache.org/jira/browse/SOLR-3696) (hossman)

### Dependency Upgrades (37 changes)

- Update apache.zookeeper to v3.9.3 [PR#3061](https://github.com/apache/solr/pull/3061) (solrbot)
- Update amazon.awssdk to v2.31.77 [PR#3228](https://github.com/apache/solr/pull/3228) (solrbot)
- Update actions/checkout action to v5 [PR#3486](https://github.com/apache/solr/pull/3486) (solrbot)
- Update com.carrotsearch.randomizedtesting:randomizedtesting-runner to v2.8.3 [PR#3526](https://github.com/apache/solr/pull/3526) (solrbot)
- Update org.xerial.snappy:snappy-java to v1.1.10.8 [PR#3528](https://github.com/apache/solr/pull/3528) (solrbot)
- Update org.apache.kafka:* to v3.9.1 [PR#3530](https://github.com/apache/solr/pull/3530) (solrbot)
- Update org.apache.zookeeper:* to v3.9.4 [PR#3531](https://github.com/apache/solr/pull/3531) (solrbot)
- Update org.eclipse.jetty*:* to v10.0.26 [PR#3533](https://github.com/apache/solr/pull/3533) (solrbot)
- Update org.glassfish.jersey*:* to v3.1.11 [PR#3534](https://github.com/apache/solr/pull/3534) (solrbot)
- Update org.slf4j:* to v2.0.17 [PR#3535](https://github.com/apache/solr/pull/3535) (solrbot)
- Update com.google.re2j:re2j to v1.8 [PR#3541](https://github.com/apache/solr/pull/3541) (solrbot)
- Update commons-codec:commons-codec to v1.19.0 [PR#3542](https://github.com/apache/solr/pull/3542) (solrbot)
- Update commons-io:commons-io to v2.20.0 [PR#3543](https://github.com/apache/solr/pull/3543) (solrbot)
- Update io.opentelemetry:opentelemetry-bom to v1.53.0 [PR#3544](https://github.com/apache/solr/pull/3544) (solrbot)
- Update org.apache.commons:commons-collections4 to v4.5.0 [PR#3545](https://github.com/apache/solr/pull/3545) (solrbot)
- Update org.apache.commons:commons-compress to v1.28.0 [PR#3546](https://github.com/apache/solr/pull/3546) (solrbot)
- Update org.apache.commons:commons-configuration2 to v2.12.0 [PR#3547](https://github.com/apache/solr/pull/3547) (solrbot)
- Update org.apache.commons:commons-exec to v1.5.0 [PR#3548](https://github.com/apache/solr/pull/3548) (solrbot)
- Update org.apache.hadoop.thirdparty:hadoop-shaded-guava to v1.4.0 [PR#3550](https://github.com/apache/solr/pull/3550) (solrbot)
- Update org.immutables:value-annotations to v2.11.3 [PR#3557](https://github.com/apache/solr/pull/3557) (solrbot)
- Update org.semver4j:semver4j to v5.8.0 [PR#3558](https://github.com/apache/solr/pull/3558) (solrbot)
- Update org.apache.curator:* to v5.9.0 [PR#3561](https://github.com/apache/solr/pull/3561) (solrbot)
- Update plugin com.github.node-gradle.node to v7.1.0 [PR#3564](https://github.com/apache/solr/pull/3564) (solrbot)
- Update plugin com.palantir.consistent-versions to v2.37.0 [PR#3569](https://github.com/apache/solr/pull/3569) (solrbot)
- Update plugin de.undercouch.download to v5.6.0 [PR#3570](https://github.com/apache/solr/pull/3570) (solrbot)
- Update actions/setup-java action to v5 [PR#3571](https://github.com/apache/solr/pull/3571) (solrbot)
- Update plugin com.diffplug.spotless to v7 [PR#3583](https://github.com/apache/solr/pull/3583) (solrbot)
- Update org.hsqldb:hsqldb to v2.7.4 [PR#3586](https://github.com/apache/solr/pull/3586) (solrbot)
- Update net.bytebuddy:* to v1.17.7 [PR#3587](https://github.com/apache/solr/pull/3587) (solrbot)
- Update org.mockito:mockito* to v5.19.0 [PR#3592](https://github.com/apache/solr/pull/3592) (solrbot)
- Update io.netty:* to v4.2.6.Final [PR#3635](https://github.com/apache/solr/pull/3635) (solrbot)
- Update org.apache.commons:commons-lang3 to v3.19.0 [PR#3689](https://github.com/apache/solr/pull/3689) (solrbot)
- Update org.bouncycastle:bcpkix-jdk18on to v1.82 [PR#3721](https://github.com/apache/solr/pull/3721) (solrbot)
- Update org.apache.kerby:* to v2.1.0 [PR#3722](https://github.com/apache/solr/pull/3722) (solrbot)
- Update gradle/actions action to v5 [PR#3735](https://github.com/apache/solr/pull/3735) (solrbot)
- Update plugin de.thetaphi.forbiddenapis to v3.10 [PR#3752](https://github.com/apache/solr/pull/3752) (solrbot)
- Upgrade Lucene to 9.12.3 [SOLR-17964](https://issues.apache.org/jira/browse/SOLR-17964) ([Christine Poerschke](https://home.apache.org/phonebook.html?uid=cpoerschke) @cpoerschke)

### Other (9 changes)

- Deprecate `CloudHttp2SolrClient.Builder#withHttpClient` in favor of `CloudHttp2SolrClient.Builder#withInternalClientBuilder`. Deprecate `LBHttp2SolrClient.Builder#withListenerFactory` in favor of `LBHttp2SolrClient.Builder#withListenerFactories`. [SOLR-17541](https://issues.apache.org/jira/browse/SOLR-17541) (James Dyer)
- Use logchange for changelog management ([Jan Høydahl](https://home.apache.org/phonebook.html?uid=janhoy) @janhoy)
- SolrCloud "live_node" now has metadata: version of Solr, roles [SOLR-17620](https://issues.apache.org/jira/browse/SOLR-17620) (Yuntong Qu) (David Smiley)
- Deprecating waitForFinalState parameter in any SolrCloud command that accepts it. It remains defaulted to false in 9, but will become true and likely removed. [SOLR-17712](https://issues.apache.org/jira/browse/SOLR-17712) (Abhishek Umarjikar) (David Smiley)
- Deprecate `CloudSolrClient.Builder` in favor of `CloudHttp2SolrClient.Builder`. [SOLR-17771](https://issues.apache.org/jira/browse/SOLR-17771) (James Dyer)
- A Solr node will now fail to start if it's major.minor version (e.g. 9.10) is *lower* than that of any existing Solr node in a SolrCloud cluster (as reported by info in "live_node"). [SOLR-17879](https://issues.apache.org/jira/browse/SOLR-17879) (David Smiley)
- Stream decorator test refactoring - use underscore rather than dot in aliases [SOLR-17952](https://issues.apache.org/jira/browse/SOLR-17952) (Andy Webb)
- XLSXResponseWriter has been deprecated and will be removed in a future release. [SOLR-17956](https://issues.apache.org/jira/browse/SOLR-17956) (Jan Høydahl)
- The Tika Language Identifier is deprecated. Use one of the other detectors instead. [SOLR-17958](https://issues.apache.org/jira/browse/SOLR-17958) (Jan Høydahl)


