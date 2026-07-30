<!-- @formatter:off -->
<!-- noinspection -->
<!-- Prevents auto format, for JetBrains IDE File > Settings > Editor > Code Style (Formatter Tab) > Turn formatter on/off with markers in code comments  -->

<!-- This file is automatically generate by logchange tool 🌳 🪓 => 🪵 -->
<!-- Visit https://github.com/logchange/logchange and leave a star 🌟 -->
<!-- !!! ⚠️ DO NOT MODIFY THIS FILE, YOUR CHANGES WILL BE LOST ⚠️ !!! -->


[9.8.1] - 2025-03-11
--------------------

### Fixed (9 changes)

- If multiple query param keys are sent that only vary by case, they were wrongly merged when doing distributed search (sharded collections). This could likely occur for fielded parameters such as f.CASE_SENSITIVE_FIELD.facet.limit=50. [SOLR-17221](https://issues.apache.org/jira/browse/SOLR-17221) (Yue Yu) (David Smiley)
- Metrics: wt=prometheus fix for non-compliant exposition format containing duplicate TYPE lines. [SOLR-17587](https://issues.apache.org/jira/browse/SOLR-17587) (Matthew Biscocho) (David Smiley)
- LBHttp2SolrClient can fail to complete async requests in certain error scenarios. This can cause the HttpShardHandler to indefinitely wait on a completed response that will never come. [SOLR-17637](https://issues.apache.org/jira/browse/SOLR-17637) (Houston Putman)
- Fixes collection creation failure when using a replica placement plugin with basic auth. SolrCloudManager now directly uses HttpSolrClientProvider's client, resolving missing auth listeners. [SOLR-17644](https://issues.apache.org/jira/browse/SOLR-17644) (Sanjay Dutt) (David Smiley)
- Fix a regression of faceting on multi-valued EnumFieldType, introduced by an earlier 9.x version. [SOLR-17649](https://issues.apache.org/jira/browse/SOLR-17649) (Thomas Wöckinger) (David Smiley)
- Fix a bug that could cause long leader elections to leave PULL replicas in DOWN state forever. [SOLR-17652](https://issues.apache.org/jira/browse/SOLR-17652) (hossman)
- Fix unnecessary memory allocation caused by a large reRankDocs param. [SOLR-17670](https://issues.apache.org/jira/browse/SOLR-17670) (JiaBao Gao)
- Disable multi-threaded search execution by default at the node-level. Users can still opt-in by providing a "indexSearcherExecutorThreads" &gt; 0. [SOLR-17673](https://issues.apache.org/jira/browse/SOLR-17673) (Houston Putman) (Varun Thacker) (David Smiley) (Luke Kot-Zaniewski)
- Before attempting a delete by query ("DBQ"), Solr now checks whether the provided query can be run using a Lucene IndexSearcher, and aborts the operation with a '400' error if it cannot. [SOLR-17677](https://issues.apache.org/jira/browse/SOLR-17677) (Jason Gerlowski)


