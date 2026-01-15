<!-- @formatter:off -->
<!-- noinspection -->
<!-- Prevents auto format, for JetBrains IDE File > Settings > Editor > Code Style (Formatter Tab) > Turn formatter on/off with markers in code comments  -->

<!-- This file is automatically generate by logchange tool 🌳 🪓 => 🪵 -->
<!-- Visit https://github.com/logchange/logchange and leave a star 🌟 -->
<!-- !!! ⚠️ DO NOT MODIFY THIS FILE, YOUR CHANGES WILL BE LOST ⚠️ !!! -->


[9.10.1] - 2026-01-08
---------------------

### Fixed (5 changes)

- When using SolrCell with TikaServer, the connection will no longer timeout after 30s idle, such as during OCR processing [PR#3926](https://github.com/apache/solr/pull/3926) ([Jan Høydahl](https://home.apache.org/phonebook.html?uid=janhoy))
- Retry creation of ZK lock on transient connection loss. [SOLR-17972](https://issues.apache.org/jira/browse/SOLR-17972) (Pierre Salagnac)
- Make distributed no-rows queries fast again [SOLR-17985](https://issues.apache.org/jira/browse/SOLR-17985) ([Houston Putman](https://home.apache.org/phonebook.html?uid=houston) @HoustonPutman)
- Fix reverse distance sorting on LatLonPointSpatialField and "SRPT" fields when combined with the filter cache. This is a regression since Solr 9.9. [SOLR-18006](https://issues.apache.org/jira/browse/SOLR-18006) [SOLR-18016](https://issues.apache.org/jira/browse/SOLR-18016) ([Jan Høydahl](https://home.apache.org/phonebook.html?uid=janhoy)) (Umut Saribiyik @umut-sar) (David Smiley)
- Ensure ParallelHttpShardHandler records submit failures so distributed requests don’t hang [SOLR-17983](https://issues.apache.org/jira/browse/SOLR-17983) (Mark Miller)

### Dependency Upgrades (1 change)

- Upgrade Log4j to 2.25.3 !3963 ([Piotr P. Karwasz](https://home.apache.org/phonebook.html?uid=pkarwasz) @ppkarwasz)

### Security (1 change)

- Mitigate CVE-2025-54988 by disabling XFA parsing in PDF documents when using SolrCell extraction [SOLR-17888](https://issues.apache.org/jira/browse/SOLR-17888) ([Jan Høydahl](https://home.apache.org/phonebook.html?uid=janhoy))


