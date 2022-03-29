Apache Solr SQL Module
===============================

Introduction
------------
This module implements the support for SQL in Apache Solr. 

Building
--------
The SQL module uses the same Gradle build as the core Solr components. 

To build the module, you can use

```
./gradlew :solr:modules:sql:assemble
```

The resulting module will be placed to the libs directory, for example:
`solr/modules/hdfs/build/libs/solr-sql-9.0.0-SNAPSHOT.jar`

To execute the module tests:

```
./gradlew :solr:modules:sql:test
```

Usage
-----
Please refer to the 'SQL Query Language' section of the reference guide: https://solr.apache.org/guide/solr/latest/query-guide/sql-query.html
