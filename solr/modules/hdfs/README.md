Apache Solr HDFS Module
===============================

Introduction
------------
This module implements the support for Hadoop Distributed File System in Apache Solr. 

Building
--------
The HDFS module uses the same Gradle build as the core Solr components. 

To build the module, you can use

```
./gradlew :solr:modules:hdfs:assemble
```

The resulting module will be placed to the libs directory, for example:
`solr/modules/hdfs/build/libs/solr-hdfs-9.0.0-SNAPSHOT.jar`

To execute the module tests:

```
./gradlew :solr:modules:hdfs:test
```

Usage
-----
Please refer to the 'Running Solr on HDFS' section of the reference guide: https://solr.apache.org/guide/running-solr-on-hdfs.html
