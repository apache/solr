Apache Solr Hadoop Authentication Module
===============================

Introduction
------------
This module implements the support for Hadoop Authentication in Apache Solr. 

Building
--------
The Hadoop Authentication module uses the same Gradle build as the core Solr components. 

To build the module, you can use

```
./gradlew :solr:modules:hadoop-auth:assemble
```

The resulting module will be placed to the libs directory, for example:
`solr/modules/hdfs/build/libs/solr-hadoop-auth-9.0.0-SNAPSHOT.jar`

To execute the module tests:

```
./gradlew :solr:modules:hadoop-auth:test
```

Usage
-----
Please refer to the 'Hadoop Authentication Plugin' and 'Kerberos Authentication Plugin' sections of the reference guide: https://solr.apache.org/guide/solr/latest/deployment-guide/hadoop-authentication-plugin.html and https://solr.apache.org/guide/solr/latest/deployment-guide/kerberos-authentication-plugin.html
