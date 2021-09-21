Apache Solr HDFS Contrib module
===============================

Introduction
------------
This contrib module implements the support for Hadoop Distributed File System in Apache Solr. 


Building
--------
The HDFS contrib module uses the same Gradle build as the core Solr components. 

To build the module, you can use

./gradlew :solr:contrib:hdfs:assemble

The resulting module will be placed to the libs directory, for example:
solr/contrib/hdfs/build/libs/solr-hdfs-9.0.0-SNAPSHOT.jar


To execute the module tests:

./gradlew :solr:contrib:hdfs:test



Usage
-----
To use the module, it needs to be placed to the Solr web application classpath (for example by symlinking it to the WEB-INF/lib directory.)

Please refer to the 'Running Solr on HDFS' section of the reference guide: https://solr.apache.org/guide/running-solr-on-hdfs.html