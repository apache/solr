<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

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
Please refer to the 'Running Solr on HDFS' section of the reference guide: https://solr.apache.org/guide/solr/latest/deployment-guide/solr-on-hdfs.html
