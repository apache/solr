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

Welcome to the Apache Solr project!
-----------------------------------

Solr is the popular, blazing fast open source search platform for all your
enterprise, e-commerce, and analytics needs, built on Apache Lucene.

For a complete description of the Solr project, team composition, source
code repositories, and other details, please see the Solr web site at
https://solr.apache.org/solr

How to Use
----------

To get started run

```
  bin/solr -help
```

Solr includes a few examples to help you get started.  For instance, if you want
to run the techproducts example, enter:

```
  bin/solr start -e techproducts
```

For a more in-depth introduction, please check out the tutorials in the Solr Reference
Guide: https://solr.apache.org/guide/solr/latest/getting-started/solr-tutorial.html.

Support
-------

- Users Mailing List: https://solr.apache.org/community.html#mailing-lists-chat
- Slack: Solr Community Channel.  Sign up at https://s.apache.org/solr-slack
- IRC: #solr on libera.chat: https://web.libera.chat/?channels=#solr



Files included in an Apache Solr binary distribution
----------------------------------------------------

server/
  A self-contained Solr instance, complete with a sample
  configuration and documents to index. Please see: bin/solr start -help
  for more information about starting a Solr server.

bin/
   Scripts to startup, manage and interact with Solr instances.

example/
  Contains example documents and an alternative Solr home
  directory containing various examples.

modules/
  Contains modules to extend the functionality of Solr.
  Libraries for these modules can be found under modules/*/lib

prometheus-exporter/
  Contains a separate application to monitor Solr instances and export Prometheus metrics

docker/
  Contains a Dockerfile to build a Docker image using the source or binary distribution.
  `docker/scripts` contains scripts that the Docker image uses to manage Solr.
  Refer to the README.md for instructions on how to build an image.

docs/index.html
  A link to the online version of Apache Solr Javadoc API documentation and Tutorial

licenses/
  Licenses, notice files and signatures for Solr dependencies.
