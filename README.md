<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

# Welcome to the Apache Solr project!
-----------------------------------

Solr is the popular, blazing fast open source search platform for all your
enterprise, e-commerce, and analytics needs, built on [Apache Lucene](https://lucene.apache.org/).

For a complete description of the Solr project, team composition, source
code repositories, and other details, please see the Solr web site at
https://solr.apache.org/

## Download

Downloads for Apache Solr distributions are available at https://solr.apache.org/downloads.html.

## Running Solr

### Installing Solr

The Reference Guide contains an entire [Deployment Guide](https://solr.apache.org/guide/solr/latest/deployment-guide/system-requirements.html) to walk you through installing Solr.

### Running Solr in Docker

You can run Solr in Docker via the [official image](https://hub.docker.com/_/solr).
Learn more about [Solr in Docker](https://solr.apache.org/guide/solr/latest/deployment-guide/solr-in-docker.html)

### Running Solr on Kubernetes

Solr has official support for running on Kubernetes, in the official Docker image.
Please refer to the [Solr Operator](https://solr.apache.org/operator) home for details, tutorials and instructions.

## How to Use

Solr includes a few examples to help you get started. To run a specific example, enter:

```
  bin/solr start -e <EXAMPLE> where <EXAMPLE> is one of:
    cloud:         SolrCloud example
    techproducts:  Comprehensive example illustrating many of Solr's core capabilities
    schemaless:    Schema-less example (schema is inferred from data during indexing)
    films:         Example of starting with _default configset and adding explicit fields dynamically    
```

For instance, if you want to run the techproducts example, enter:

```
  bin/solr start -e techproducts
```

For a more in-depth introduction, please check out the [tutorials in the Solr Reference
Guide](https://solr.apache.org/guide/solr/latest/getting-started/solr-tutorial.html).


## Support

- [Users Mailing List](https://solr.apache.org/community.html#mailing-lists-chat)
- Slack: Solr Community Channel.  Sign up at https://s.apache.org/solr-slack
- IRC: `#solr` on [libera.chat](https://web.libera.chat/?channels=#solr)


## Get Involved
Please review [CONTRIBUTING.md](CONTRIBUTING.md) for information on contributing to the project.

To get involved in the developer community:

- [Mailing Lists](https://solr.apache.org/community.html#mailing-lists-chat)
- Slack: `#solr-dev` in the `the-asf` organization.  Sign up at https://the-asf.slack.com/messages/CE70MDPMF
- [Issue Tracker (JIRA)](https://issues.apache.org/jira/browse/SOLR)
- IRC: `#solr-dev` on [libera.chat](https://web.libera.chat/?channels=#solr-dev)

Learn more about developing Solr by reading through the developer docs in [./dev-docs](./dev-docs) source tree.
