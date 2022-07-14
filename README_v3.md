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
enterprise, ecommerce, and analytics needs,
written in Java and using [Apache Lucene](https://lucene.apache.org/).

Key links
  - Solr Website
  - Ref Guide
  - Slack Community (not the ASF one??)
  - ? What else?


## Download

Downloads for the Apache Solr distribution are available from https://solr.apache.org/downloads.html.

## Running Solr

### Installing Solr

The Reference Guide contains an entire  [Deployment Guide](https://solr.apache.org/guide/solr/latest/deployment-guide/system-requirements.html) to walk you through installing Solr.

### Running Solr in Docker

You can run Solr in Docker via the [official image](https://hub.docker.com/_/solr).
Learn more about [Solr in Docker](https://solr.apache.org/guide/solr/latest/deployment-guide/solr-in-docker.html)

### Running Solr on Kubernetes

Solr has official support for running on Kubernetes, in the official Docker image.
Please refer to the [Solr Operator](https://solr.apache.org/operator) home for details, tutorials and instructions.

### How to Use

Solr includes a few examples to help you get started. To run a specific example, enter:

```
  bin/solr -e <EXAMPLE> where <EXAMPLE> is one of:
    cloud:         SolrCloud example
    techproducts:  Comprehensive example illustrating many of Solr's core capabilities
    schemaless:    Schema-less example (schema is inferred from data during indexing)
    films:         Example of starting with _default configset and adding explicit fields dynamically    
```

For instance, if you want to run the techproducts example, enter:

```
  bin/solr -e techproducts
```

There are a number of tutorials to follow.  Start out with [5 Minutes to Indexing and Querying Solr](MOVE TUTORIAL TO REF GUIDE),
and then move to the ecommerce inspired [Tech Products](https://solr.apache.org/guide/solr/latest/getting-started/tutorial-techproducts.html) tutorial.   Go deep into faceting your data with some [Film data](https://solr.apache.org/guide/solr/latest/getting-started/tutorial-films.html), as well as learn about dynamic schemas.   The third tutorial is built around (Indexing Your Own Data)[https://solr.apache.org/guide/solr/latest/getting-started/tutorial-diy.html] so you develop some hands on experience with Solr.

You can wrap your learning path by running through [Scaling with SolrCloud](https://solr.apache.org/guide/solr/latest/getting-started/tutorial-solrcloud.html),
and some practice deploying [Solr on Amazon AWS](https://solr.apache.org/guide/solr/latest/getting-started/tutorial-aws.html).


## Support

- [Mailing Lists](https://solr.apache.org/community.html#mailing-lists-chat)
- Slack: Solr Community Channel.  Sign up at https://s.apache.org/solr-slack
- IRC: `#solr` on (libera.chat)[https://web.libera.chat/?channels=#solr]


## Get Involved
Please review the [Contributing to Solr Guide](https://cwiki.apache.org/confluence/display/solr/HowToContribute)
for information on contributing.

To get involved in the developer community:

- [Mailing Lists](https://solr.apache.org/community.html#mailing-lists-chat)
- Slack: `#solr-dev` in the `the-asf` organization.  Sign up at https://the-asf.slack.com/messages/CE70MDPMF
- [Issue Tracker (JIRA)](https://issues.apache.org/jira/browse/SOLR)
- IRC: `#solr-dev` on (libera.chat)[https://web.libera.chat/?channels=#solr-dev]

Learn more about developing Solr by reading through the developer docs in [./dev-docs](./dev-docs) source tree.
