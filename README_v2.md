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

Solr is the popular, blazing fast open source enterprise search platform
written in Java and using [Apache Lucene](https://lucene.apache.org/).

For a complete description of the Solr project, team composition, source
code repositories, and other details, please see the Solr web site at
https://solr.apache.org/

## Online Documentation

This README file only contains basic instructions.  For comprehensive documentation,
visit the [Solr Reference Guide](https://solr.apache.org/guide/).

## Getting Started
FIXME maybe put the tutorial that Noble or Ishan wrote???   Then at the end put the examples?

### Starting Solr

Start a Solr node in cluster mode (SolrCloud mode)

```
bin/solr -c
```

To start another Solr node and have it join the cluster alongside the first node,

```
bin/solr -c -z localhost:9983 -p 8984
```

An instance of the cluster coordination service, i.e. Zookeeper, was started on port 9983 when the first node was started. To start Zookeeper separately, please refer to XXXX.

### Creating a collection

Like a database system holds data in tables, Solr holds data in collections. A collection can be created as follows:

```
curl --request POST \
--url http://localhost:8983/api/collections \
--header 'Content-Type: application/json' \
--data '{
  "create": {
    "name": "techproducts",
    "numShards": 1,
    "replicationFactor": 1
  }
}'
```

### Defining a schema

Let us define some of the fields that our documents will contain.

```
curl --request POST \
  --url http://localhost:8983/api/collections/techproducts/schema \
  --header 'Content-Type: application/json' \
  --data '{
  "add-field": [
    {"name": "name", "type": "text_general", "multiValued": false},
    {"name": "cat", "type": "string", "multiValued": true},
    {"name": "manu", "type": "string"},
    {"name": "features", "type": "text_general", "multiValued": true},
    {"name": "weight", "type": "pfloat"},
    {"name": "price", "type": "pfloat"},
    {"name": "popularity", "type": "pint"},
    {"name": "inStock", "type": "boolean", "stored": true},
    {"name": "store", "type": "location"}
  ]
}'
```

### Indexing documents

A single document can be indexed as:

```
curl --request POST \
--url 'http://localhost:8983/api/collections/techproducts/update' \
  --header 'Content-Type: application/json' \
  --data '  {
    "id" : "978-0641723445",
    "cat" : ["book","hardcover"],
    "name" : "The Lightning Thief",
    "author" : "Rick Riordan",
    "series_t" : "Percy Jackson and the Olympians",
    "sequence_i" : 1,
    "genre_s" : "fantasy",
    "inStock" : true,
    "price" : 12.50,
    "pages_i" : 384
  }'
```

Multiple documents can be indexed in the same request:
```
curl --request POST \
  --url 'http://localhost:8983/api/collections/techproducts/update' \
  --header 'Content-Type: application/json' \
  --data '  [
  {
    "id" : "978-0641723445",
    "cat" : ["book","hardcover"],
    "name" : "The Lightning Thief",
    "author" : "Rick Riordan",
    "series_t" : "Percy Jackson and the Olympians",
    "sequence_i" : 1,
    "genre_s" : "fantasy",
    "inStock" : true,
    "price" : 12.50,
    "pages_i" : 384
  }
,
  {
    "id" : "978-1423103349",
    "cat" : ["book","paperback"],
    "name" : "The Sea of Monsters",
    "author" : "Rick Riordan",
    "series_t" : "Percy Jackson and the Olympians",
    "sequence_i" : 2,
    "genre_s" : "fantasy",
    "inStock" : true,
    "price" : 6.49,
    "pages_i" : 304
  }
]'
```

A file containing the documents can be indexed as follows:
```
curl -H "Content-Type: application/json" \
       -X POST \
       -d @example/products.json \
       --url 'http://localhost:8983/api/collections/techproducts/update?commit=true'
```

### Commit
After documents are indexed into a collection, they are not immediately available for searching. In order to have them searchable, a commit operation (also called `refresh` in other search engines like OpenSearch etc.) is needed. Commits can be scheduled at periodic intervals using auto-commits as follows.

```
curl -X POST -H 'Content-type: application/json' -d '{"set-property":{"updateHandler.autoCommit.maxTime":15000}}' http://localhost:8983/api/collections/techproducts/config
```  

### Basic search queries
FIXME

### Solr Examples

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

### Running Solr in Docker

You can run Solr in Docker via the [official image](https://hub.docker.com/_/solr).

To run Solr in a container and expose the Solr port, run:

`docker run -p 8983:8983 solr`

In order to start Solr in cloud mode, run the following.

`docker run -p 8983:8983 solr solr-fg -c`

For documentation on using the official docker builds, please refer to the [DockerHub page](https://hub.docker.com/_/solr).  
Up to date documentation for running locally built images of this branch can be found in the [local reference guide](solr/solr-ref-guide/src/running-solr-in-docker.adoc).

There is also a gradle task for building custom Solr images from your local checkout.
These local images are built identically to the official image except for retrieving the Solr artifacts locally instead of from the official release.
This can be useful for testing out local changes as well as creating custom images for yourself or your organization.
The task will output the image name to use at the end of the build.

`./gradlew docker`

For more info on building an image, run:

`./gradlew helpDocker`

### Running Solr on Kubernetes

Solr has official support for running on Kubernetes, in the official Docker image.
Please refer to the [Solr Operator](https://solr.apache.org/operator) home for details, tutorials and instructions.

## Building Solr from Source
Download the Java 11 JDK (Java Development Kit) or later.  We recommend the OpenJDK
distribution Eclipse Temurin available from https://adoptium.net/.
You will need the JDK installed, and the $JAVA_HOME/bin (Windows: %JAVA_HOME%\bin)
folder included on your command path. To test this, issue a "java -version" command
from your shell (command prompt) and verify that the Java version is 11 or later.

Download the Apache Solr distribution, from https://solr.apache.org/downloads.html.
Unzip the distribution to a folder of your choice, e.g. C:\solr or ~/solr
Alternately, you can obtain a copy of the latest Apache Solr source code
directly from the Git repository:

<https://solr.apache.org/community.html#version-control>

Solr uses [Gradle](https://gradle.org/) as the build
system.  Navigate to the root of your source tree folder and issue the `./gradlew tasks`
command to see the available options for building, testing, and packaging Solr.

`./gradlew dev` will create a Solr executable suitable for development.
cd to `./solr/packaging/build/dev` and run the `bin/solr` script
to start Solr.

NOTE: `gradlew` is the "Gradle Wrapper" and will automatically download and
start using the correct version of Gradle for Solr.

NOTE: `./gradlew help` will print a list of high-level tasks. There are also a
number of plain-text files in <source folder root>/help.

The first time you run Gradle, it will create a file "gradle.properties" that
contains machine-specific settings. Normally you can use this file as-is, but it
can be modified if necessary.

Note as well that the gradle build does not create or copy binaries throughout the
source repository so you need to switch to the packaging output folder `./solr/packaging/build`;
the rest of the instructions below remain identical. The packaging directory
is rewritten on each build.

If you want to build the documentation, type `./gradlew -p solr documentation`.

`./gradlew check` will assemble Solr and run all validation tasks unit tests.

To build the final Solr artifacts run `./gradlew assemble`.

Lastly, there is developer oriented documentation in `./dev-docs/README.adoc` that
you may find useful in working with Solr.


### Gradle build and IDE support

- *IntelliJ* - IntelliJ idea can import the project out of the box.
               Code formatting conventions should be manually adjusted.
- *Eclipse*  - Not tested.
- *Netbeans* - Not tested.

## Contributing

Please review the [Contributing to Solr Guide](https://cwiki.apache.org/confluence/display/solr/HowToContribute)
for information on contributing.

## Discussion and Support

- [Mailing Lists](https://solr.apache.org/community.html#mailing-lists-chat)
- [Issue Tracker (JIRA)](https://issues.apache.org/jira/browse/SOLR)
- IRC: `#solr` and `#solr-dev` on libera.chat
- [Slack](https://solr.apache.org/community.html#slack)

## Export control

This distribution includes cryptographic software.  The country in
which you currently reside may have restrictions on the import,
possession, use, and/or re-export to another country, of
encryption software.  BEFORE using any encryption software, please
check your country's laws, regulations and policies concerning the
import, possession, or use, and re-export of encryption software, to
see if this is permitted.  See <https://www.wassenaar.org/> for more
information.

The U.S. Government Department of Commerce, Bureau of Industry and
Security (BIS), has classified this software as Export Commodity
Control Number (ECCN) 5D002.C.1, which includes information security
software using or performing cryptographic functions with asymmetric
algorithms.  The form and manner of this Apache Software Foundation
distribution makes it eligible for export under the License Exception
ENC Technology Software Unrestricted (TSU) exception (see the BIS
Export Administration Regulations, Section 740.13) for both object
code and source code.

The following provides more details on the included cryptographic
software:

Apache Solr uses Apache Tika which uses the Bouncy Castle generic encryption libraries for
extracting text content and metadata from encrypted PDF files.
See https://www.bouncycastle.org/ for more details on Bouncy Castle.
