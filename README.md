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

# Apache Solr

Apache Solr is an enterprise search platform written in Java and using [Apache Lucene](https://lucene.apache.org/).
Major features include full-text search, index replication and sharding, and
result faceting and highlighting.


[![Build Status](https://ci-builds.apache.org/job/Solr/job/Solr-Artifacts-main/badge/icon?subject=Solr)](https://ci-builds.apache.org/job/Solr/job/Solr-Artifacts-main/)


## Online Documentation

This README file only contains basic setup instructions.  For more
comprehensive documentation, visit <https://solr.apache.org/guide/>

## Building with Gradle

Firstly, you need to set up your development environment (OpenJDK 11 or greater).

We'll assume that you know how to get and set up the JDK - if you
don't, then we suggest starting at https://jdk.java.net/ and learning
more about Java, before returning to this README. Solr runs with
Java 11 and later.

As of 9.0, Solr uses [Gradle](https://gradle.org/) as the build
system. Ant build support has been removed.

To build Solr, run (`./` can be omitted on Windows):

`./gradlew assemble`

NOTE: DO NOT use `gradle` command that is already installed on your machine (unless you know what you'll do).
The "gradle wrapper" (gradlew) does the job - downloads the correct version of it, setups necessary configurations.

The first time you run Gradle, it will create a file "gradle.properties" that
contains machine-specific settings. Normally you can use this file as-is, but it
can be modified if necessary.

The command above packages a full distribution of Solr server; the 
package can be located at:

`solr/packaging/build/solr-*`

Note that the gradle build does not create or copy binaries throughout the
source repository so you need to switch to the packaging output folder above;
the rest of the instructions below remain identical. The packaging directory 
is rewritten on each build. 

For development, especially when you have created test indexes etc, use
the `./gradlew dev` task which will copy binaries to `./solr/packaging/build/dev`
but _only_ overwrite the binaries which will preserve your test setup.

If you want to build the documentation, type `./gradlew -p solr documentation`.

## Running Solr

After building Solr, the server can be started using
the `bin/solr` control scripts.  Solr can be run in either standalone or
distributed (SolrCloud mode).

To run Solr in standalone mode, run the following command from the `solr/`
directory:

`bin/solr start`

To run Solr in SolrCloud mode, run the following command from the `solr/`
directory:

`bin/solr start -c`

The `bin/solr` control script allows heavy modification of the started Solr.
Common options are described in some detail in solr/README.txt.  For an
exhaustive treatment of options, run `bin/solr start -h` from the `solr/`
directory.

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

Docker images can also be built from the Solr binary distribution (i.e. `solr-<version>.tgz`).
Please refer to the [Solr Docker README](solr/docker/README.md) for more information.

### Running Solr on Kubernetes

Solr has official support for running on Kubernetes, in the official Docker image.
Please refer to the [Solr Operator](https://solr.apache.org/operator) home for details, tutorials and instructions.

### Gradle build and IDE support

- *IntelliJ* - IntelliJ idea can import the project out of the box. 
               Code formatting conventions should be manually adjusted. 
- *Eclipse*  - Not tested.
- *Netbeans* - Not tested.


### Gradle build and tests

`./gradlew assemble` will build a runnable Solr as noted above.

`./gradlew check` will assemble Solr and run all validation
  tasks unit tests.

`./gradlew help` will print a list of help commands for high-level tasks. One
  of these is `helpAnt` that shows the gradle tasks corresponding to ant
  targets you may be familiar with.

## Contributing

Please review the [Contributing to Solr
Guide](https://cwiki.apache.org/confluence/display/solr/HowToContribute) for information on
contributing.

## Discussion and Support

- [Mailing Lists](https://solr.apache.org/community.html#mailing-lists-chat)
- [Issue Tracker (JIRA)](https://issues.apache.org/jira/browse/SOLR)
- IRC: `#solr` and `#solr-dev` on freenode.net
- [Slack](https://solr.apache.org/community.html#slack) 
