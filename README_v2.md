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

### Solr Examples

Solr includes a few examples to help you get started. To run a specific example, enter:

```
  bin/solr -e <EXAMPLE> where <EXAMPLE> is one of:
    FIXME the labels in the README do NOT match what is in the bin/solr start -help, and are better!
    cloud:         SolrCloud example
    techproducts:  Comprehensive example illustrating many of Solr's core capabilities
    schemaless:    Schema-less example (schema is inferred from data during indexing)
    films:         Example of starting with _default configset and adding explicit fields dynamically    
```

For instance, if you want to run the SolrCloud example, enter:

```
  bin/solr -e cloud
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
FIXME --> is this where we get JDK from??   What about OpenJDK?
Download the Java 11 JDK (Java Development Kit) or later from https://jdk.java.net/
You will need the JDK installed, and the $JAVA_HOME/bin (Windows: %JAVA_HOME%\bin)
folder included on your command path. To test this, issue a "java -version" command
from your shell (command prompt) and verify that the Java version is 11 or later.

Download the Apache Solr distribution, from http://solr.apache.org/.  FIXME link??
Unzip the distribution to a folder of your choice, e.g. C:\solr or ~/solr
Alternately, you can obtain a copy of the latest Apache Solr source code
directly from the Git repository:

<https://solr.apache.org/community.html#version-control>

As of 9.0, Solr uses [Gradle](https://gradle.org/) as the build
system.  Navigate to the root of your source tree folder and issue the `./gradlew tasks`
command to see the available options for building, testing, and packaging Solr.

`./gradlew assemble` will create a Solr executable.
cd to `./solr/packaging/build/solr-9.0.0-SNAPSHOT` and run the `bin/solr` script
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
- IRC: `#solr` and `#solr-dev` on freenode.net
- [Slack](https://solr.apache.org/community.html#slack)

# Export control
-------------------------------------------------
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
