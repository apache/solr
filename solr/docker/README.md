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

Solr Docker
----

This Solr docker module creates both a local Docker image from the source as well as the official Solr Dockerfile.
This allows for local images to be completely compatible with the official Solr images available on DockerHub.

In order to build/test/tag your Docker images using local Solr source code, please refer to `./gradlew helpDocker` in a git checkout or a source-release download.

Please refer to the [Solr Reference Guide](https://solr.apache.org/guide/solr-in-docker.html) for information on using the Solr Docker image.

Building from the Solr Binary Distribution
----

Officially-compliant Docker images can be built directly from the Solr binary distribution (i.e. `solr-<version>.tgz`).
A Dockerfile is included in the binary distribution, under `solr-<version>/docker/Dockerfile`, and is the same one used when building a docker image via Gradle.

To build the Docker image, pass the Solr TGZ as the Docker context and provide the path of the Dockerfile.
Note, that Docker will accept either a URL or a local TGZ file, but each require slightly different syntax.
Therefore custom Solr releases or official releases can be used to create custom Solr images.

```bash
docker build -f solr-X.Y.Z/docker/Dockerfile - < solr-X.Y.Z.tgz
docker build -f solr-X.Y.Z/docker/Dockerfile https://www.apache.org/dyn/closer.lua/solr/X.Y.Z/solr-X.Y.Z.tgz
```

When building the image, Solr accepts arguments for customization. Currently only one argument is accepted:

- `BASE_IMAGE`: Change the base java image for Solr. This can be used to change java versions, jvms, etc.

```bash
docker build --build-arg BASE_IMAGE=custom/jdk:17-slim -f solr-X.Y.Z/docker/Dockerfile https://www.apache.org/dyn/closer.lua/solr/X.Y.Z/solr-X.Y.Z.tgz
```

Official Image Management
----

Please refer to the `dev-docs` in [apache/solr-docker](https://github.com/apache/solr-docker) for information on how the [Official Solr Dockerfile](https://hub.docker.com/_/solr) is maintained & released.
