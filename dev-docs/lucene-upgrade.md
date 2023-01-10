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

# Lucene upgrade steps

## Read

https://lucene.apache.org/core/9_4_0/MIGRATE.html

## Start

Create a new branch locally e.g. `git checkout -b lucene940 -t origin/main` for upgrading to Lucene 9.4.0 version.

## Build

### `versions.props` update

```
- org.apache.lucene:*=9.3.0
+ org.apache.lucene:*=9.4.0
```

### `versions.lock` update

```
gradlew --write-locks
```

### `solr/licenses` update

```
gradlew updateLicenses

git add solr/licenses
```

## Code

```
gradlew compileJava
```

* adjust for signature changes e.g.
  * `int` to `long` type change
  * additional or removed constructor arguments
  * additional abstract base class or interface methods
  * inner classes becoming outer classes
* codec changes (if any)
  * conceptually `s/org.apache.lucene.codecs.lucene9x.Lucene9x/org.apache.lucene.codecs.lucene94.Lucene94`

## Test

```
gradlew compileTestJava
```

```
gradlew precommit
```

```
gradlew test
```

## Finish

Push the local branch to github (fork) and open a pull request.

