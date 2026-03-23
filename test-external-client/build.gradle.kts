/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    java
}

group = "org.apache.solr"
version = "1.0-SNAPSHOT"
description = "A smoke test for solr-test-framework & SolrJ dependencies and usage."

val solrVersion = providers.gradleProperty("solr.version").orElse("11.0.0-SNAPSHOT")
val localSolrRepo =
    providers.gradleProperty("local.solr.repo")
        .orElse("${System.getProperty("user.dir")}/build/maven-local")

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(21)
}

repositories {
    maven {
        name = "local-solr-repo"
        url = uri(localSolrRepo.map { "file://$it" })
    }
    mavenCentral()
}

// Approximate Maven updatePolicy=always for changing versions such as -SNAPSHOT.
configurations.configureEach {
    resolutionStrategy.cacheChangingModulesFor(0, "seconds")
    resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
}

dependencies {
    testImplementation("org.apache.solr:solr-solrj:${solrVersion.get()}")
    testImplementation("org.apache.solr:solr-test-framework:${solrVersion.get()}")
}

tasks.withType<Test>().configureEach {
    useJUnit()
}

layout.buildDirectory.set(file("build"))

