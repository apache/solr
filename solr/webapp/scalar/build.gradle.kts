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

// The Admin UI's API Explorer: a standalone page that renders the generated OpenAPI spec with
// Scalar. It can't be an ordinary Admin UI screen because Scalar manages its own URL hash for
// deep-linking, which conflicts with AngularJS's ngRoute (both rewrite window.location.hash) and
// causes an infinite digest/navigation loop. The Admin UI instead loads this page in an iframe,
// and :solr:webapp mounts this project's output at libs/scalar in the war.

plugins {
  id("base")
}

description = "The Admin UI's API Explorer, a standalone Scalar page loaded in an iframe"

// Scalar's prebuilt browser bundle is published only to npm. Fetch the package archive directly
// (from npm's registry, or the mirror the node-based builds use) rather than running npm.
val propertyOrEnvOrDefault: groovy.lang.Closure<*> by extra
val npmRegistry = propertyOrEnvOrDefault.call("solr.npm.registry", "SOLR_NPM_REGISTRY", "")
  .toString()
  .ifEmpty { "https://registry.npmjs.org" }

repositories {
  exclusiveContent {
    forRepository {
      ivy {
        name = "scalarNpm"
        url = uri(npmRegistry)
        patternLayout {
          artifact("[organisation]/[module]/-/[module]-[revision].[ext]")
        }
        metadataSources { artifact() }
      }
    }
    filter { includeModule("@scalar", "api-reference") }
  }
}

val scalarArchive = configurations.create("scalarArchive") {
  isTransitive = false
}

dependencies {
  scalarArchive(libs.scalar.api.reference) {
    artifact {
      type = "tgz"
      extension = "tgz"
    }
  }
}

val scalarBundleDir = layout.buildDirectory.dir("scalarBundle")

val scalarBundle = configurations.create("scalarBundle") {
  isCanBeConsumed = true
  isCanBeResolved = false
}

val assembleScalarBundle = tasks.register<Sync>("assembleScalarBundle") {
  from("web")
  from(provider { tarTree(resources.gzip(scalarArchive.singleFile)) }) {
    include("package/dist/browser/standalone.js")
    eachFile { path = name }
    includeEmptyDirs = false
  }

  into(scalarBundleDir)
}

artifacts {
  add("scalarBundle", scalarBundleDir) {
    builtBy(assembleScalarBundle)
  }
}
