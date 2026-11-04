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

import com.github.gradle.node.npm.task.NpmTask

// Downloads the pre-built @scalar/api-reference browser bundle via npm, for :solr:webapp to
// include in the war. Keeps the large generated bundle out of source control, unlike a
// hand-vendored copy.

plugins {
  id("base")
  alias(libs.plugins.nodegradle.node)
}

description = "Downloads the Scalar API Reference browser bundle for the Admin UI's API Explorer"

val scalarBundleDir = layout.buildDirectory.dir("scalarBundle").get().asFile

val scalarBundle = configurations.create("scalarBundle") {
  isCanBeConsumed = true
  isCanBeResolved = false
}

val downloadScalarApiReference = tasks.register<NpmTask>("downloadScalarApiReference") {
  args.set(listOf("install", "@scalar/api-reference@${libs.versions.scalar.api.reference.get()}"))

  inputs.property("scalar-api-reference version", libs.versions.scalar.api.reference.get())
  outputs.dir(project.extra["nodeProjectDir"].toString() + "/node_modules/@scalar/api-reference")
}

val copyScalarBundle = tasks.register<Sync>("copyScalarBundle") {
  dependsOn(downloadScalarApiReference)

  from(project.extra["nodeProjectDir"].toString() + "/node_modules/@scalar/api-reference/dist/browser") {
    include("standalone.js")
  }

  into(scalarBundleDir)
}

artifacts {
  add("scalarBundle", scalarBundleDir) {
    builtBy(copyScalarBundle)
  }
}
