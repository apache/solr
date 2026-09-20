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
import com.github.gradle.node.npm.task.NpxTask

// Builds the OpenAPI-generated JS client (from :solr:api) into a single bundled
// file, for :solr:webapp to include in the war. This is the only place in the
// build that needs npm/node for this purpose; it can be disabled entirely via
// -PdisableJsClient=true (see settings.gradle).

plugins {
  id("base")
  alias(libs.plugins.nodegradle.node)
}

description = "Generates a JavaScript client for Solr OpenApi"

val jsClientWorkspace = layout.buildDirectory.dir("jsClientWorkspace").get().asFile
val jsClientBuildDir = layout.buildDirectory.dir("jsClientBuild").get().asFile
val jsClientBundleDir = layout.buildDirectory.dir("jsClientBundle").get().asFile

val generatedJSClient = configurations.create("generatedJSClient")
val jsClientBundle = configurations.create("jsClientBundle") {
  isCanBeConsumed = true
  isCanBeResolved = false
}

dependencies {
  generatedJSClient(project(path = ":solr:api", configuration = "jsClient"))
}

val syncJSClientSourceCode = tasks.register<Sync>("syncJSClientSourceCode") {
  from(generatedJSClient)

  into(jsClientWorkspace)

  // Keep the node modules, so that they don't need to be re-downloaded
  preserve {
    include("node_modules/**")
  }
}

val jsClientDownloadDeps = tasks.register<NpmTask>("jsClientDownloadDeps") {
  dependsOn(syncJSClientSourceCode)

  args.set(listOf("install"))
  workingDir.set(jsClientWorkspace)

  inputs.dir("$jsClientWorkspace/src")
  inputs.file("$jsClientWorkspace/package.json")
  outputs.dir("$jsClientWorkspace/node_modules")
}

val jsClientBuild = tasks.register<NpmTask>("jsClientBuild") {
  dependsOn(jsClientDownloadDeps)

  args.set(listOf("run", "build"))
  workingDir.set(jsClientWorkspace)

  inputs.dir("$jsClientWorkspace/src")
  inputs.file("$jsClientWorkspace/package.json")
  inputs.dir("$jsClientWorkspace/node_modules")
  outputs.dir("$jsClientWorkspace/dist")
}

val downloadBrowserify = tasks.register<NpmTask>("downloadBrowserify") {
  args.set(listOf("install", "browserify@${libs.versions.browserify.get()}"))

  inputs.property("browserify version", libs.versions.browserify.get())
  outputs.dir(project.extra["nodeProjectDir"].toString() + "/node_modules/browserify")
}

val generateJsClientBundle = tasks.register<NpxTask>("generateJsClientBundle") {
  dependsOn(downloadBrowserify)
  dependsOn(jsClientBuild)

  command.set("browserify")
  args.set(listOf("dist/index.js", "-s", "solrApi", "-o", "$jsClientBuildDir/index.js"))
  workingDir.set(jsClientWorkspace)

  inputs.dir(jsClientWorkspace)
  inputs.property("browserify version", libs.versions.browserify.get())

  outputs.file("$jsClientBuildDir/index.js")
}

val finalizeJsBundleDir = tasks.register<Sync>("finalizeJsBundleDir") {
  from(generatedJSClient) {
    include("README.md")
    include("docs/**")
  }

  from(generateJsClientBundle) {
    include("index.js")
  }

  into(jsClientBundleDir)
}

artifacts {
  add("jsClientBundle", jsClientBundleDir) {
    builtBy(finalizeJsBundleDir)
  }
}
