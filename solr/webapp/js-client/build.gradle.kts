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
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

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

  // Keep the outputs of "npm install", so that they don't need to be regenerated
  preserve {
    include("node_modules/**")
    include("package-lock.json")
  }

  // The OpenAPI generator wrongly declares the @babel/cli build tool as a runtime
  // dependency; move it to devDependencies, so that the SBOM of the bundle
  // (generated with --omit dev) only lists what browserify actually bundles.
  doLast {
    val packageJson = File(jsClientWorkspace, "package.json")
    @Suppress("UNCHECKED_CAST")
    val json = JsonSlurper().parse(packageJson) as MutableMap<String, Any?>

    @Suppress("UNCHECKED_CAST")
    val dependencies = json["dependencies"] as? MutableMap<String, String>
    dependencies?.remove("@babel/cli")?.let { babelCliVersion ->
      @Suppress("UNCHECKED_CAST")
      val devDependencies =
        json.getOrPut("devDependencies") { mutableMapOf<String, String>() } as MutableMap<String, String>
      devDependencies["@babel/cli"] = babelCliVersion
    }
    packageJson.writeText(JsonOutput.prettyPrint(JsonOutput.toJson(json)))
  }
}

val jsClientDownloadDeps = tasks.register<NpmTask>("jsClientDownloadDeps") {
  dependsOn(syncJSClientSourceCode)

  args.set(listOf("install"))
  workingDir.set(jsClientWorkspace)

  inputs.dir("$jsClientWorkspace/src")
  inputs.file("$jsClientWorkspace/package.json")
  outputs.dir("$jsClientWorkspace/node_modules")
  outputs.file("$jsClientWorkspace/package-lock.json")
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

// CycloneDX SBOM of the bundle, merged into the distribution SBOMs by :solr:packaging

val jsClientSbomFile = layout.buildDirectory.file("cyclonedx/bom-js-client.json").get().asFile

val downloadCyclonedxNpm = tasks.register<NpmTask>("downloadCyclonedxNpm") {
  args.set(listOf("install", "@cyclonedx/cyclonedx-npm@${libs.versions.cyclonedx.npm.get()}"))

  inputs.property("cyclonedx-npm version", libs.versions.cyclonedx.npm.get())
  outputs.dir(project.extra["nodeProjectDir"].toString() + "/node_modules/@cyclonedx/cyclonedx-npm")
}

val generateJsClientSbom = tasks.register<NpxTask>("generateJsClientSbom") {
  dependsOn(downloadCyclonedxNpm)
  // Needs the package-lock.json and node_modules produced by the install
  dependsOn(jsClientDownloadDeps)

  // The full package spec, since the bare "cyclonedx-npm" command name resolves to an
  // unrelated npm package. Runs from the node project dir, where downloadCyclonedxNpm
  // installed the pinned version, and points at the workspace manifest instead.
  command.set("@cyclonedx/cyclonedx-npm@${libs.versions.cyclonedx.npm.get()}")
  args.set(
    listOf(
      // Only the packages bundled into the shipped file, not the build tooling
      "--omit", "dev",
      // Match the spec version emitted by the CycloneDX Gradle plugin in :solr:packaging
      "--spec-version", "1.6",
      "--output-reproducible",
      "--output-format", "JSON",
      "--output-file", jsClientSbomFile.absolutePath,
      "$jsClientWorkspace/package.json",
    ),
  )
  workingDir.set(File(project.extra["nodeProjectDir"].toString()))

  inputs.file("$jsClientWorkspace/package.json")
  inputs.file("$jsClientWorkspace/package-lock.json")
  inputs.property("cyclonedx-npm version", libs.versions.cyclonedx.npm.get())
  outputs.file(jsClientSbomFile)
}

val jsClientSbom = configurations.create("jsClientSbom") {
  isCanBeConsumed = true
  isCanBeResolved = false
}

artifacts {
  add("jsClientSbom", jsClientSbomFile) {
    builtBy(generateJsClientSbom)
  }
}
