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

import org.cyclonedx.gradle.CyclonedxDirectTask
import org.cyclonedx.model.Component
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.gradle.api.tasks.bundling.Zip
import java.security.MessageDigest
import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.targets.js.webpack.KotlinWebpackConfig

plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.compose.multiplatform)
    alias(libs.plugins.compose.compiler)
    alias(libs.plugins.kotlin.serialization)
    alias(rootLibs.plugins.cyclonedx)
}

kotlin {
//    js {
//        browser()
//        binaries.executable()
//    }

    @OptIn(ExperimentalWasmDsl::class)
    wasmJs {
        browser {
            commonWebpackConfig {
                outputFileName = "webApp.js"
                devServer = (devServer ?: KotlinWebpackConfig.DevServer()).apply {
                    // Serve sources to debug inside browser
                    static(directory = project.projectDir.path)
                }
                // Note that webpack.config.d/ contains additional configuration
            }
            testTask {
                // Explicitly disable the wasmJs browser tests, as we do not have the suitable
                // environments right now (running only tests for JVM)
                enabled = false
            }
        }
        binaries.executable()
    }

    sourceSets {
        commonMain.dependencies {
            implementation(projects.shared)
            implementation(libs.kotlinx.serialization.json)

            implementation(libs.compose.ui)

            implementation(project.dependencies.platform(libs.ktor.bom))
            implementation(libs.ktor.client.core)
            implementation(libs.compose.foundation)
            implementation(libs.compose.material3)
        }
    }
}

// CycloneDX SBOM of the Maven dependencies compiled into the wasmJs bundle, shipped as
// "bom.json" in the root of the zip and merged into the distribution SBOMs by :solr:packaging.
val cyclonedxUi = tasks.register<CyclonedxDirectTask>("cyclonedxUi") {
    group = "Bill of Materials"
    description = "Generates a CycloneDX BOM of the dependencies compiled into the wasmJs UI bundle"

    includeConfigs.set(listOf("wasmJsRuntimeClasspath"))
    projectType.set(Component.Type.LIBRARY)

    // The plugin resolves the configuration leniently and without depending on it,
    // so the artifacts must be present first or their hashes are missing.
    inputs.files(configurations.named("wasmJsRuntimeClasspath"))
        .withPropertyName("wasmJsRuntimeArtifacts")
        .withNormalizer(ClasspathNormalizer::class)

    jsonOutput.set(layout.buildDirectory.file("cyclonedx/bom.json"))

    // The npm packages webpack bundles next to the Kotlin code are not on the classpath
    val yarnLock = rootProject.file("kotlin-js-store/wasm/yarn.lock")
    dependsOn(":kotlinWasmStoreYarnLock")
    inputs.file(yarnLock)
        .withPropertyName("yarnLock")
        .withPathSensitivity(PathSensitivity.NONE)

    doLast {
        val bomFile = jsonOutput.get().asFile
        @Suppress("UNCHECKED_CAST")
        val bom = JsonSlurper().parse(bomFile) as MutableMap<String, Any?>
        @Suppress("UNCHECKED_CAST")
        val components = bom["components"] as MutableList<Any?>
        @Suppress("UNCHECKED_CAST")
        val dependencies = bom["dependencies"] as MutableList<MutableMap<String, Any?>>
        @Suppress("UNCHECKED_CAST")
        val rootRef = (bom["metadata"] as Map<String, Map<String, Any?>>)["component"]!!["bom-ref"]

        val version = Regex("""(?m)^"@js-joda/core@[^"]+":\s+version "([^"]+)"""")
            .find(yarnLock.readText())?.groupValues?.get(1)
            ?: throw GradleException("@js-joda/core is missing from $yarnLock")
        val purl = "pkg:npm/%40js-joda/core@$version"
        components.add(mapOf(
            "type" to "library",
            "bom-ref" to purl,
            "name" to "@js-joda/core",
            "version" to version,
            "licenses" to listOf(mapOf("license" to mapOf("id" to "BSD-3-Clause"))),
            "purl" to purl,
        ))
        dependencies.add(mutableMapOf("ref" to purl, "dependsOn" to emptyList<String>()))
        @Suppress("UNCHECKED_CAST")
        val rootEntry = dependencies.first { it["ref"] == rootRef }
        @Suppress("UNCHECKED_CAST")
        rootEntry["dependsOn"] = (rootEntry["dependsOn"] as List<String>? ?: emptyList()) + purl

        bomFile.writeText(JsonOutput.prettyPrint(JsonOutput.toJson(bom)))
    }
}

// package as a plain zip and upload to nightlies by CI
val wasmJsBrowserDistribution = tasks.named("wasmJsBrowserDistribution")

val zipWasmDist = tasks.register<Zip>("zipWasmDist") {
  dependsOn(wasmJsBrowserDistribution)
  from(wasmJsBrowserDistribution.map { it.outputs.files.singleFile })
  from(cyclonedxUi.flatMap { it.jsonOutput })
  archiveFileName.set("solr-ui-wasm.zip")
  destinationDirectory.set(layout.buildDirectory.dir("dist"))
  finalizedBy("checksumWasmDist")
}

tasks.register("checksumWasmDist") {
  dependsOn(zipWasmDist)

  val archiveFile = zipWasmDist.flatMap { it.archiveFile }
  val checksumFile = archiveFile.map { it.asFile.parentFile.resolve("${it.asFile.name}.sha256") }

  inputs.file(archiveFile)
  outputs.file(checksumFile)

  doLast {
    val file = archiveFile.get().asFile
    val checksum = MessageDigest.getInstance("SHA-256")
      .digest(file.readBytes())
      .joinToString("") { b -> "%02x".format(b) }
    checksumFile.get().writeText(checksum)
  }
}
