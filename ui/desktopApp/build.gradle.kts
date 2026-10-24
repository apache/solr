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

import java.security.MessageDigest
import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.api.artifacts.result.ResolvedArtifactResult
import org.jetbrains.compose.desktop.application.dsl.TargetFormat

plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.compose.multiplatform)
  alias(libs.plugins.compose.compiler)
}

val linuxX64RuntimeClasspath by configurations.creating {
  isCanBeConsumed = false
  isCanBeResolved = true
  extendsFrom(configurations.runtimeClasspath.get())
  for (os in listOf("linux-arm64", "macos-x64", "macos-arm64", "windows-x64")) {
    exclude(group = "org.jetbrains.compose.desktop", module = "desktop-jvm-$os")
    exclude(group = "org.jetbrains.skiko", module = "skiko-awt-runtime-$os")
  }
}

dependencies {
  implementation(projects.shared)

  implementation(compose.desktop.currentOs)
  implementation(libs.kotlinx.coroutines.swing)
  implementation(libs.compose.material3)

  implementation(libs.compose.uiToolingPreview)

  linuxX64RuntimeClasspath(compose.desktop.linux_x64)
}

val licensesDir = rootProject.file("licenses")

fun sha1Hex(file: File): String =
  MessageDigest.getInstance("SHA-1").digest(file.readBytes()).joinToString("") { "%02x".format(it) }

fun externalJars(): List<ResolvedArtifactResult> =
  linuxX64RuntimeClasspath.incoming.artifacts.artifacts.filter { it.id.componentIdentifier is ModuleComponentIdentifier }

fun checksumFileName(artifact: ResolvedArtifactResult): String {
  val id = artifact.id.componentIdentifier as ModuleComponentIdentifier
  return "${id.group}-${artifact.file.name}.sha1"
}

val updateLicenses by tasks.registering {
  group = "Dependency validation"
  description = "Writes SHA-1 checksum files for the linux_x64 desktop runtime jars."
  inputs.files(linuxX64RuntimeClasspath)
  doLast {
    licensesDir.mkdirs()
    externalJars().forEach { artifact ->
      licensesDir.resolve(checksumFileName(artifact)).writeText(sha1Hex(artifact.file) + "\n")
    }
  }
}

val validateLicenses by tasks.registering {
  group = "Dependency validation"
  description = "Checks the linux_x64 desktop runtime jars against their SHA-1 checksum files."
  inputs.files(linuxX64RuntimeClasspath)
  inputs.files(project.fileTree(licensesDir))
  doLast {
    val errors = mutableListOf<String>()
    externalJars().forEach { artifact ->
      val checksumFile = licensesDir.resolve(checksumFileName(artifact))
      when {
        !checksumFile.exists() -> errors += "missing checksum file ${checksumFile.name}"
        checksumFile.readText().trim() != sha1Hex(artifact.file) -> errors += "checksum mismatch for ${artifact.file.name}"
      }
    }
    val known = externalJars().map { checksumFileName(it) }.toSet()
    licensesDir.listFiles { f -> f.name.endsWith(".sha1") && f.name !in known }?.forEach {
      errors += "stale checksum file ${it.name}"
    }
    if (errors.isNotEmpty()) {
      throw GradleException("Dependency checksum validation failed:\n  - " + errors.joinToString("\n  - ") +
          "\n\nRegenerate with: ./gradlew -p ui :desktopApp:updateLicenses")
    }
  }
}

tasks.named("check") {
  dependsOn(validateLicenses)
}

compose.desktop {
  application {
    mainClass = "org.apache.solr.ui.MainKt"

    buildTypes.release.proguard {
      version.set("7.6.0")
      configurationFiles.from("proguard.pro")
    }

    nativeDistributions {
      targetFormats(TargetFormat.Dmg, TargetFormat.Msi, TargetFormat.Deb)

      packageVersion = "1.0.0"

      windows {
        packageName = "Apache Solr Desktop"
        // App icon (needs to be .ico)
        iconFile.set(project.file("assets/logo.ico"))
        // Directory name (if not per user "C:\Program Files\[installationPath]")
        installationPath = "Apache Solr Desktop"
        // Create desktop shortcut
        shortcut = true
      }

      linux {
        packageName = "solr-desktop"
        iconFile.set(project.file("assets/logo.png"))
      }

      macOS {
        packageName = "ApacheSolrDesktop"
        iconFile.set(project.file("assets/logo.png"))
      }
    }
  }
}
