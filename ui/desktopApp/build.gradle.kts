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

import org.jetbrains.compose.desktop.application.dsl.TargetFormat

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.compose.multiplatform)
    alias(libs.plugins.compose.compiler)
}

dependencies {
    implementation(projects.shared)

    implementation(compose.desktop.currentOs)
    implementation(libs.kotlinx.coroutines.swing)

    implementation(libs.compose.uiToolingPreview)
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
