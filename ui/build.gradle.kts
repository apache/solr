import com.diffplug.gradle.spotless.SpotlessExtension

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
  alias(libs.plugins.kotlin.multiplatform) apply false
  alias(libs.plugins.kotlin.jvm) apply false
  alias(libs.plugins.compose.multiplatform) apply false
  alias(libs.plugins.compose.compiler) apply false
  alias(libs.plugins.ktor) apply false
  alias(rootLibs.plugins.diffplug.spotless) apply false
}

group = "org.apache.solr.ui"
version = "0.1.0"

subprojects {
  repositories {
    google {
      mavenContent {
        includeGroupAndSubgroups("androidx")
        includeGroupAndSubgroups("com.android")
        includeGroupAndSubgroups("com.google")
      }
    }
  }
  // mavenCentral / enterprise mirror, shared with the rest of the build.
  apply(from = rootProject.file("../build-tools/build-infra/declare-repositories.gradle"))

  // Configure spotless for kotlin sources
  plugins.apply(rootProject.rootLibs.plugins.diffplug.spotless.get().pluginId)

  project.extensions.getByType<SpotlessExtension>().apply {
    kotlin {
      // Apply to all Kotlin and Kotlin DSL files
      target("**/*.kt", "**/*.kts")

      // TODO Enable ktlint in the UI module
//      ktlint(rootProject.rootLibs.versions.ktlint.get())
//        .setEditorConfigPath(rootProject.file("../.editorconfig"))
//        .customRuleSets(listOf(libs.nlopez.compose.ktlintrules.get().toString()))
    }
  }
}
