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
import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.targets.js.webpack.KotlinWebpackConfig

repositories {
    google {
        mavenContent {
            includeGroupAndSubgroups("androidx")
            includeGroupAndSubgroups("com.android")
            includeGroupAndSubgroups("com.google")
        }
    }
    mavenCentral()
}

plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.jetbrains.compose)
    alias(libs.plugins.compose.compiler)
}

kotlin {

    // Add targets to support
    @OptIn(ExperimentalWasmDsl::class)
    wasmJs {
        outputModuleName = provider { "composeApp" }
        browser {
            commonWebpackConfig {
                outputFileName = "composeApp.js"
                devServer = (devServer ?: KotlinWebpackConfig.DevServer()).apply {
                    static = (static ?: mutableListOf()).apply {
                        // Serve sources to debug inside browser
                        add(project.projectDir.path)
                    }
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

    jvm("desktop")

    sourceSets {
        // Shared multiplatform dependencies
        val commonMain by getting {
            dependencies {
                implementation(project.dependencies.platform(project(":platform")))
                implementation(compose.runtime)
                implementation(compose.foundation)
                implementation(compose.material3)
                implementation(compose.ui)
                implementation(compose.components.resources)
                implementation(compose.components.uiToolingPreview)
                implementation(compose.materialIconsExtended)

                implementation(libs.kotlinx.serialization.core)
                implementation(libs.kotlinx.serialization.json)
                implementation(libs.kotlinx.coroutines.core)
                implementation(libs.kotlinx.datetime)

                implementation(libs.decompose.decompose)
                implementation(libs.essenty.lifecycle)
                implementation(libs.decompose.extensions.compose)
                implementation(libs.mvikotlin.extensions.coroutines)
                implementation(libs.mvikotlin.mvikotlin)
                implementation(libs.mvikotlin.main)
                implementation(libs.mvikotlin.logging)

                implementation(libs.ktor.client.auth)
                implementation(libs.ktor.client.core)
                implementation(libs.ktor.client.cio)
                implementation(libs.ktor.client.contentNegotiation)
                implementation(libs.ktor.client.serialization.json)

                implementation(libs.oshai.logging)
                implementation(libs.slf4j.api)
            }
        }

        val commonTest by getting {
            dependencies {
                implementation(kotlin("test"))
                implementation(libs.kotlinx.coroutines.test)
                @OptIn(org.jetbrains.compose.ExperimentalComposeLibrary::class)
                implementation(compose.uiTest)
                implementation(libs.ktor.client.mock)
            }
        }

        val desktopMain by getting {
            dependencies {
                implementation(compose.desktop.currentOs)
                implementation(libs.kotlinx.coroutines.swing)
            }
        }
    }
}

configurations {
    all {
        // Exclude old material dependencies
        exclude(group = "org.jetbrains.compose.material", module = "material")
    }
}

compose.desktop {
    application {
        mainClass = "org.apache.solr.ui.MainKt"

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

tasks.matching { task ->
    task.name in listOf(
        "allTests",
        "desktopTest",
        "wasmJsTest",
        "wasmJsBrowserTest",
    )
}.configureEach {
    // These kotlin multiplatform test tasks are not excluded on "gradlew ... -x test",
    // so we disable them explicitly
    onlyIf { !gradle.startParameter.excludedTaskNames.contains("test") }

    // Note that "gradlew check -x test --dry-run" does not correctly resolve this exclusion rule,
    // and you will see the test tasks being listed there as well, but they will be skipped as
    // expected
}
