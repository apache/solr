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

import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.targets.js.webpack.KotlinWebpackConfig

plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.compose.multiplatform)
    alias(libs.plugins.compose.compiler)
    alias(libs.plugins.kotlin.serialization)
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
        }
    }
}
