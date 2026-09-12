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

// This adds jacoco code coverage to test tasks, along with an aggregated
// coverage report for all projects.
//
// Coverage is opt-in and this script is only applied (from build.gradle, to
// the root project) when the 'coverage' task is explicitly requested or
// -Ptests.coverage=true is passed; see settings.gradle.

// Set up an aggregated coverage report over all projects with tests.
// Logs a coverage summary to the console after report tasks.
plugins.apply("org.barfuin.gradle.jacocolog")
plugins.apply("jacoco-report-aggregation")

configure<ReportingExtension> {
  reports.create<JacocoCoverageReport>("testCodeCoverageReport") {
    testSuiteName.set("test")
  }
}

subprojects.filter { it.file("src/test").exists() }.forEach { subproject ->
  dependencies.add("jacocoAggregation", subproject)
}

val aggregatedReport = tasks.named<JacocoReport>("testCodeCoverageReport") {
  // XML report is consumed by CI tooling.
  reports.xml.required.set(true)
  doLast {
    logger.lifecycle("Aggregated code coverage report at: ${reports.html.entryPoint}\n")
  }
}

tasks.register("coverage") {
  dependsOn(aggregatedReport)
}

allprojects {
  plugins.withType<JavaPlugin> {
    // Applies the jacoco plugin and logs a coverage summary to the console.
    plugins.apply("org.barfuin.gradle.jacocolog")

    val jacocoTestReport = tasks.named<JacocoReport>("jacocoTestReport") {
      // XML report is consumed by CI tooling.
      reports.xml.required.set(true)
      doLast {
        logger.lifecycle("Code coverage report at: ${reports.html.entryPoint}\n")
      }
    }

    val testTasks = tasks.withType<Test>()

    tasks.register("coverage") {
      dependsOn(testTasks)
      dependsOn(jacocoTestReport)
    }

    testTasks.configureEach {
      // Configure the jacoco data file to be within the test task's
      // working directory.
      extensions.configure(JacocoTaskExtension::class) {
        setDestinationFile(providers.provider { workingDir.resolve("jacoco.exec") })
      }

      // Test reports run after the test task, if it's run at all.
      finalizedBy(jacocoTestReport)
    }
  }
}
