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

import com.autonomousapps.DependencyAnalysisExtension

// See dev-docs/gradle-help/dependencies.txt

// Opt-in only: not wired into "check", since the advice needs human judgment
tasks.register("analyzeDependencies") {
  group = "verification"
  description =
    "Reports dependency-analysis (DAGP) advice: unused/undeclared/misconfigured dependencies."
  dependsOn(tasks.named("buildHealth")) // non-obvious name
}

configure<DependencyAnalysisExtension> {
  issues {
    project(":solr:ui") {
      onUnusedDependencies {
        severity("ignore")
      }
      onUsedTransitiveDependencies {
        severity("ignore")
      }
      onIncorrectConfiguration {
        severity("ignore")
      }
    }

    project(":solr:solrj-jetty") {
      onUnusedDependencies {
        exclude(":solr:solrj")
      }
    }

    project(":solr:modules:jwt-auth") {
      onDuplicateClassWarnings {
        severity("ignore")
      }
    }

    all {
      // Report advice as warnings rather than failing the build: several categories
      // (api-vs-implementation promotions, some test-scope changes) need case-by-case
      // human judgment.
      onAny {
        severity("warn")
      }
      onUnusedDependencies {
        exclude(
          "org.jspecify:jspecify",
          "com.google.code.findbugs:jsr305",
          "com.google.errorprone:error_prone_annotations",
        )
      }
    }
  }
}
