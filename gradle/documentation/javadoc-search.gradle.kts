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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import javax.inject.Inject

// Javadoc renders each module as a self-contained tree whose search only covers
// that module. This merges every tree's search index into one published at the
// documentation root, so javadocs.html searches the whole API and so does the
// search box on each individual page.

abstract class UnifiedJavadocSearchTask : DefaultTask() {

  private data class Index(val file: String, val variable: String, val category: String)

  companion object {
    /** The five index files javadoc generates, and the search category of each. */
    private val INDEXES =
      listOf(
        Index("module-search-index.js", "moduleSearchIndex", "modules"),
        Index("package-search-index.js", "packageSearchIndex", "packages"),
        Index("type-search-index.js", "typeSearchIndex", "types"),
        Index("member-search-index.js", "memberSearchIndex", "members"),
        Index("tag-search-index.js", "tagSearchIndex", "searchTags"),
      )

    /**
     * Javadoc's search engine, copied beside the merged index. Its own stylesheet is deliberately
     * left behind: javadocs.html is a documentation site page styled by solr-docs.css, not a
     * javadoc page.
     */
    private val ASSETS = listOf("script.js", "search.js", "search-page.js", "link.svg")
  }

  @get:Internal abstract val docroot: DirectoryProperty

  @get:Inject abstract val fs: FileSystemOperations

  /** Mirror of getURL() in javadoc's search.js, minus the module prefix. */
  private fun entryUrl(item: Map<String, Any?>, category: String): String {
    fun field(name: String): String? = (item[name] as String?)?.takeIf { it.isNotEmpty() }
    val pkg = field("p")?.let { it.replace('.', '/') + "/" } ?: ""
    return when (category) {
      "modules" -> field("l") + "/module-summary.html"
      "packages" -> field("u") ?: (field("l")!!.replace('.', '/') + "/package-summary.html")
      "types" -> field("u") ?: (pkg + field("l") + ".html")
      "members" -> pkg + field("c") + ".html#" + (field("u") ?: field("l"))
      "searchTags" -> field("u")!!
      else -> throw GradleException("unknown search category $category")
    }
  }

  @Suppress("UNCHECKED_CAST")
  private fun readIndex(file: File): List<MutableMap<String, Any?>> {
    val match = Regex("(?s)=\\s*(\\[.*\\])\\s*;").find(file.readText(Charsets.UTF_8)) ?: return emptyList()
    return JsonSlurper().parseText(match.groupValues[1]) as List<MutableMap<String, Any?>>
  }

  @TaskAction
  fun mergeSearchIndexes() {
    val root = docroot.get().asFile
    val trees =
      docroot
        .asFileTree
        .matching { include("**/type-search-index.js") }
        .files
        .map { it.parentFile }
        .filter { it != root }
        .sortedBy { it.path }
    if (trees.isEmpty()) {
      throw GradleException("No javadoc trees found under $root")
    }

    val merged = INDEXES.associate { it.variable to mutableListOf<Map<String, Any?>>() }
    for (tree in trees) {
      val prefix = root.toPath().relativize(tree.toPath()).toString().replace(File.separator, "/") + "/"
      for (index in INDEXES) {
        val file = File(tree, index.file)
        if (!file.exists()) continue
        for (item in readIndex(file)) {
          // javadoc synthesises these per tree; merged they would be one
          // identical-looking row per module.
          if (item["l"] in listOf("All Packages", "All Classes and Interfaces")) continue
          // A precomputed url short-circuits getURL() in javadoc's search.js,
          // which is what lets an unmodified search engine resolve a hit in
          // another module.
          item["url"] = prefix + entryUrl(item, index.category)
          if (index.category == "packages") {
            // The only category whose result label shows item.m.
            item["m"] = prefix.dropLast(1)
          }
          merged.getValue(index.variable).add(item)
        }
      }
    }

    for (index in INDEXES) {
      val rows = merged.getValue(index.variable)
      // The trailing call is how javadoc's search page learns an index
      // finished loading; it stays on "Loading..." without one.
      File(root, index.file)
        .writeText("${index.variable} = ${JsonOutput.toJson(rows)};updateSearchResults();\n", Charsets.UTF_8)
      logger.lifecycle("${index.file}: ${rows.size} entries")
    }

    val source = trees.first()
    for (asset in ASSETS) {
      val file = File(source, asset)
      if (file.exists()) {
        fs.copy {
          from(file)
          into(root)
        }
      }
    }
    fs.copy {
      from(File(source, "script-dir"))
      into(File(root, "script-dir"))
    }

    // The copied script.js carries the bar config of whichever tree it came
    // from; at the documentation root there is nothing to climb and no module
    // to preselect.
    val rootScript = File(root, "script.js")
    rootScript.writeText(
      rootScript
        .readText(Charsets.UTF_8)
        .replaceFirst(Regex("var solrDocsRootUp = \"[^\"]*\";"), "var solrDocsRootUp = \"\";")
        .replaceFirst(Regex("var solrDocsModule = \"[^\"]*\";"), "var solrDocsModule = \"\";"),
      Charsets.UTF_8,
    )

    logger.lifecycle("Merged ${trees.size} javadoc trees into a site-wide search index")
  }
}

project(":solr:documentation") {
  val siteDir = extra["docroot"] as File
  val javadocTasks = parent!!.subprojects.map { it.tasks.matching { t -> t.name == "renderSiteJavadoc" } }

  tasks.register<UnifiedJavadocSearchTask>("unifiedJavadocSearch") {
    dependsOn(javadocTasks)
    docroot.set(siteDir)

    // Writes into another task's output directory, so there is no input/output
    // pair Gradle can fingerprint. It only adds files at the documentation root
    // and never touches the javadoc trees, which must stay byte-identical or
    // every renderSiteJavadoc task would be dirty on the next build.
    outputs.upToDateWhen { false }
  }
}
