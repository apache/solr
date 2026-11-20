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

package org.apache.solr.ui.preview.files

import androidx.compose.runtime.Composable
import androidx.compose.ui.tooling.preview.Preview
import org.apache.solr.ui.preview.PreviewContainer
import org.apache.solr.ui.views.files.FileTree
import org.apache.solr.ui.views.files.FileTreeFile
import org.apache.solr.ui.views.files.FileTreeFolder

@Preview
@Composable
private fun PreviewFileTreeEmpty() = PreviewContainer {
    FileTree(
        nodes = emptyList(),
        selectedPath = null,
        onItemClick = {},
        onItemDoubleClick = {},
    )
}

@Preview
@Composable
private fun PreviewFileTreeTypicalConfigset() = PreviewContainer {
    FileTree(
        nodes = listOf(
            FileTreeFolder(
                name = "conf",
                path = "conf",
                children = listOf(
                    FileTreeFile(name = "managed-schema.xml", path = "conf/managed-schema.xml"),
                    FileTreeFile(name = "solrconfig.xml", path = "conf/solrconfig.xml"),
                    FileTreeFolder(
                        name = "lang",
                        path = "conf/lang",
                        children = listOf(
                            FileTreeFile(name = "stopwords_en.txt", path = "conf/lang/stopwords_en.txt"),
                        ),
                    ),
                ),
            ),
            FileTreeFolder(
                name = "lib",
                path = "lib",
                children = listOf(
                    FileTreeFile(name = "analytics.jar", path = "lib/analytics.jar"),
                ),
            ),
            FileTreeFile(name = "README.txt", path = "README.txt"),
            FileTreeFile(name = "backup.zip", path = "backup.zip"),
        ),
        selectedPath = "conf/managed-schema.xml",
        onItemClick = {},
        onItemDoubleClick = {},
    )
}

@Preview
@Composable
private fun PreviewFileTreeLongNames() = PreviewContainer {
    FileTree(
        nodes = listOf(
            FileTreeFolder(
                name = "this-is-an-extremely-long-configset-folder-name-that-keeps-going-and-going",
                path = "long-folder",
                children = emptyList(),
            ),
            FileTreeFile(
                name = "managed-schema-with-a-very-long-descriptive-name-and-version-suffix.xml",
                path = "long-file",
            ),
        ),
        selectedPath = null,
        onItemClick = {},
        onItemDoubleClick = {},
    )
}
