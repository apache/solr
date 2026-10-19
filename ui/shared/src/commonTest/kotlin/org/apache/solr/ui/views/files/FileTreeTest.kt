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
 */package org.apache.solr.ui.views.files

import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.doubleClick
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performTouchInput
import androidx.compose.ui.test.v2.runComposeUiTest
import kotlin.test.Test
import kotlin.test.assertEquals

class FileTreeTest {

    private val solrconfig = FileTreeFile("solrconfig.xml", "conf/solrconfig.xml")
    private val conf = FileTreeFolder("conf", "conf", listOf(solrconfig))
    private val readme = FileTreeFile("README.md", "README.md")
    private val nodes = listOf(conf, readme)

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testHidesChildrenUntilFolderIsExpanded() = runComposeUiTest {
        setContent { FileTree(nodes, selectedPath = null, onItemClick = {}, onItemDoubleClick = {}) }

        onNodeWithText("conf").assertExists()
        onNodeWithText("README.md").assertExists()
        onNodeWithText("solrconfig.xml").assertDoesNotExist()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testChevronExpandsAndCollapsesFolder() = runComposeUiTest {
        setContent { FileTree(nodes, selectedPath = null, onItemClick = {}, onItemDoubleClick = {}) }

        onNodeWithTag("chevron_conf").performClick()
        onNodeWithText("solrconfig.xml").assertExists()

        onNodeWithTag("chevron_conf").performClick()
        onNodeWithText("solrconfig.xml").assertDoesNotExist()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testDoubleClickOnFolderNameExpandsFolder() = runComposeUiTest {
        setContent { FileTree(nodes, selectedPath = null, onItemClick = {}, onItemDoubleClick = {}) }

        onNodeWithText("conf").performTouchInput { doubleClick() }
        onNodeWithText("solrconfig.xml").assertExists()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testSingleClickReportsItem() = runComposeUiTest {
        val clicked = mutableListOf<FileTreeNode>()
        setContent {
            FileTree(nodes, selectedPath = null, onItemClick = { clicked += it }, onItemDoubleClick = {})
        }

        onNodeWithText("README.md").performClick()
        waitUntil(timeoutMillis = 2_000) { clicked.isNotEmpty() }
        assertEquals(listOf<FileTreeNode>(readme), clicked)
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testDoubleClickReportsItem() = runComposeUiTest {
        val doubleClicked = mutableListOf<FileTreeNode>()
        setContent {
            FileTree(nodes, selectedPath = null, onItemClick = {}, onItemDoubleClick = { doubleClicked += it })
        }

        onNodeWithText("README.md").performTouchInput { doubleClick() }
        assertEquals(listOf<FileTreeNode>(readme), doubleClicked)
    }
}
