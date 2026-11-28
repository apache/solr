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

package org.apache.solr.ui.views.files

import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.v2.runComposeUiTest
import kotlin.test.Test
import kotlin.test.assertEquals
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.cd_collapse_folder
import org.apache.solr.ui.shared.generated.resources.cd_expand_folder
import org.apache.solr.ui.shared.generated.resources.state_collapsed
import org.apache.solr.ui.shared.generated.resources.state_expanded
import org.jetbrains.compose.resources.getString

/**
 * Runs on the JVM only: the wasm test bundle does not package Compose string resources, so
 * localized accessibility text cannot be read there. The semantics themselves come from commonMain.
 */
class FileTreeAccessibilityTest {

    private val conf =
        FileTreeFolder(
            name = "conf",
            path = "conf",
            children = listOf(FileTreeFile(name = "solrconfig.xml", path = "conf/solrconfig.xml")),
        )

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testChevronExposesAccessibleFolderState() = runComposeUiTest {
        val expandDescription = getString(Res.string.cd_expand_folder, conf.name)
        val collapseDescription = getString(Res.string.cd_collapse_folder, conf.name)
        val collapsedState = getString(Res.string.state_collapsed)
        val expandedState = getString(Res.string.state_expanded)

        setContent {
            FileTree(
                nodes = listOf(conf),
                selectedPath = null,
                onItemClick = {},
                onItemDoubleClick = {},
            )
        }

        val chevron = onNodeWithTag("chevron_conf")
        var semantics = chevron.fetchSemanticsNode().config
        assertEquals(listOf(expandDescription), semantics[SemanticsProperties.ContentDescription])
        assertEquals(collapsedState, semantics[SemanticsProperties.StateDescription])
        assertEquals(Role.Button, semantics[SemanticsProperties.Role])

        chevron.performClick()

        semantics = chevron.fetchSemanticsNode().config
        assertEquals(listOf(collapseDescription), semantics[SemanticsProperties.ContentDescription])
        assertEquals(expandedState, semantics[SemanticsProperties.StateDescription])
        assertEquals(Role.Button, semantics[SemanticsProperties.Role])
    }
}
