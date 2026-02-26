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

package org.apache.solr.ui.views.configsets

import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assertIsNotEnabled
import androidx.compose.ui.test.isDisplayed
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.runComposeUiTest
import kotlin.test.Test
import org.apache.solr.ui.domain.Configset

@OptIn(ExperimentalTestApi::class)
class ConfigsetsDropdownTest {

    @Test
    fun `GIVEN empty availableConfigsets WHEN dropdown clicked THEN not expanded`() = runComposeUiTest {
        setContent {
            ConfigsetsDropdown(
                selectConfigset = {},
                availableConfigsets = emptyList(),
                selectedConfigSet = "",
            )
        }

        // Field click shouldn’t open the menu (anchor is disabled in our impl)
        onNodeWithTag(testTag = "configsets_dropdown").performClick()
        onNodeWithTag(testTag = "configsets_exposed_dropdown_menu").assertDoesNotExist()
    }

    @Test
    fun `GIVEN empty availableConfigsets THEN dropdown disabled`() = runComposeUiTest {
        setContent {
            ConfigsetsDropdown(
                selectConfigset = {},
                availableConfigsets = emptyList(),
                selectedConfigSet = "",
            )
        }

        // Field click shouldn’t open the menu (anchor is disabled in our impl)
        onNodeWithTag(testTag = "configsets_dropdown").assertIsNotEnabled()
    }

    @Test
    fun `GIVEN configsets WHEN clicking dropdown THEN dropdown expands`() = runComposeUiTest {
        val selectedConfigset = "gettingstarted"

        setContent {
            ConfigsetsDropdown(
                selectConfigset = {},
                availableConfigsets = listOf(selectedConfigset, "techproducts")
                    .map { Configset(it) },
                selectedConfigSet = selectedConfigset,
            )
        }

        onNodeWithTag(testTag = "configsets_dropdown").performClick()
        onNodeWithTag(testTag = "configsets_exposed_dropdown_menu").isDisplayed()
    }
}
