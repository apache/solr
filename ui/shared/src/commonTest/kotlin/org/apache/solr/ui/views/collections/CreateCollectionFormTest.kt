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
package org.apache.solr.ui.views.collections

import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assertIsEnabled
import androidx.compose.ui.test.assertIsNotEnabled
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performTextClearance
import androidx.compose.ui.test.performTextInput
import androidx.compose.ui.test.v2.runComposeUiTest
import org.apache.solr.ui.domain.Configset
import kotlin.test.Test
import kotlin.test.assertEquals

class CreateCollectionFormTest {

    private val configsets = listOf(Configset("techproducts"), Configset("films"))

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testShowsFieldsAndButtons() = runComposeUiTest {
        setContent {
            CreateCollectionForm(
                configsets = configsets,
                nameError = null,
                generalError = null,
                onCreate = {},
                onCancel = {},
            )
        }

        onNodeWithTag("create_collection_name_field").assertExists()
        onNodeWithTag("create_collection_shards_field").assertExists()
        onNodeWithTag("create_collection_nrt_field").assertExists()
        onNodeWithTag("create_collection_tlog_field").assertExists()
        onNodeWithTag("create_collection_pull_field").assertExists()
        onNodeWithTag("create_collection_placement_field").assertExists()
        onNodeWithTag("create_collection_button").assertExists()
        onNodeWithText("Cancel").assertExists()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testAdvancedOptionsAreHiddenUntilOpened() = runComposeUiTest {
        setContent {
            CreateCollectionForm(
                configsets = configsets,
                nameError = null,
                generalError = null,
                onCreate = {},
                onCancel = {},
            )
        }

        onNodeWithTag("create_collection_route_field").assertDoesNotExist()
        onNodeWithTag("create_collection_advanced_toggle").performClick()
        onNodeWithTag("create_collection_route_field").assertExists()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testShowsInlineErrorWhenNameExists() = runComposeUiTest {
        setContent {
            CreateCollectionForm(
                configsets = configsets,
                nameError = "A collection with this name already exists",
                generalError = null,
                onCreate = {},
                onCancel = {},
            )
        }

        onNodeWithText("A collection with this name already exists").assertExists()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testCreateIsDisabledWithoutAName() = runComposeUiTest {
        setContent {
            CreateCollectionForm(
                configsets = configsets,
                nameError = null,
                generalError = null,
                onCreate = {},
                onCancel = {},
            )
        }

        onNodeWithTag("create_collection_button").assertIsNotEnabled()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testDefaultsAndShardStepperFeedCreate() = runComposeUiTest {
        val created = mutableListOf<CreateCollectionInput>()
        setContent {
            CreateCollectionForm(
                configsets = configsets,
                nameError = null,
                generalError = null,
                onCreate = { created.add(it) },
                onCancel = {},
            )
        }

        onNodeWithTag("create_collection_name_field").performTextInput("films2")
        onNodeWithTag("create_collection_shards_increase").performClick()
        onNodeWithTag("create_collection_button").performClick()

        assertEquals(
            listOf(
                CreateCollectionInput(
                    name = "films2",
                    configset = "techproducts",
                    shards = 3,
                    nrtReplicas = 2,
                    tlogReplicas = 0,
                    pullReplicas = 1,
                    router = "compositeId",
                    routeField = null,
                )
            ),
            created,
        )
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testCreatePassesTheTypedValuesAndAdvancedRouter() = runComposeUiTest {
        val created = mutableListOf<CreateCollectionInput>()
        setContent {
            CreateCollectionForm(
                configsets = configsets,
                nameError = null,
                generalError = null,
                onCreate = { created.add(it) },
                onCancel = {},
            )
        }

        onNodeWithTag("create_collection_name_field").performTextInput("films2")
        onNodeWithTag("create_collection_shards_field").performTextClearance()
        onNodeWithTag("create_collection_shards_field").performTextInput("2")
        onNodeWithTag("create_collection_nrt_field").performTextClearance()
        onNodeWithTag("create_collection_nrt_field").performTextInput("3")
        onNodeWithTag("create_collection_tlog_field").performTextClearance()
        onNodeWithTag("create_collection_tlog_field").performTextInput("1")
        onNodeWithTag("create_collection_pull_field").performTextClearance()
        onNodeWithTag("create_collection_pull_field").performTextInput("0")
        onNodeWithTag("create_collection_advanced_toggle").performClick()
        onNodeWithText("Composite ID").performClick()
        onNodeWithText("Implicit").performClick()
        onNodeWithTag("create_collection_route_field").performTextInput("id")
        onNodeWithTag("create_collection_button").performClick()

        assertEquals(
            listOf(
                CreateCollectionInput(
                    name = "films2",
                    configset = "techproducts",
                    shards = 2,
                    nrtReplicas = 3,
                    tlogReplicas = 1,
                    pullReplicas = 0,
                    router = "implicit",
                    routeField = "id",
                )
            ),
            created,
        )
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testRejectsNamesTheServerRejects() = runComposeUiTest {
        setContent {
            CreateCollectionForm(
                configsets = configsets,
                nameError = null,
                generalError = null,
                onCreate = {},
                onCancel = {},
            )
        }

        onNodeWithTag("create_collection_name_field").performTextInput("bad name")
        onNodeWithTag("create_collection_button").assertIsNotEnabled()
        onNodeWithTag("create_collection_name_field").performTextClearance()
        onNodeWithTag("create_collection_name_field").performTextInput("-films")
        onNodeWithTag("create_collection_button").assertIsNotEnabled()
        onNodeWithTag("create_collection_name_field").performTextClearance()
        onNodeWithTag("create_collection_name_field").performTextInput("films.v2")
        onNodeWithTag("create_collection_button").assertIsEnabled()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testRejectsPullOnlyCollection() = runComposeUiTest {
        setContent {
            CreateCollectionForm(
                configsets = configsets,
                nameError = null,
                generalError = null,
                onCreate = {},
                onCancel = {},
            )
        }

        onNodeWithTag("create_collection_name_field").performTextInput("films2")
        onNodeWithTag("create_collection_nrt_field").performTextClearance()
        onNodeWithTag("create_collection_nrt_field").performTextInput("0")
        onNodeWithTag("create_collection_tlog_field").performTextClearance()
        onNodeWithTag("create_collection_tlog_field").performTextInput("0")
        onNodeWithTag("create_collection_button").assertIsNotEnabled()
    }
}
