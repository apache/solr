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

package org.apache.solr.ui.views.indexquery

import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assertIsEnabled
import androidx.compose.ui.test.assertIsNotEnabled
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.v2.runComposeUiTest
import org.apache.solr.ui.domain.RequestHandlerOption
import org.apache.solr.ui.views.components.HeaderDropdownOption
import org.apache.solr.ui.views.components.HeaderDropdownState
import kotlin.test.Test
import kotlin.test.assertEquals

class IndexQueryHeaderTest {

    private val handlers = listOf(
        RequestHandlerOption(name = "/select", className = "solr.SearchHandler"),
        RequestHandlerOption(name = "/update", className = "solr.UpdateRequestHandler"),
    )

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testNoCollectionsDisablesBothDropdownsAndShowsMessage() = runComposeUiTest {
        setContent {
            IndexQueryHeader(
                collectionDropdown = collectionDropdown(options = emptyList(), selectedKey = null),
                requestHandlers = requestHandlersDropdown(options = emptyList()),
            )
        }

        // The message text is a string resource that loads asynchronously on wasm, so the test checks the node
        onNodeWithTag("index_query_no_collections").assertExists()
        onNodeWithTag("index_query_collection_dropdown").assertIsNotEnabled()
        onNodeWithTag("index_query_handler_dropdown").assertIsNotEnabled()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testCollectionsLoadingDisablesBothDropdowns() = runComposeUiTest {
        setContent {
            IndexQueryHeader(
                collectionDropdown = collectionDropdown(isLoading = true),
                requestHandlers = requestHandlersDropdown(selectedKey = "/select"),
            )
        }

        onNodeWithTag("index_query_collection_dropdown").assertIsNotEnabled()
        onNodeWithTag("index_query_handler_dropdown").assertIsNotEnabled()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testHandlerDropdownStaysDisabledUntilCollectionIsSelected() = runComposeUiTest {
        setContent {
            IndexQueryHeader(
                collectionDropdown = collectionDropdown(selectedKey = null),
                requestHandlers = requestHandlersDropdown(options = handlers),
            )
        }

        onNodeWithTag("index_query_collection_dropdown").assertIsEnabled()
        onNodeWithTag("index_query_handler_dropdown").assertIsNotEnabled()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testHandlersLoadingDisablesOnlyHandlerDropdown() = runComposeUiTest {
        setContent {
            IndexQueryHeader(
                collectionDropdown = collectionDropdown(),
                requestHandlers = requestHandlersDropdown(options = emptyList(), isLoading = true),
            )
        }

        onNodeWithTag("index_query_collection_dropdown").assertIsEnabled()
        onNodeWithTag("index_query_handler_dropdown").assertIsNotEnabled()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testSelectingHandlerReportsItsName() = runComposeUiTest {
        val selected = mutableListOf<String>()
        setContent {
            IndexQueryHeader(
                collectionDropdown = collectionDropdown(),
                requestHandlers = requestHandlersDropdown(
                    options = handlers,
                    onSelect = { selected += it },
                ),
            )
        }

        onNodeWithTag("index_query_handler_dropdown").performClick()
        onNodeWithText("solr.SearchHandler").assertExists()
        onNodeWithTag("index_query_handler_dropdown_item_/select").performClick()
        assertEquals(listOf("/select"), selected)
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testSelectedHandlerShowsItsClassInTheClosedField() = runComposeUiTest {
        setContent {
            IndexQueryHeader(
                collectionDropdown = collectionDropdown(),
                requestHandlers = requestHandlersDropdown(selectedKey = "/select"),
            )
        }

        onNodeWithText("solr.SearchHandler").assertExists()
    }

    private fun collectionDropdown(
        options: List<String> = listOf("films"),
        isLoading: Boolean = false,
        selectedKey: String? = "films",
        onSelect: (String) -> Unit = {},
    ) = HeaderDropdownState(
        options = options.map { HeaderDropdownOption(key = it, label = it) },
        isLoading = isLoading,
        selectedKey = selectedKey,
        onSelect = onSelect,
    )

    private fun requestHandlersDropdown(
        options: List<RequestHandlerOption> = handlers,
        isLoading: Boolean = false,
        selectedKey: String? = null,
        onSelect: (String) -> Unit = {},
    ) = HeaderDropdownState(
        options = options.map {
            HeaderDropdownOption(key = it.name, label = it.name, subtitle = it.className)
        },
        isLoading = isLoading,
        selectedKey = selectedKey,
        onSelect = onSelect,
    )
}
