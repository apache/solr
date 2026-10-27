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
 */package org.apache.solr.ui.views.indexquery

import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assertIsEnabled
import androidx.compose.ui.test.assertIsNotEnabled
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.v2.runComposeUiTest
import org.apache.solr.ui.domain.RequestHandlerOption
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
                collections = emptyList(),
                collectionsLoading = false,
                selectedCollection = null,
                onSelectCollection = {},
                handlers = emptyList(),
                handlersLoading = false,
                selectedHandler = null,
                onSelectHandler = {},
            )
        }

        onNodeWithText("No collections are available").assertExists()
        onNodeWithTag("index_query_collection_dropdown").assertIsNotEnabled()
        onNodeWithTag("index_query_handler_dropdown").assertIsNotEnabled()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testCollectionsLoadingDisablesBothDropdowns() = runComposeUiTest {
        setContent {
            IndexQueryHeader(
                collections = listOf("films"),
                collectionsLoading = true,
                selectedCollection = null,
                onSelectCollection = {},
                handlers = emptyList(),
                handlersLoading = false,
                selectedHandler = null,
                onSelectHandler = {},
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
                collections = listOf("films"),
                collectionsLoading = false,
                selectedCollection = null,
                onSelectCollection = {},
                handlers = emptyList(),
                handlersLoading = false,
                selectedHandler = null,
                onSelectHandler = {},
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
                collections = listOf("films"),
                collectionsLoading = false,
                selectedCollection = "films",
                onSelectCollection = {},
                handlers = emptyList(),
                handlersLoading = true,
                selectedHandler = null,
                onSelectHandler = {},
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
                collections = listOf("films"),
                collectionsLoading = false,
                selectedCollection = "films",
                onSelectCollection = {},
                handlers = handlers,
                handlersLoading = false,
                selectedHandler = null,
                onSelectHandler = { selected += it },
            )
        }

        onNodeWithTag("index_query_handler_dropdown").performClick()
        onNodeWithText("solr.SearchHandler").assertExists()
        onNodeWithTag("index_query_handler_dropdown_item_/select").performClick()
        assertEquals(listOf("/select"), selected)
    }
}
