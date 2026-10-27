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

package org.apache.solr.ui.components.indexquery

import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.components.indexquery.data.IndexQueryRepository
import org.apache.solr.ui.components.indexquery.viewmodel.IndexQueryStateHolder
import org.apache.solr.ui.domain.RequestHandlerOption
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

private val selectHandler = RequestHandlerOption(name = "/select", className = "solr.SearchHandler")

private class FakeIndexQueryRepository(
    var collections: Result<List<String>> = Result.success(emptyList()),
    var handlers: Map<String, Result<List<RequestHandlerOption>>> = emptyMap(),
) : IndexQueryRepository {
    override suspend fun listCollections(): Result<List<String>> = collections

    override suspend fun listRequestHandlers(collection: String): Result<List<RequestHandlerOption>> =
        handlers[collection] ?: Result.success(emptyList())
}

class IndexQueryStateHolderTest {

    @Test
    fun testLoadCollectionsPopulatesList() = runTest {
        val repository = FakeIndexQueryRepository(collections = Result.success(listOf("films", "shop")))
        val holder = IndexQueryStateHolder(repository, this)

        holder.loadCollections()
        advanceUntilIdle()

        assertEquals(listOf("films", "shop"), holder.uiState.value.collections)
        assertFalse(holder.uiState.value.collectionsLoading)
    }

    @Test
    fun testSelectingCollectionLoadsItsHandlersAndClearsHandlerChoice() = runTest {
        val repository = FakeIndexQueryRepository(
            handlers = mapOf("films" to Result.success(listOf(selectHandler))),
        )
        val holder = IndexQueryStateHolder(repository, this)

        holder.selectCollection("films")
        advanceUntilIdle()

        assertEquals(listOf(selectHandler), holder.uiState.value.handlers)
        assertNull(holder.uiState.value.selectedHandler)
        assertFalse(holder.uiState.value.handlersLoading)
    }

    @Test
    fun testFailedLoadSetsError() = runTest {
        val repository = FakeIndexQueryRepository(collections = Result.failure(Exception("down")))
        val holder = IndexQueryStateHolder(repository, this)

        holder.loadCollections()
        advanceUntilIdle()

        assertTrue(holder.uiState.value.error)
        assertFalse(holder.uiState.value.collectionsLoading)
    }
}
