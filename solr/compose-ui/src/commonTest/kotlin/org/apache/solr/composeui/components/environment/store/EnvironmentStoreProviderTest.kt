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

package org.apache.solr.composeui.components.environment.store

import com.arkivanov.mvikotlin.extensions.coroutines.stateFlow
import com.arkivanov.mvikotlin.main.store.DefaultStoreFactory
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.last
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.test.runTest
import org.apache.solr.composeui.components.environment.data.SystemData

@OptIn(ExperimentalCoroutinesApi::class)
class EnvironmentStoreProviderTest {

    @Test
    fun testFetchInitialSystemDataAction() = runTest(timeout = 1.seconds) {
        val expectedJavaProperties = listOf("key" to "value")
        val expectedSystemData = SystemData(solrHome = "some/path")
        val client = MockedEnvironmentStoreClient(
            onGetJavaProperties = { Result.success(expectedJavaProperties) },
            onGetSystemData = { Result.success(expectedSystemData) },
        )

        // When provide is called, the initial action is already included
        val store = EnvironmentStoreProvider(
            storeFactory = DefaultStoreFactory(),
            client = client,
            ioContext = coroutineContext
        ).provide()

        // not the best way to consume a flow, but the simplest
        // We expect 2 states to be emitted, one for each response received
        val state = store.stateFlow.take(2).last()

        assertNotNull(actual = state, message = "Expected state to be emitted.")
        assertContentEquals(
            expected = expectedJavaProperties,
            actual = state.javaProperties,
            message = "Expected java properties to match.",
        )
        assertEquals(
            expected = expectedSystemData.system,
            actual = state.system,
            message = "Expected system data to match.",
        )
        assertEquals(
            expected = expectedSystemData.jvm,
            actual = state.jvm,
            message = "Expected jvm data to match.",
        )
        assertEquals(
            expected = expectedSystemData.solrHome,
            actual = state.solrHome,
            message = "Expected solrHome to match.",
        )
    }

    @Test
    fun testFetchSystemDataIntent() = runTest(timeout = 5.seconds) {
        var initialRequest1 = true
        var initialRequest2 = true
        val expectedJavaProperties = listOf("key2" to "value2")
        val expectedSystemData = SystemData(solrHome = "some/path2")

        val client = MockedEnvironmentStoreClient(
            onGetJavaProperties = {
                if (initialRequest1) {
                    initialRequest1 = false
                    Result.success(listOf("key" to "value"))
                }
                // A second request should be sent in this scenario, so we
                // respond with different data
                else Result.success(expectedJavaProperties)
            },
            onGetSystemData = {
                if (initialRequest2) {
                    initialRequest2 = false
                    Result.success(SystemData(solrHome = "some/path"))
                }
                else Result.success(expectedSystemData)
            },
        )

        // When provide is called, the initial action is already included
        val store = EnvironmentStoreProvider(
            storeFactory = DefaultStoreFactory(),
            client = client,
            ioContext = coroutineContext,
        ).provide()

        // Send a fetch to re-fetch the data
        store.accept(EnvironmentStore.Intent.FetchSystemData)

        // This time we expect 4 states to be emitted, 2 for initialization, 2 for the intent
        val state = store.stateFlow.take(4).last()

        assertNotNull(actual = state, message = "Expected state to be emitted.")
        assertContentEquals(
            expected = expectedJavaProperties,
            actual = store.state.javaProperties,
            message = "Expected java properties to match.",
        )
        assertEquals(
            expected = expectedSystemData.system,
            actual = store.state.system,
            message = "Expected system data to match.",
        )
        assertEquals(
            expected = expectedSystemData.jvm,
            actual = store.state.jvm,
            message = "Expected jvm data to match.",
        )
        assertEquals(
            expected = expectedSystemData.solrHome,
            actual = store.state.solrHome,
            message = "Expected solrHome to match.",
        )
    }
}
