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

package org.apache.solr.ui.components.environment.viewmodel

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.TestDispatchers
import org.apache.solr.ui.components.environment.data.JavaProperty
import org.apache.solr.ui.components.environment.data.SystemData
import org.apache.solr.ui.components.environment.domain.LoadJavaPropertiesUseCase
import org.apache.solr.ui.components.environment.domain.LoadSystemDataUseCase

@OptIn(ExperimentalCoroutinesApi::class)
class EnvironmentStateHolderTest {

    @Test
    fun `GIVEN system data and java properties WHEN initialized THEN state contains them`() = runTest {
        val expectedJavaProperties = listOf("key" to "value")
        val expectedSystemData = SystemData(solrHome = "some/path")

        val stateHolder = createStateHolder(
            onLoadSystemData = { Result.success(expectedSystemData) },
            onLoadJavaProperties = { Result.success(expectedJavaProperties) },
        )
        advanceUntilIdle()

        val state = stateHolder.uiState.value
        assertEquals(expectedJavaProperties, state.javaProperties)
        assertEquals(expectedSystemData.system, state.system)
        assertEquals(expectedSystemData.jvm, state.jvm)
        assertEquals(expectedSystemData.solrHome, state.solrHome)
    }

    @Test
    fun `GIVEN loaded state WHEN fetchSystemData THEN state contains new data`() = runTest {
        var isInitialRequest = true
        val expectedJavaProperties = listOf("key2" to "value2")
        val expectedSystemData = SystemData(solrHome = "some/path2")

        val stateHolder = createStateHolder(
            onLoadSystemData = {
                // A second request should be sent in this scenario, so we
                // respond with different data
                if (isInitialRequest) Result.success(SystemData(solrHome = "some/path"))
                else Result.success(expectedSystemData)
            },
            onLoadJavaProperties = {
                if (isInitialRequest) Result.success(listOf("key" to "value"))
                else Result.success(expectedJavaProperties)
            },
        )
        advanceUntilIdle()

        isInitialRequest = false
        stateHolder.fetchSystemData()
        advanceUntilIdle()

        val state = stateHolder.uiState.value
        assertEquals(expectedJavaProperties, state.javaProperties)
        assertEquals(expectedSystemData.system, state.system)
        assertEquals(expectedSystemData.jvm, state.jvm)
        assertEquals(expectedSystemData.solrHome, state.solrHome)
    }

    private fun TestScope.createStateHolder(
        onLoadSystemData: () -> Result<SystemData>,
        onLoadJavaProperties: () -> Result<List<JavaProperty>>,
    ) = EnvironmentStateHolder(
        scope = this,
        loadSystemDataUseCase = object : LoadSystemDataUseCase {
            override suspend fun invoke() = onLoadSystemData()
        },
        loadJavaPropertiesUseCase = object : LoadJavaPropertiesUseCase {
            override suspend fun invoke() = onLoadJavaProperties()
        },
        dispatchers = TestDispatchers(UnconfinedTestDispatcher(scheduler = testScheduler)),
    )
}
