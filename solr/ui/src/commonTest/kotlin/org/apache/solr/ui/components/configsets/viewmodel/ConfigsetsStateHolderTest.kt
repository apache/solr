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

package org.apache.solr.ui.components.configsets.viewmodel

import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.TestDispatchers
import org.apache.solr.ui.components.configsets.domain.LoadConfigsetsUseCase
import org.apache.solr.ui.domain.Configset

@OptIn(ExperimentalCoroutinesApi::class)
class ConfigsetsStateHolderTest {

    private val configset1 = "_default"
    private val configset2 = "techproducts"
    private val configset3 = "getting_started"
    private val configsets = listOf(configset1, configset2, configset3)

    @Test
    fun `GIVEN configsets WHEN initialized THEN configsets fetched`() = runTest {
        val stateHolder = ConfigsetsStateHolder(
            scope = this,
            loadConfigsetsUseCase = getLoadConfigsetsUseCase(configsets),
            dispatchers = TestDispatchers(UnconfinedTestDispatcher(scheduler = testScheduler)),
        )
        advanceUntilIdle()

        stateHolder.uiState.value.configsets.forEach { configset ->
            assertContains(
                iterable = configsets,
                element = configset.name,
            )
        }
    }

    @Test
    fun `GIVEN configsets WHEN configsets fetched THEN configsets sorted`() = runTest {
        val stateHolder = ConfigsetsStateHolder(
            scope = this,
            loadConfigsetsUseCase = getLoadConfigsetsUseCase(configsets),
            dispatchers = TestDispatchers(UnconfinedTestDispatcher(scheduler = testScheduler)),
        )
        advanceUntilIdle()

        assertContentEquals(
            expected = configsets.sorted(),
            actual = stateHolder.uiState.value.configsets.map(Configset::name),
        )
    }

    @Test
    fun `GIVEN configsets WHEN configset selected THEN selectedConfigset updated`() = runTest {
        val stateHolder = ConfigsetsStateHolder(
            scope = this,
            loadConfigsetsUseCase = getLoadConfigsetsUseCase(configsets),
            dispatchers = TestDispatchers(UnconfinedTestDispatcher(scheduler = testScheduler)),
        )
        advanceUntilIdle()

        stateHolder.selectConfigset(configset = configset2)
        advanceUntilIdle()

        assertEquals(
            expected = configset2,
            actual = stateHolder.uiState.value.selectedConfigset,
        )
    }

    private fun getLoadConfigsetsUseCase(configsets: List<String>) =
        object : LoadConfigsetsUseCase {
            override suspend fun invoke(): Result<List<Configset>> =
                Result.success(configsets.map { Configset(name = it) })
        }
}
