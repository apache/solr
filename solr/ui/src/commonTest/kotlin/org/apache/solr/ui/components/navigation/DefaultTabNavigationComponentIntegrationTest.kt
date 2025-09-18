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

package org.apache.solr.ui.components.navigation

import com.arkivanov.decompose.router.slot.child
import com.arkivanov.essenty.lifecycle.Lifecycle
import com.arkivanov.essenty.lifecycle.LifecycleRegistry
import com.arkivanov.essenty.lifecycle.create
import com.arkivanov.essenty.lifecycle.resume
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import org.apache.solr.ui.TestAppComponentContext
import org.apache.solr.ui.components.navigation.TabNavigationComponent.Configuration
import org.apache.solr.ui.components.navigation.integration.DefaultTabNavigationComponent
import org.apache.solr.ui.components.start.integration.DefaultStartComponent
import org.apache.solr.ui.utils.AppComponentContext

@OptIn(ExperimentalCoroutinesApi::class)
class DefaultTabNavigationComponentIntegrationTest {

    @Serializable
    private enum class TestNavigationTab {
        Tab1,
        Tab2,
    }

    private data class TabChild(
        val configuration: Configuration<TestNavigationTab>,
        val context: AppComponentContext? = null,
    )

    @Test
    fun `GIVEN initial tab WHEN initialized THEN childFactory called with initial tab`() = runTest {
        val initialTab = TestNavigationTab.Tab1
        var called = false
        val component = createComponent(
            initialTab = initialTab,
            tabSerializer = TestNavigationTab.serializer(),
            childFactory = { configuration, context ->
                called = true
                TabChild(configuration, context)
            },
        )

        advanceUntilIdle()
        assertNotNull(actual = component.tabSlot.child?.instance)
        assertEquals(
            expected = TestNavigationTab.Tab1,
            actual = component.tabSlot.child?.configuration?.tab,
        )
        assertTrue(actual = called, message = "child factory never called")
    }

    @Test
    fun `GIVEN a selection WHEN navigate to other tab THEN other tab selected`() = runTest {
        val expectedTab = TestNavigationTab.Tab2
        val component = createComponent(
            initialTab = TestNavigationTab.Tab1,
            tabSerializer = TestNavigationTab.serializer(),
            childFactory = { configuration, context -> TabChild(configuration, context) },
        )
        advanceUntilIdle()

        component.onNavigate(expectedTab)
        advanceUntilIdle()

        assertEquals(
            expected = expectedTab,
            actual = component.tabSlot.child?.configuration?.tab,
        )
    }

    @Test
    fun `GIVEN a selection WHEN navigate to same tab THEN childFactory not called again`() = runTest {
        val initialTab = TestNavigationTab.Tab1
        var called = false
        val component = createComponent(
            initialTab = initialTab,
            tabSerializer = TestNavigationTab.serializer(),
            childFactory = { configuration, context ->
                if (!called) {
                    called = true
                } else {
                    fail("Should not be called twice")
                }
                TabChild(configuration, context)
            },
        )

        component.onNavigate(initialTab)
        advanceUntilIdle()
    }

    /**
     * Helper function for creating an instance of the [DefaultStartComponent].
     */
    private fun <T : Enum<T>, C : Any> TestScope.createComponent(
        componentContext: AppComponentContext = TestAppComponentContext(scheduler = testScheduler),
        lifecycle: LifecycleRegistry = LifecycleRegistry(),
        initialTab: T,
        tabSerializer: KSerializer<T>,
        childFactory: (Configuration<T>, AppComponentContext) -> C,
    ): TabNavigationComponent<T, C> {
        val component = DefaultTabNavigationComponent(
            componentContext = componentContext,
            tabSerializer = tabSerializer,
            initialTab = initialTab,
            childFactory = childFactory,
        )

        lifecycle.resume()
        return component
    }
}
