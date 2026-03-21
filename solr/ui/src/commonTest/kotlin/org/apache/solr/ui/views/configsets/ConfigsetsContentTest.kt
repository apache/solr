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
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.runComposeUiTest
import com.arkivanov.decompose.Child
import com.arkivanov.decompose.router.slot.ChildSlot
import com.arkivanov.decompose.value.MutableValue
import com.arkivanov.decompose.value.Value
import kotlin.test.Ignore
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.configsets.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.ConfigsetsComponent.Model
import org.apache.solr.ui.components.configsets.ConfigsetsOverviewComponent
import org.apache.solr.ui.components.configsets.ConfigsetsRouteComponent
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.views.navigation.configsets.ConfigsetsTab

@OptIn(ExperimentalTestApi::class)
class ConfigsetsContentTest {

    @Test
    @Ignore // See why the placeholder text is not shown
    fun `GIVEN no configsets THEN no_configsets_placeholder is shown`() = runComposeUiTest {
        val component = TestConfigsetsRouteComponent()

        setContent { ConfigsetsContent(component = component) }

        // Placeholder text from the TextField
        onNodeWithTag(testTag = "no_configsets_placeholder").assertExists()
    }

    @Test
    fun `GIVEN configsets WHEN a configset selected THEN onSelectConfigset called with configset`() = runComposeUiTest {
        val selectedConfigset = "gettingstarted"
        val expectedConfigsetSelection = "techproducts"
        val testComponent = DummyConfigsetsComponent(
            model = Model(
                configsets = listOf(selectedConfigset, expectedConfigsetSelection)
                    .map { Configset(it) },
                selectedConfigset = selectedConfigset,
            ),
        )
        val component = TestConfigsetsRouteComponent(
            configsetsComponent = testComponent,
        )

        setContent { ConfigsetsContent(component = component) }

        // Expand menu and select expected configset
        onNodeWithTag(testTag = "configsets_dropdown").performClick()
        onNodeWithTag(testTag = expectedConfigsetSelection).performClick()

        waitForIdle()
        assertEquals(
            expected = expectedConfigsetSelection,
            actual = testComponent.onSelectConfigset,
        )
    }
}

private class TestConfigsetsRouteComponent(
    override val configsetsComponent: ConfigsetsComponent = DummyConfigsetsComponent(),
) : ConfigsetsRouteComponent {

    private val overviewChild = ConfigsetsRouteComponent.Child.Overview(
        component = DummyConfigsetsOverviewComponent(),
    )

    override val tabSlot: Value<ChildSlot<ConfigsetsTab, ConfigsetsRouteComponent.Child>> =
        MutableValue(
            ChildSlot(
                Child.Created(
                    configuration = ConfigsetsTab.Overview,
                    instance = overviewChild,
                ),
            ),
        )

    // Tested in TabNavigationTest (no need to test here)
    override fun onNavigate(tab: ConfigsetsTab) = Unit
}

private class DummyConfigsetsComponent(model: Model = Model()) : ConfigsetsComponent {
    var onSelectConfigset: String? = null
    override val model: StateFlow<Model> = MutableStateFlow(model)

    override fun onSelectConfigset(name: String, reload: Boolean) {
        onSelectConfigset = name
    }
}

private class DummyConfigsetsOverviewComponent : ConfigsetsOverviewComponent {
    override val dialog: Value<ChildSlot<ConfigsetsOverviewComponent.CreateConfigsetDialogConfig, Unit>> =
        MutableValue(ChildSlot())

    override fun createConfigset() = Unit
    override fun importConfigset() = Unit
    override fun closeDialog() = Unit
    override fun editSolrConfig(name: String) = Unit
}
