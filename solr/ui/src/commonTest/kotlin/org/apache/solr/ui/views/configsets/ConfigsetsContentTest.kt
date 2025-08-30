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
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.runComposeUiTest
import com.arkivanov.decompose.router.stack.ChildStack
import com.arkivanov.decompose.value.MutableValue
import com.arkivanov.decompose.value.Value
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.configsets.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.data.Configset
import org.apache.solr.ui.components.configsets.overview.ConfigsetsOverviewComponent
import org.apache.solr.ui.views.navigation.configsets.ConfigsetsTab

@OptIn(ExperimentalTestApi::class)
class ConfigsetsContentTest {

    @Test
    fun emptyList_showsPlaceholder() = runComposeUiTest {
        val comp = TestConfigsetsComponent(emptyList())

        setContent { ConfigsetsContent(component = comp) }

        // Placeholder text from the TextField
        onNodeWithText("No configsets available").assertExists()

        // Field click shouldn’t open the menu (anchor is disabled in our impl)
        onNodeWithText("No configsets available").performClick()
        runOnIdle { assertFalse(comp.model.value.expanded) }
    }

    @Test
    fun selectItem_updatesSelection_andClosesMenu() = runComposeUiTest {
        val comp = TestConfigsetsComponent(
            names = listOf("gettingstarted", "techproducts"),
            selected = "gettingstarted",
        )

        setContent { ConfigsetsContent(component = comp) }

        // Open
        onNodeWithText("gettingstarted").assertExists().performClick()

        // Select another
        onNodeWithText("techproducts").assertExists().performClick()

        // State updated + menu closed
        runOnIdle {
            assertEquals("techproducts", comp.model.value.selectedConfigset)
            assertFalse(comp.model.value.expanded)
        }
    }

    @Test
    fun testSubsectionSwitch() = runComposeUiTest {
        val comp = TestConfigsetsComponent(names = listOf("basic_configs"))

        setContent { ConfigsetsContent(component = comp) }

        // Click using the text node
        onNodeWithText("Schema").assertExists().performClick()

        runOnIdle { assertEquals(ConfigsetsTab.Schema, comp.model.value.selectedTab) }
    }
}

private object DummyOverviewComponent : ConfigsetsOverviewComponent

class TestConfigsetsComponent(
    names: List<String>,
    selected: String = "",
    expanded: Boolean = false,
) : ConfigsetsComponent {

    private val _model = MutableStateFlow(
        ConfigsetsComponent.Model(
            selectedTab = ConfigsetsTab.Overview,
            configsets = names.map(::Configset),
            selectedConfigset = selected,
            expanded = expanded,
        ),
    )
    override val model: StateFlow<ConfigsetsComponent.Model> = _model

    private val overviewChild =
        ConfigsetsComponent.Child.Overview(object : ConfigsetsOverviewComponent {})

    private val _childStack = MutableValue(
        ChildStack<Any, ConfigsetsComponent.Child>(
            configuration = Unit,
            instance = overviewChild,
        ),
    )
    override val childStack: Value<ChildStack<*, ConfigsetsComponent.Child>> = _childStack

    override fun onSelectTab(tab: ConfigsetsTab) {
        _model.value = _model.value.copy(selectedTab = tab)
    }

    override fun onSelectConfigset(name: String) {
        _model.value = _model.value.copy(selectedConfigset = name)
    }

    override fun setMenuExpanded(expanded: Boolean) {
        _model.value = _model.value.copy(expanded = expanded)
    }
}
