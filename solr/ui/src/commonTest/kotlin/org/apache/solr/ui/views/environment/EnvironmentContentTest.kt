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

package org.apache.solr.ui.views.environment

import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.runComposeUiTest
import kotlin.test.Test
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.environment.EnvironmentComponent
import org.apache.solr.ui.components.environment.data.Versions

class EnvironmentContentTest {

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testEnvironmentContentRendering() = runComposeUiTest {
        setContent {
            EnvironmentContent(TestEnvironmentComponent)
        }

        // Test if component data is displayed
        onNodeWithText("solr-spec version").assertExists()

        (1..7).forEach {
            onNodeWithText("key $it").assertExists()
            onNodeWithText("value $it").assertExists()
        }
    }
}

private object TestEnvironmentComponent : EnvironmentComponent {
    override val model: StateFlow<EnvironmentComponent.Model> = MutableStateFlow(
        EnvironmentComponent.Model(
            versions = Versions(
                solrSpecVersion = "solr-spec version",
            ),
            javaProperties = listOf(
                "key 1" to "value 1",
                "key 2" to "value 2",
                "key 3" to "value 3",
                "key 4" to "value 4",
                "key 5" to "value 5",
                "key 6" to "value 6",
                "key 7" to "value 7",
            ),
        ),
    )
}
