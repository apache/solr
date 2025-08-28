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

package org.apache.solr.ui.views.auth

import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assert
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.assertIsNotEnabled
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.runComposeUiTest
import kotlin.test.Test
import kotlin.test.assertEquals
import org.apache.solr.ui.components.auth.BasicAuthComponent
import org.apache.solr.ui.components.auth.BasicAuthComponent.Model
import org.apache.solr.ui.isErrorSemantics

@OptIn(ExperimentalTestApi::class)
class BasicAuthContentTest {

    @Test
    fun `WHEN initialized THEN input is empty`() = runComposeUiTest {
        setContent {
            BasicAuthContent(createComponent())
        }

        assertEquals(
            expected = "",
            actual = onNodeWithTag("username_input_field")
                .assertExists()
                .fetchSemanticsNode()
                .config[SemanticsProperties.EditableText]
                .text,
        )

        assertEquals(
            expected = "",
            actual = onNodeWithTag("password_input_field")
                .assertExists()
                .fetchSemanticsNode()
                .config[SemanticsProperties.EditableText]
                .text,
        )
    }

    @Test
    fun `GIVEN isAuthenticating THEN inputs disabled`() = runComposeUiTest {
        setContent {
            BasicAuthContent(component = createComponent(), isAuthenticating = true)
        }

        onNodeWithTag("username_input_field").assertIsNotEnabled()
        onNodeWithTag("password_input_field").assertIsNotEnabled()
        onNodeWithTag("sign_in_button").assertIsNotEnabled()
    }

    @Test
    fun `GIVEN isAuthenticating THEN loading indicator displayed`() = runComposeUiTest {
        setContent {
            BasicAuthContent(component = createComponent(), isAuthenticating = true)
        }

        onNodeWithTag("loading_indicator").assertIsDisplayed()
    }

    @Test
    fun `GIVEN has error THEN input fields highlighted as error`() = runComposeUiTest {
        setContent {
            BasicAuthContent(component = createComponent(Model(hasError = true)))
        }

        onNodeWithTag("username_input_field").assert(matcher = isErrorSemantics)
        onNodeWithTag("password_input_field").assert(matcher = isErrorSemantics)
    }

    private fun createComponent(model: Model = Model()): BasicAuthComponent = TestBasicAuthComponent(model = model)
}
