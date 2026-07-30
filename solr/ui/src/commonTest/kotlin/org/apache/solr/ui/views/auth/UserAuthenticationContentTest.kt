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

import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.runComposeUiTest
import kotlin.test.Test
import org.apache.solr.ui.components.auth.AuthenticationComponent.Model
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.error_invalid_credentials

@OptIn(ExperimentalTestApi::class)
class UserAuthenticationContentTest {

    @Test
    fun `GIVEN error THEN error displayed`() = runComposeUiTest {
        val error = Res.string.error_invalid_credentials
        setContent {
            UserAuthenticationContent(createComponent(model = Model(error = error)))
        }

        onNodeWithTag("error_text").assertIsDisplayed()
    }

    private fun createComponent(model: Model = Model()) = TestAuthenticationComponent(model)
}
