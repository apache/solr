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
import androidx.compose.ui.test.assertIsEnabled
import androidx.compose.ui.test.assertIsNotEnabled
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.performTextClearance
import androidx.compose.ui.test.performTextInput
import androidx.compose.ui.test.runComposeUiTest
import kotlin.test.Test
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import org.apache.solr.ui.TestDispatchers
import org.apache.solr.ui.components.configsets.domain.CreateConfigsetResult
import org.apache.solr.ui.components.configsets.domain.CreateConfigsetUseCase
import org.apache.solr.ui.components.configsets.domain.LoadConfigsetsUseCase
import org.apache.solr.ui.components.configsets.viewmodel.CreateConfigsetViewModel
import org.apache.solr.ui.domain.Configset

@OptIn(ExperimentalTestApi::class, ExperimentalCoroutinesApi::class)
class CreateConfigsetDialogTest {
    @Test
    fun `WHEN configset name typed THEN creation button enabled`() = runComposeUiTest {
        setContent {
            CreateConfigsetDialog(viewModel())
        }

        val configsetTextField = onNodeWithTag(testTag = "create_configset_name_field")
        configsetTextField.performTextInput("testconfigset")

        onNodeWithTag(testTag = "configset_create_button").assertIsEnabled()
    }

    @Test
    fun `GIVEN configset name WHEN clearing configset name THEN creation button disabled`() = runComposeUiTest {
        setContent {
            CreateConfigsetDialog(viewModel())
        }

        val configsetTextField = onNodeWithTag(testTag = "create_configset_name_field")
        configsetTextField.performTextInput("testconfigset")

        configsetTextField.performTextClearance()

        onNodeWithTag(testTag = "configset_create_button").assertIsNotEnabled()
    }

    private fun viewModel() = CreateConfigsetViewModel(
        createConfigsetUseCase = SuccessCreateConfigsetUseCase,
        loadConfigsetsUseCase = SuccessLoadConfigsetsUseCase(),
        dispatchers = TestDispatchers(io = UnconfinedTestDispatcher()),
    )
}

private object SuccessCreateConfigsetUseCase : CreateConfigsetUseCase {
    override suspend fun invoke(
        configsetName: String,
        baseConfigset: String?,
    ): CreateConfigsetResult = CreateConfigsetResult.Success(Configset(name = configsetName))
}

private class SuccessLoadConfigsetsUseCase(
    private val configsets: List<Configset> = emptyList(),
) : LoadConfigsetsUseCase {
    override suspend fun invoke(): Result<List<Configset>> = Result.success(configsets)
}
