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
import androidx.compose.ui.test.assertTextContains
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performTextClearance
import androidx.compose.ui.test.runComposeUiTest
import kotlin.test.Test
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import org.apache.solr.ui.TestDispatchers
import org.apache.solr.ui.components.configsets.domain.ImportConfigsetResult
import org.apache.solr.ui.components.configsets.domain.ImportConfigsetUseCase
import org.apache.solr.ui.components.configsets.viewmodel.ImportConfigsetViewModel
import org.apache.solr.ui.components.files.domain.SelectFileResult
import org.apache.solr.ui.components.files.domain.SelectFileUseCase
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.domain.PickedFile

@OptIn(ExperimentalTestApi::class, ExperimentalCoroutinesApi::class)
class ImportConfigsetDialogTest {
    val expectedConfigsetName = "testconfigset"
    val fileName = "${expectedConfigsetName}_configset.zip"
    val file = PickedFile(name = fileName, bytes = byteArrayOf(), extension = "zip")

    @Test
    fun `WHEN configset file selected THEN import button enabled`() = runComposeUiTest {
        setContent {
            val viewModel = viewModel(file)
            ImportConfigsetDialog(viewModel)
            viewModel.selectFile()
        }

        onNodeWithTag(testTag = "configset_import_button").assertIsEnabled()
    }

    @Test
    fun `WHEN configset file selected THEN configset name autofilled with modified file name`() = runComposeUiTest {
        setContent {
            val viewModel = viewModel(file)
            ImportConfigsetDialog(viewModel)
            viewModel.selectFile()
        }

        onNodeWithTag(testTag = "import_configset_name_field")
            .assertTextContains(expectedConfigsetName)
    }

    @Test
    fun `GIVEN configset file selected WHEN clearing configset file THEN import button disabled`() = runComposeUiTest {
        setContent {
            val viewModel = viewModel(file)
            ImportConfigsetDialog(viewModel)
            viewModel.selectFile()
        }

        val configsetClearButton = onNodeWithTag(testTag = "file_selector_clear_button")
        configsetClearButton.performClick()

        onNodeWithTag(testTag = "configset_import_button").assertIsNotEnabled()
    }

    @Test
    fun `GIVEN configset file selected WHEN clearing configset name THEN import button disabled`() = runComposeUiTest {
        setContent {
            val viewModel = viewModel(file)
            ImportConfigsetDialog(viewModel)
            viewModel.selectFile()
        }

        val configsetTextField = onNodeWithTag(testTag = "import_configset_name_field")
        configsetTextField.performTextClearance()

        onNodeWithTag(testTag = "configset_import_button").assertIsNotEnabled()
    }

    private fun viewModel(file: PickedFile) = ImportConfigsetViewModel(
        importConfigsetUseCase = SuccessImportConfigsetUseCase,
        selectFileUseCase = SuccessSelectFileUseCase(file),
        dispatchers = TestDispatchers(io = UnconfinedTestDispatcher()),
    )
}

private object SuccessImportConfigsetUseCase : ImportConfigsetUseCase {
    override suspend fun invoke(
        configsetName: String,
        file: PickedFile
    ): ImportConfigsetResult = ImportConfigsetResult.Success(Configset(name = configsetName))
}

private class SuccessSelectFileUseCase(private val file: PickedFile) : SelectFileUseCase {
    override suspend fun invoke(
        extensions: List<String>,
        maxSize: Int?
    ): SelectFileResult = SelectFileResult.Success(file)
}
