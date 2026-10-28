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

package org.apache.solr.ui.views.collections

import androidx.compose.ui.test.ExperimentalTestApi
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.v2.runComposeUiTest
import org.apache.solr.ui.domain.CreateCollectionResult
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.error_collection_name_exists
import org.apache.solr.ui.shared.generated.resources.error_create_collection_general
import org.jetbrains.compose.resources.getString
import kotlin.test.Test

class CreateCollectionErrorsTest {

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testNameExistsShowsTheNameError() = runComposeUiTest {
        getString(Res.string.error_collection_name_exists)
        setContent {
            CreateCollectionForm(
                configsets = emptyList(),
                nameError = CreateCollectionResult.NameExists.nameError(),
                generalError = CreateCollectionResult.NameExists.generalError(),
                onCreate = {},
                onCancel = {},
            )
        }
        awaitIdle()

        onNodeWithText("A collection with this name already exists").assertExists()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testFailureWithoutMessageShowsTheFallback() = runComposeUiTest {
        getString(Res.string.error_create_collection_general)
        setContent {
            CreateCollectionForm(
                configsets = emptyList(),
                nameError = CreateCollectionResult.Failed(null).nameError(),
                generalError = CreateCollectionResult.Failed(null).generalError(),
                onCreate = {},
                onCancel = {},
            )
        }
        awaitIdle()

        onNodeWithText("Could not create the collection.").assertExists()
    }

    @OptIn(ExperimentalTestApi::class)
    @Test
    fun testFailureWithMessageShowsTheServerMessage() = runComposeUiTest {
        setContent {
            CreateCollectionForm(
                configsets = emptyList(),
                nameError = CreateCollectionResult.Failed("Invalid collection").nameError(),
                generalError = CreateCollectionResult.Failed("Invalid collection").generalError(),
                onCreate = {},
                onCancel = {},
            )
        }

        onNodeWithText("Invalid collection").assertExists()
    }
}
