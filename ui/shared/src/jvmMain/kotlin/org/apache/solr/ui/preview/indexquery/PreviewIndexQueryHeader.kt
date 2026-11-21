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

package org.apache.solr.ui.preview.indexquery

import androidx.compose.runtime.Composable
import androidx.compose.ui.tooling.preview.Preview
import org.apache.solr.ui.preview.PreviewContainer
import org.apache.solr.ui.views.components.HeaderDropdownOption
import org.apache.solr.ui.views.components.HeaderDropdownState
import org.apache.solr.ui.views.indexquery.IndexQueryHeader

@Composable
@Preview
internal fun PreviewBasicIndexQueryHeader() = PreviewContainer {
    IndexQueryHeader(
        collectionDropdown = HeaderDropdownState(
            options = listOf("dog", "cat").map { HeaderDropdownOption(key = it, label = it) },
            isLoading = false,
            selectedKey = "cat",
            onSelect = {},
        ),
        requestHandlers = HeaderDropdownState(
            options = listOf(HeaderDropdownOption(key = "Handle", label = "Handle", subtitle = "solr.myRequestHandler")),
            isLoading = false,
            selectedKey = null,
            onSelect = {},
        ),
    )
}

@Composable
@Preview
internal fun PreviewLoadingIndexQueryHeader() = PreviewContainer {
    IndexQueryHeader(
        collectionDropdown = HeaderDropdownState(
            options = listOf("dog", "cat").map { HeaderDropdownOption(key = it, label = it) },
            isLoading = true,
            selectedKey = "cat",
            onSelect = {},
        ),
        requestHandlers = HeaderDropdownState(
            options = listOf(HeaderDropdownOption(key = "Handle", label = "Handle", subtitle = "solr.myRequestHandler")),
            isLoading = true,
            selectedKey = "Handle",
            onSelect = {},
        ),
    )
}

@Composable
@Preview
internal fun PreviewNoCollectionsIndexQueryHeader() = PreviewContainer {
    IndexQueryHeader(
        collectionDropdown = HeaderDropdownState(
            options = emptyList(),
            isLoading = false,
            selectedKey = null,
            onSelect = {},
        ),
        requestHandlers = HeaderDropdownState(
            options = emptyList(),
            isLoading = false,
            selectedKey = null,
            onSelect = {},
        ),
    )
}

@Composable
@Preview
internal fun PreviewNoRequestHandlersIndexQueryHeader() = PreviewContainer {
    IndexQueryHeader(
        collectionDropdown = HeaderDropdownState(
            options = listOf(HeaderDropdownOption(key = "dog", label = "dog")),
            isLoading = false,
            selectedKey = "dog",
            onSelect = {},
        ),
        requestHandlers = HeaderDropdownState(
            options = emptyList(),
            isLoading = false,
            selectedKey = null,
            onSelect = {},
        ),
    )
}
