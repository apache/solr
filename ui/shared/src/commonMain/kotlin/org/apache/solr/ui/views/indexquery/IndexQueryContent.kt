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
package org.apache.solr.ui.views.indexquery

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.indexquery.viewmodel.IndexQueryViewModel
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.index_query_error
import org.apache.solr.ui.views.components.HeaderDropdownOption
import org.apache.solr.ui.views.components.HeaderDropdownState
import org.jetbrains.compose.resources.stringResource

/**
 * The Index & Query screen: the collection and request handler header, hosted in the UI.
 *
 * The request form for the selected handler is a separate task and plugs in below the header
 * once it lands.
 *
 * @param viewModel Holds the collections and request handlers.
 * @param modifier Modifier to apply to the root column.
 */
@Composable
fun IndexQueryContent(
    viewModel: IndexQueryViewModel,
    modifier: Modifier = Modifier,
) {
    val stateHolder = viewModel.stateHolder
    val state by stateHolder.uiState.collectAsState()

    Column(modifier = modifier.fillMaxWidth()) {
        IndexQueryHeader(
            collectionDropdown = HeaderDropdownState(
                options = state.collections.map { HeaderDropdownOption(key = it, label = it) },
                isLoading = state.collectionsLoading,
                selectedKey = state.selectedCollection,
                onSelect = stateHolder::selectCollection,
            ),
            requestHandlers = HeaderDropdownState(
                options = state.handlers.map {
                    HeaderDropdownOption(key = it.name, label = it.name, subtitle = it.className)
                },
                isLoading = state.handlersLoading,
                selectedKey = state.selectedHandler,
                onSelect = stateHolder::selectHandler,
            ),
        )
        if (state.error) {
            Text(
                text = stringResource(Res.string.index_query_error),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.error,
                modifier = Modifier.padding(top = 16.dp),
            )
        }
    }
}
