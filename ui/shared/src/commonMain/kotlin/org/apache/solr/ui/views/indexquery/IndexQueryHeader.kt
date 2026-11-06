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

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.width
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.label_collection_core
import org.apache.solr.ui.shared.generated.resources.label_request_handler
import org.apache.solr.ui.shared.generated.resources.no_collections_available
import org.apache.solr.ui.shared.generated.resources.no_request_handlers
import org.apache.solr.ui.shared.generated.resources.placeholder_select_request_handler
import org.apache.solr.ui.views.components.HeaderDropdown
import org.apache.solr.ui.views.components.HeaderDropdownState
import org.jetbrains.compose.resources.stringResource

/**
 * Header of the Index & Query section with a collection dropdown and a request handler dropdown.
 *
 * The header only renders what it is given. Loading and selection happen in the caller.
 *
 * @param collectionDropdown Dropdown state for the available collections.
 * @param requestHandlers Dropdown state for the selected collection's request handlers.
 * @param modifier Modifier to apply to the root column.
 */
@Composable
fun IndexQueryHeader(
    collectionDropdown: HeaderDropdownState,
    requestHandlers: HeaderDropdownState,
    modifier: Modifier = Modifier,
) {
    val requestHandlersDropdown = requestHandlers.copy(
        isEnabled = requestHandlers.isEnabled &&
            !collectionDropdown.isLoading &&
            collectionDropdown.selectedKey != null,
    )

    Column(modifier = modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
        Row(horizontalArrangement = Arrangement.spacedBy(16.dp)) {
            HeaderDropdown(
                label = stringResource(Res.string.label_collection_core),
                state = collectionDropdown,
                testTag = "index_query_collection_dropdown",
                modifier = Modifier.width(256.dp),
            )
            HeaderDropdown(
                label = stringResource(Res.string.label_request_handler),
                state = requestHandlersDropdown,
                testTag = "index_query_handler_dropdown",
                modifier = Modifier.width(384.dp),
                placeholder = stringResource(Res.string.placeholder_select_request_handler),
            )
        }
        if (!collectionDropdown.isLoading && collectionDropdown.options.isEmpty()) {
            Text(
                text = stringResource(Res.string.no_collections_available),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.testTag("index_query_no_collections"),
            )
        }
        if (
            collectionDropdown.selectedKey != null &&
            !requestHandlers.isLoading &&
            requestHandlers.options.isEmpty()
        ) {
            Text(
                text = stringResource(Res.string.no_request_handlers),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.testTag("index_query_no_handlers"),
            )
        }
    }
}
