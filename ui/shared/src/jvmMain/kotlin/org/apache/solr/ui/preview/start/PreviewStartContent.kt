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

package org.apache.solr.ui.preview.start

import androidx.compose.runtime.Composable
import androidx.compose.ui.tooling.preview.Preview
import org.apache.solr.ui.components.start.viewmodel.StartUiState
import org.apache.solr.ui.preview.PreviewContainer
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.error_invalid_url
import org.apache.solr.ui.views.start.StartContent

@Preview
@Composable
private fun PreviewStartContent() = PreviewContainer {
    StartContent(
        uiState = StartUiState(),
        onSolrUrlChange = {},
        onConnect = {},
    )
}

@Preview
@Composable
private fun PreviewStartContentWithError() = PreviewContainer {
    StartContent(
        uiState = StartUiState(
            url = "some-invalid-url!",
            error = Res.string.error_invalid_url,
        ),
        onSolrUrlChange = {},
        onConnect = {},
    )
}

@Preview
@Composable
private fun PreviewStartContentWithConnecting() = PreviewContainer {
    StartContent(
        uiState = StartUiState(isConnecting = true),
        onSolrUrlChange = {},
        onConnect = {},
    )
}
