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

import androidx.compose.desktop.ui.tooling.preview.Preview
import androidx.compose.runtime.Composable
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.error_invalid_url
import org.apache.solr.ui.preview.PreviewContainer
import org.apache.solr.ui.views.start.StartContent

@Preview
@Composable
private fun PreviewStartContent() = PreviewContainer {
    StartContent(component = PreviewStartComponent)
}

@Preview
@Composable
private fun PreviewStartContentWithError() = PreviewContainer {
    StartContent(component = PreviewStartComponentWithError)
}

@Preview
@Composable
private fun PreviewStartContentWithConnecting() = PreviewContainer {
    StartContent(component = PreviewStartComponentWithConnecting)
}

private object PreviewStartComponent : StartComponent {
    override val model: StateFlow<StartComponent.Model> =
        MutableStateFlow(StartComponent.Model())

    override fun onSolrUrlChange(url: String) = Unit
    override fun onConnect() = Unit
}

private object PreviewStartComponentWithError : StartComponent {
    override val model: StateFlow<StartComponent.Model> = MutableStateFlow(
        StartComponent.Model(
            url = "some-invalid-url!",
            error = Res.string.error_invalid_url,
        ),
    )

    override fun onSolrUrlChange(url: String) = Unit
    override fun onConnect() = Unit
}

private object PreviewStartComponentWithConnecting : StartComponent {
    override val model: StateFlow<StartComponent.Model> = MutableStateFlow(
        StartComponent.Model(
            isConnecting = true,
        ),
    )

    override fun onSolrUrlChange(url: String) = Unit
    override fun onConnect() = Unit
}
