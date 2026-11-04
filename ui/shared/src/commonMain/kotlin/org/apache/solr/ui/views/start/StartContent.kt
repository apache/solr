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

package org.apache.solr.ui.views.start

import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.widthIn
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.rememberUpdatedState
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.scale
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.components.start.domain.StartEvent
import org.apache.solr.ui.components.start.viewmodel.StartUiState
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.action_connect
import org.apache.solr.ui.shared.generated.resources.cd_solr_logo
import org.apache.solr.ui.shared.generated.resources.connecting
import org.apache.solr.ui.shared.generated.resources.desc_to_get_started
import org.apache.solr.ui.shared.generated.resources.solr_sun
import org.apache.solr.ui.shared.generated.resources.title_welcome_to_solr
import org.apache.solr.ui.utils.defaultSolrUrl
import org.apache.solr.ui.views.components.SolrButton
import org.apache.solr.ui.views.components.SolrCard
import org.apache.solr.ui.views.components.SolrLinearProgressIndicator
import org.apache.solr.ui.views.components.SolrOutlinedTextField
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.resources.stringResource

/**
 * The composable used for users that have already authenticated.
 *
 * @param component Component that provides the view model of the composable.
 * @param onEvent Called when the start screen emits an event that the parent has to handle,
 * e.g. when a connection has been established.
 */
@Composable
fun StartContent(
    component: StartComponent,
    onEvent: (StartEvent) -> Unit,
    modifier: Modifier = Modifier,
) {
    val viewModel = viewModel { component.createStartViewModel() }
    val uiState by viewModel.uiState.collectAsState()
    val currentOnEvent by rememberUpdatedState(onEvent)

    LaunchedEffect(viewModel) {
        viewModel.events.collect { currentOnEvent(it) }
    }

    StartContent(
        uiState = uiState,
        onSolrUrlChange = viewModel::changeSolrUrl,
        onConnect = viewModel::connect,
        modifier = modifier,
    )
}

/**
 * The composable used for connecting to a Solr instance.
 *
 * @param uiState The state of the start screen to render.
 * @param onSolrUrlChange Called when the user changes the Solr URL.
 * @param onConnect Called when the user wants to connect to the Solr URL.
 */
@Composable
fun StartContent(
    uiState: StartUiState,
    onSolrUrlChange: (String) -> Unit,
    onConnect: () -> Unit,
    modifier: Modifier = Modifier,
) = Row(
    modifier = modifier,
    horizontalArrangement = Arrangement.spacedBy(16.dp),
    verticalAlignment = Alignment.CenterVertically,
) {
    Image(
        modifier = Modifier.weight(1f)
            .align(Alignment.Bottom)
            .fillMaxWidth()
            .aspectRatio(1f)
            .scale(1.5f),
        alpha = .3f,
        painter = painterResource(Res.drawable.solr_sun),
        contentDescription = stringResource(Res.string.cd_solr_logo),
    )

    Column(
        modifier = Modifier.weight(1f).padding(64.dp),
    ) {
        SolrCard(
            modifier = Modifier.widthIn(min = 512.dp, max = 640.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            Text(
                modifier = Modifier.testTag("start_title"),
                text = stringResource(Res.string.title_welcome_to_solr),
                style = MaterialTheme.typography.headlineMedium,
            )
            Text(
                modifier = Modifier.testTag("start_description"),
                text = stringResource(Res.string.desc_to_get_started),
                style = MaterialTheme.typography.bodyMedium,
            )

            SolrOutlinedTextField(
                modifier = Modifier.fillMaxWidth().testTag("solr_url_input"),
                value = uiState.url,
                singleLine = true,
                onValueChange = onSolrUrlChange,
                placeholder = { Text(text = defaultSolrUrl()) },
                enabled = !uiState.isConnecting,
                supportingText = {
                    uiState.error?.let {
                        Text(
                            modifier = Modifier.testTag("input_error"),
                            text = stringResource(it),
                            color = MaterialTheme.colorScheme.error,
                        )
                    }
                },
                // TODO Update colors if necessary
            )

            Column {
                SolrButton(
                    modifier = Modifier.fillMaxWidth().testTag("connect_button"),
                    enabled = !uiState.isConnecting,
                    onClick = onConnect,
                ) {
                    Text(
                        text = stringResource(
                            if (uiState.isConnecting) {
                                Res.string.connecting
                            } else {
                                Res.string.action_connect
                            },
                        ),
                    )
                }
                if (uiState.isConnecting) {
                    SolrLinearProgressIndicator(
                        modifier = Modifier.fillMaxWidth().testTag("loading_indicator"),
                    )
                }
            }
        }
    }
}
