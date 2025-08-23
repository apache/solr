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
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.scale
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.action_connect
import org.apache.solr.ui.generated.resources.cd_solr_logo
import org.apache.solr.ui.generated.resources.connecting
import org.apache.solr.ui.generated.resources.desc_to_get_started
import org.apache.solr.ui.generated.resources.solr_sun
import org.apache.solr.ui.generated.resources.title_welcome_to_solr
import org.apache.solr.ui.utils.DEFAULT_SOLR_URL
import org.apache.solr.ui.views.components.SolrButton
import org.apache.solr.ui.views.components.SolrCard
import org.apache.solr.ui.views.components.SolrLinearProgressIndicator
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.resources.stringResource

/**
 * The composable used for users that have already authenticated.
 *
 * @param component Component that manages the state of the composable.
 */
@Composable
fun StartContent(
    component: StartComponent,
    modifier: Modifier = Modifier,
) = Row(
    modifier = modifier,
    horizontalArrangement = Arrangement.spacedBy(16.dp),
    verticalAlignment = Alignment.CenterVertically,
) {
    val model by component.model.collectAsState()

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

            OutlinedTextField(
                modifier = Modifier.fillMaxWidth().testTag("solr_url_input"),
                value = model.url,
                singleLine = true,
                onValueChange = component::onSolrUrlChange,
                placeholder = { Text(text = DEFAULT_SOLR_URL) },
                enabled = !model.isConnecting,
                supportingText = {
                    model.error?.let {
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
                    enabled = !model.isConnecting,
                    onClick = component::onConnect,
                ) {
                    Text(
                        text = stringResource(
                            if (model.isConnecting) {
                                Res.string.connecting
                            } else {
                                Res.string.action_connect
                            },
                        ),
                    )
                }
                if (model.isConnecting) {
                    SolrLinearProgressIndicator(
                        modifier = Modifier.fillMaxWidth().testTag("loading_indicator"),
                    )
                }
            }
        }
    }
}
