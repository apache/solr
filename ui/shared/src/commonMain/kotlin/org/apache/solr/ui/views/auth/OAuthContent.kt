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

package org.apache.solr.ui.views.auth

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalUriHandler
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.auth.OAuthComponent
import org.apache.solr.ui.components.auth.store.OAuthStore
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.action_sign_in_with_identity_provider
import org.apache.solr.ui.generated.resources.action_sign_in_with_realm
import org.apache.solr.ui.generated.resources.authenticating
import org.apache.solr.ui.generated.resources.desc_sign_in_with_oauth
import org.apache.solr.ui.generated.resources.desc_sign_in_with_oauth_to_realm
import org.apache.solr.ui.views.components.SolrButton
import org.jetbrains.compose.resources.stringResource

/**
 * The Oauth content is an input button where the user can click to authenticate and sign in to a
 * Solr instance via an identity provider.
 *
 * @param component The [OAuthComponent] that is handling the interactions with this composable.
 * @param modifier Modifier that is applied to the root of this composable.
 * @param isAuthenticating Whether the user is currently being authenticated. This disables the inputs
 * and updates the text shown in the button.
 * @param showSupportingText Whether to show supporting text above the sign-in button. Settings this
 * value to `false` allows a more compact view when multiple authentication options are available.
 */
@Composable
fun OAuthContent(
    component: OAuthComponent,
    modifier: Modifier = Modifier,
    isAuthenticating: Boolean = true,
    showSupportingText: Boolean = true,
) = Column(
    modifier = modifier,
    verticalArrangement = Arrangement.spacedBy(16.dp),
) {
    val model by component.model.collectAsState()

    if (showSupportingText) {
        Text(
            text = model.realm?.let {
                stringResource(Res.string.desc_sign_in_with_oauth_to_realm, it)
            } ?: stringResource(Res.string.desc_sign_in_with_oauth),
            style = MaterialTheme.typography.bodyMedium,
        )
    }

    Column {
        SolrButton(
            modifier = Modifier.fillMaxWidth().testTag(tag = "oauth_sign_in_button"),
            onClick = component::onAuthenticate,
            enabled = !isAuthenticating,
        ) {
            Text(
                text = if (isAuthenticating) {
                    stringResource(Res.string.authenticating)
                } else {
                    model.realm?.let {
                        stringResource(Res.string.action_sign_in_with_realm, it)
                    } ?: stringResource(Res.string.action_sign_in_with_identity_provider)
                },
            )
        }
        if (isAuthenticating) {
            LinearProgressIndicator(
                modifier = Modifier.fillMaxWidth().testTag(tag = "loading_indicator"),
            )
        }
    }

    val uriHandler = LocalUriHandler.current
    // Listen for labels once per composition
    LaunchedEffect(component) {
        component.labels.collect { label ->
            when (label) {
                is OAuthStore.Label.AuthenticationStarted -> uriHandler.openUri(uri = label.url.toString())
                else -> Unit
            }
        }
    }
}
