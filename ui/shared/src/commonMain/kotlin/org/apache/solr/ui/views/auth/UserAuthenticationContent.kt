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

import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.rememberUpdatedState
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.scale
import androidx.compose.ui.platform.LocalUriHandler
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import org.apache.solr.ui.components.auth.AuthenticationComponent
import org.apache.solr.ui.components.auth.domain.AuthenticationEvent
import org.apache.solr.ui.components.auth.viewmodel.AuthenticationUiState
import org.apache.solr.ui.shared.generated.resources.Res
import org.apache.solr.ui.shared.generated.resources.action_go_back
import org.apache.solr.ui.shared.generated.resources.cd_back_navigation
import org.apache.solr.ui.shared.generated.resources.cd_solr_logo
import org.apache.solr.ui.shared.generated.resources.chevron_left
import org.apache.solr.ui.shared.generated.resources.desc_solr_instance_with_auth
import org.apache.solr.ui.shared.generated.resources.seperator_or
import org.apache.solr.ui.shared.generated.resources.solr_sun
import org.apache.solr.ui.shared.generated.resources.title_sign_in_to_solr
import org.apache.solr.ui.views.components.SolrCard
import org.apache.solr.ui.views.components.SolrTextButton
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.resources.stringResource

/**
 * The user authentication content is the composable that will check and display
 * the available authentication options to the user.
 *
 * @param component The authentication component that provides the view model of this composable.
 * @param onEvent Called when the authentication screen emits an event that the parent has to
 * handle, e.g. when the user has been authenticated.
 * @param modifier Modifier to apply to the root composable.
 */
@Composable
fun UserAuthenticationContent(
    component: AuthenticationComponent,
    onEvent: (AuthenticationEvent) -> Unit,
    modifier: Modifier = Modifier,
) {
    val viewModel = viewModel { component.createAuthenticationViewModel() }
    val uiState by viewModel.uiState.collectAsState()
    val currentOnEvent by rememberUpdatedState(onEvent)
    val uriHandler = LocalUriHandler.current

    LaunchedEffect(viewModel) {
        viewModel.events.collect { currentOnEvent(it) }
    }

    LaunchedEffect(viewModel) {
        viewModel.authorizationUrls.collect { uriHandler.openUri(uri = it.toString()) }
    }

    val basicAuth = viewModel.basicAuth
    val oAuth = viewModel.oAuth

    UserAuthenticationContent(
        uiState = uiState,
        onAbort = viewModel::abort,
        modifier = modifier,
        basicAuthContent = basicAuth?.let { holder ->
            @Composable { contentModifier ->
                val basicAuthState by holder.uiState.collectAsState()

                BasicAuthContent(
                    uiState = basicAuthState,
                    onChangeUsername = holder::changeUsername,
                    onChangePassword = holder::changePassword,
                    onAuthenticate = holder::authenticate,
                    modifier = contentModifier,
                    isAuthenticating = uiState.isAuthenticating,
                )
            }
        },
        oAuthContent = oAuth?.let { holder ->
            @Composable { contentModifier, showSupportingText ->
                val oAuthState by holder.uiState.collectAsState()

                OAuthContent(
                    uiState = oAuthState,
                    onAuthenticate = holder::authenticate,
                    modifier = contentModifier,
                    isAuthenticating = uiState.isAuthenticating,
                    showSupportingText = showSupportingText,
                )
            }
        },
    )
}

/**
 * The user authentication content is the composable that will check and display
 * the available authentication options to the user.
 *
 * @param uiState The state of the authentication screen to render.
 * @param onAbort Called when the user wants to abort the authentication.
 * @param modifier Modifier to apply to the root composable.
 * @param basicAuthContent The content for authenticating with credentials (basic auth), or
 * `null` if the method is not supported.
 * @param oAuthContent The content for authenticating with OAuth, or `null` if the method is not
 * supported. Its second parameter tells whether to show supporting text, which is not the case
 * if multiple authentication options are available.
 */
@Composable
fun UserAuthenticationContent(
    uiState: AuthenticationUiState,
    onAbort: () -> Unit,
    modifier: Modifier = Modifier,
    basicAuthContent: (@Composable (modifier: Modifier) -> Unit)? = null,
    oAuthContent: (@Composable (modifier: Modifier, showSupportingText: Boolean) -> Unit)? = null,
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
        modifier = Modifier.weight(1f).padding(16.dp),
    ) {
        SolrTextButton(
            onClick = onAbort,
            contentPadding = ButtonDefaults.TextButtonWithIconContentPadding,
        ) {
            Icon(
                painter = painterResource(Res.drawable.chevron_left),
                contentDescription = stringResource(Res.string.cd_back_navigation),
            )
            Text(text = stringResource(Res.string.action_go_back))
        }

        SolrCard(
            modifier = Modifier
                .widthIn(min = 512.dp, max = 640.dp)
                .verticalScroll(rememberScrollState()),
            verticalArrangement = Arrangement.spacedBy(16.dp),
            horizontalAlignment = Alignment.CenterHorizontally,
        ) {
            Text(
                text = stringResource(Res.string.title_sign_in_to_solr),
                style = MaterialTheme.typography.headlineMedium,
            )

            Text(
                text = stringResource(Res.string.desc_solr_instance_with_auth, uiState.url),
                style = MaterialTheme.typography.bodyMedium,
            )

            basicAuthContent?.invoke(Modifier.testTag("basic_auth_content"))

            val hasMultiAuth = basicAuthContent != null && oAuthContent != null
            if (hasMultiAuth) {
                Text(
                    modifier = Modifier.testTag("separator_text"),
                    text = stringResource(Res.string.seperator_or),
                )
            }

            oAuthContent?.invoke(Modifier.testTag("basic_auth_content"), !hasMultiAuth)

            uiState.error?.let { error ->
                Text(
                    modifier = Modifier.testTag("error_text"),
                    text = stringResource(resource = error),
                    color = MaterialTheme.colorScheme.error,
                    style = MaterialTheme.typography.bodyMedium,
                )
            }
        }
    }
}
