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

package org.apache.solr.ui.preview.auth

import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.tooling.preview.Preview
import io.ktor.http.Url
import org.apache.solr.ui.components.auth.viewmodel.AuthenticationUiState
import org.apache.solr.ui.components.auth.viewmodel.BasicAuthUiState
import org.apache.solr.ui.components.auth.viewmodel.OAuthUiState
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthorizationFlow
import org.apache.solr.ui.domain.OAuthData
import org.apache.solr.ui.preview.PreviewContainer
import org.apache.solr.ui.views.auth.BasicAuthContent
import org.apache.solr.ui.views.auth.OAuthContent
import org.apache.solr.ui.views.auth.UserAuthenticationContent

@Composable
@Preview
internal fun PreviewBasicUserAuthenticationContent() = PreviewContainer {
    PreviewUserAuthenticationContent(
        withBasicAuth = true,
        withOAuth = false,
    )
}

@Composable
@Preview
internal fun PreviewOAuthUserAuthenticationContent() = PreviewContainer {
    PreviewUserAuthenticationContent(
        withBasicAuth = false,
        withOAuth = true,
    )
}

@Composable
@Preview
internal fun PreviewMultiUserAuthenticationContent() = PreviewContainer {
    PreviewUserAuthenticationContent(
        withBasicAuth = true,
        withOAuth = true,
    )
}

@Composable
private fun PreviewUserAuthenticationContent(
    withBasicAuth: Boolean,
    withOAuth: Boolean,
) = UserAuthenticationContent(
    uiState = AuthenticationUiState(
        methods = listOfNotNull(
            if (withBasicAuth) AuthMethod.BasicAuthMethod() else null,
            if (withOAuth) {
                AuthMethod.OAuthMethod(
                    data = OAuthData(
                        clientId = "",
                        authorizationFlow = AuthorizationFlow.CodePKCE,
                        scope = "",
                        redirectUris = listOf(Url("http://127.0.0.1")),
                        authorizationEndpoint = Url("http://127.0.0.1"),
                        tokenEndpoint = Url("http://127.0.0.1"),
                    ),
                )
            } else {
                null
            },
        ),
    ),
    onAbort = {},
    basicAuthContent = if (withBasicAuth) {
        { modifier: Modifier ->
            BasicAuthContent(
                uiState = BasicAuthUiState(),
                onChangeUsername = {},
                onChangePassword = {},
                onAuthenticate = {},
                modifier = modifier,
                isAuthenticating = false,
            )
        }
    } else {
        null
    },
    oAuthContent = if (withOAuth) {
        { modifier: Modifier, showSupportingText: Boolean ->
            OAuthContent(
                uiState = OAuthUiState(),
                onAuthenticate = {},
                modifier = modifier,
                isAuthenticating = false,
                showSupportingText = showSupportingText,
            )
        }
    } else {
        null
    },
)
