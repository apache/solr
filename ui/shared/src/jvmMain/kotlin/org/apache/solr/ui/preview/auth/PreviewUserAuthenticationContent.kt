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
import androidx.compose.ui.tooling.preview.Preview
import com.arkivanov.decompose.Child
import com.arkivanov.decompose.router.slot.ChildSlot
import com.arkivanov.decompose.value.MutableValue
import com.arkivanov.decompose.value.Value
import io.ktor.http.Url
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.auth.AuthenticationComponent
import org.apache.solr.ui.components.auth.AuthenticationComponent.BasicAuthConfiguration
import org.apache.solr.ui.components.auth.AuthenticationComponent.OAuthConfiguration
import org.apache.solr.ui.components.auth.BasicAuthComponent
import org.apache.solr.ui.components.auth.OAuthComponent
import org.apache.solr.ui.components.auth.store.OAuthStore
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthorizationFlow
import org.apache.solr.ui.domain.OAuthData
import org.apache.solr.ui.preview.PreviewContainer
import org.apache.solr.ui.views.auth.UserAuthenticationContent

@Composable
@Preview
internal fun PreviewBasicUserAuthenticationContent() = PreviewContainer {
    UserAuthenticationContent(
        component = PreviewAuthenticationComponentWithBasicAuth(
            withBasicAuth = true,
            withOAuth = false,
        ),
    )
}

@Composable
@Preview
internal fun PreviewOAuthUserAuthenticationContent() = PreviewContainer {
    UserAuthenticationContent(
        component = PreviewAuthenticationComponentWithBasicAuth(
            withBasicAuth = false,
            withOAuth = true,
        ),
    )
}

@Composable
@Preview
internal fun PreviewMultiUserAuthenticationContent() = PreviewContainer {
    UserAuthenticationContent(
        component = PreviewAuthenticationComponentWithBasicAuth(
            withBasicAuth = true,
            withOAuth = true,
        ),
    )
}

private class PreviewAuthenticationComponentWithBasicAuth(
    private val withBasicAuth: Boolean = false,
    private val withOAuth: Boolean = false,
    model: AuthenticationComponent.Model = AuthenticationComponent.Model(),
) : AuthenticationComponent {
    constructor(
        withBasicAuth: Boolean = false,
        withOAuth: Boolean = false,
    ) : this(
        withBasicAuth = withBasicAuth,
        withOAuth = withOAuth,
        model = AuthenticationComponent.Model(
            methods = listOfNotNull(
                if (withBasicAuth) AuthMethod.BasicAuthMethod() else null,
                if (withOAuth) {
                    AuthMethod.OAuthMethod(
                        data = OAuthData(
                            clientId = "",
                            authorizationFlow = AuthorizationFlow.CodePKCE,
                            scope = "",
                            redirectUris = listOf(Url("http://localhost")),
                            authorizationEndpoint = Url("http://localhost"),
                            tokenEndpoint = Url("http://localhost"),
                        ),
                    )
                } else {
                    null
                },
            ),
        ),
    )

    override val model: StateFlow<AuthenticationComponent.Model> = MutableStateFlow(model)

    override val basicAuthSlot: Value<ChildSlot<BasicAuthConfiguration, BasicAuthComponent>> =
        MutableValue(
            if (withBasicAuth) {
                ChildSlot(
                    Child.Created(
                        configuration = BasicAuthConfiguration(),
                        instance = PreviewBasicAuthComponent,
                    ),
                )
            } else {
                ChildSlot()
            },
        )

    override val oAuthSlot: Value<ChildSlot<OAuthConfiguration, OAuthComponent>> =
        MutableValue(
            if (withOAuth) {
                ChildSlot(
                    Child.Created(
                        configuration = OAuthConfiguration(
                            method = model.methods.filterIsInstance<AuthMethod.OAuthMethod>().single(),
                        ),
                        instance = PreviewOAuthComponent,
                    ),
                )
            } else {
                ChildSlot()
            },
        )

    override fun onAbort() = Unit
}

private object PreviewBasicAuthComponent : BasicAuthComponent {
    override val model: StateFlow<BasicAuthComponent.Model> =
        MutableStateFlow(BasicAuthComponent.Model())

    override fun onChangeUsername(username: String) = Unit
    override fun onChangePassword(password: String) = Unit
    override fun onAuthenticate() = Unit
}

private object PreviewOAuthComponent : OAuthComponent {
    override val model: StateFlow<OAuthComponent.Model> =
        MutableStateFlow(OAuthComponent.Model())
    override val labels: Flow<OAuthStore.Label> = MutableSharedFlow()

    override fun onAuthenticate() = Unit
}
