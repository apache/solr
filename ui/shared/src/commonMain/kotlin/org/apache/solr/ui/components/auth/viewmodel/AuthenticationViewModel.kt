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

package org.apache.solr.ui.components.auth.viewmodel

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import io.ktor.http.Url
import org.apache.solr.ui.components.auth.domain.BasicAuthenticateUseCase
import org.apache.solr.ui.components.auth.domain.CreateOAuthAuthorizationUseCase
import org.apache.solr.ui.components.auth.domain.OAuthAuthenticateUseCase
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.utils.AppDispatchers

class AuthenticationViewModel(
    url: Url,
    methods: List<AuthMethod>,
    basicAuthenticateUseCase: BasicAuthenticateUseCase,
    createOAuthAuthorizationUseCase: CreateOAuthAuthorizationUseCase,
    oAuthAuthenticateUseCase: OAuthAuthenticateUseCase,
    dispatchers: AppDispatchers,
) : ViewModel() {

    private val authenticationState = AuthenticationStateHolder(
        scope = viewModelScope,
        url = url,
        methods = methods,
        basicAuthenticateUseCase = basicAuthenticateUseCase,
        createOAuthAuthorizationUseCase = createOAuthAuthorizationUseCase,
        oAuthAuthenticateUseCase = oAuthAuthenticateUseCase,
        dispatchers = dispatchers,
    )

    /**
     * UI state of the authentication screen.
     */
    val uiState = authenticationState.uiState

    /**
     * Events emitted by the authentication screen.
     */
    val events = authenticationState.events

    /**
     * URLs of the identity provider that are supposed to be opened for authorizing the user.
     */
    val authorizationUrls = authenticationState.authorizationUrls

    /**
     * State holder for authentication with credentials (basic auth), if it is supported.
     */
    val basicAuth = authenticationState.basicAuth

    /**
     * State holder for authentication with OAuth (bearer token), if it is supported.
     */
    val oAuth = authenticationState.oAuth

    /**
     * Aborts the authentication attempt.
     */
    fun abort() = authenticationState.abort()
}
