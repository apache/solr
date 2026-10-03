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

package org.apache.solr.ui.components.auth.integration

import io.ktor.client.HttpClient
import io.ktor.http.Url
import org.apache.solr.ui.components.auth.AuthenticationComponent
import org.apache.solr.ui.components.auth.data.HttpBasicAuthRepository
import org.apache.solr.ui.components.auth.data.PlatformOAuthRepository
import org.apache.solr.ui.components.auth.domain.BasicAuthenticateUseCase
import org.apache.solr.ui.components.auth.domain.CreateOAuthAuthorizationUseCase
import org.apache.solr.ui.components.auth.domain.DefaultBasicAuthenticateUseCase
import org.apache.solr.ui.components.auth.domain.DefaultCreateOAuthAuthorizationUseCase
import org.apache.solr.ui.components.auth.domain.DefaultOAuthAuthenticateUseCase
import org.apache.solr.ui.components.auth.domain.OAuthAuthenticateUseCase
import org.apache.solr.ui.components.auth.repository.BasicAuthRepository
import org.apache.solr.ui.components.auth.repository.OAuthRepository
import org.apache.solr.ui.components.auth.viewmodel.AuthenticationViewModel
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.utils.AppDispatchers
import org.apache.solr.ui.utils.platformDispatchers

/**
 * Default implementation of the [AuthenticationComponent].
 *
 * @param httpClient HTTP client to use for API interactions. The client has to be pre-configured
 * with a base URL pointing to a Solr instance.
 * @param url The URL of the Solr instance the user is authenticating against.
 * @param methods A list of authentication methods that are supported by the Solr instance the
 * [httpClient] is pointing to.
 */
class DefaultAuthenticationComponent(
    httpClient: HttpClient,
    private val url: Url,
    private val methods: List<AuthMethod>,
    private val dispatchers: AppDispatchers = platformDispatchers(),
) : AuthenticationComponent {

    override val basicAuthRepository: BasicAuthRepository by lazy {
        HttpBasicAuthRepository(httpClient)
    }

    override val oAuthRepository: OAuthRepository by lazy {
        PlatformOAuthRepository(httpClient)
    }

    override val basicAuthenticateUseCase: BasicAuthenticateUseCase by lazy {
        DefaultBasicAuthenticateUseCase(basicAuthRepository)
    }

    override val createOAuthAuthorizationUseCase: CreateOAuthAuthorizationUseCase by lazy {
        DefaultCreateOAuthAuthorizationUseCase()
    }

    override val oAuthAuthenticateUseCase: OAuthAuthenticateUseCase by lazy {
        DefaultOAuthAuthenticateUseCase(oAuthRepository)
    }

    override fun createAuthenticationViewModel(): AuthenticationViewModel = AuthenticationViewModel(
        url = url,
        methods = methods,
        basicAuthenticateUseCase = basicAuthenticateUseCase,
        createOAuthAuthorizationUseCase = createOAuthAuthorizationUseCase,
        oAuthAuthenticateUseCase = oAuthAuthenticateUseCase,
        dispatchers = dispatchers,
    )
}
