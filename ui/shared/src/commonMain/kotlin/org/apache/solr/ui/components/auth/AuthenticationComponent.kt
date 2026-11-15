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

package org.apache.solr.ui.components.auth

import org.apache.solr.ui.components.auth.domain.BasicAuthenticateUseCase
import org.apache.solr.ui.components.auth.domain.CreateOAuthAuthorizationUseCase
import org.apache.solr.ui.components.auth.domain.OAuthAuthenticateUseCase
import org.apache.solr.ui.components.auth.repository.BasicAuthRepository
import org.apache.solr.ui.components.auth.repository.OAuthRepository
import org.apache.solr.ui.components.auth.viewmodel.AuthenticationViewModel

/**
 * The authentication component takes care of the authentication processes. This typically includes
 * user authentication with credentials, tokens or certificates.
 *
 * Note that Solr does not support multiple authentication methods of the same type with the
 * MultiAuthPlugin, so there will be always up to one authentication option per method.
 */
interface AuthenticationComponent {

    /**
     * Dependencies provided by the application.
     */
    val basicAuthRepository: BasicAuthRepository

    /**
     * Dependencies provided by the application.
     */
    val oAuthRepository: OAuthRepository

    /**
     * Use case responsible for authenticating with credentials (basic auth).
     */
    val basicAuthenticateUseCase: BasicAuthenticateUseCase

    /**
     * Use case responsible for creating OAuth authorization requests.
     */
    val createOAuthAuthorizationUseCase: CreateOAuthAuthorizationUseCase

    /**
     * Use case responsible for authenticating with OAuth.
     */
    val oAuthAuthenticateUseCase: OAuthAuthenticateUseCase

    /**
     * Factory method to create a [AuthenticationViewModel] instance.
     */
    fun createAuthenticationViewModel(): AuthenticationViewModel
}
