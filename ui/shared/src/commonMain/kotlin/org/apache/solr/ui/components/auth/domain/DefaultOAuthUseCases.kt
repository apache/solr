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

package org.apache.solr.ui.components.auth.domain

import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.http.ParametersBuilder
import io.ktor.http.URLBuilder
import org.apache.solr.ui.components.auth.generateCodeChallenge
import org.apache.solr.ui.components.auth.generateCodeVerifier
import org.apache.solr.ui.components.auth.generateOAuthState
import org.apache.solr.ui.components.auth.getRedirectUri
import org.apache.solr.ui.components.auth.repository.OAuthRepository
import org.apache.solr.ui.domain.AuthMethod

internal class DefaultCreateOAuthAuthorizationUseCase : CreateOAuthAuthorizationUseCase {

    override fun invoke(method: AuthMethod.OAuthMethod): OAuthAuthorization {
        val verifier = generateCodeVerifier()
        val challenge = generateCodeChallenge(verifier)
        val state = generateOAuthState()
        val url = URLBuilder(method.data.authorizationEndpoint)
            .apply {
                encodedParameters = ParametersBuilder().apply {
                    set("client_id", method.data.clientId)
                    set("scope", method.data.scope)
                    set("redirect_uri", getRedirectUri())
                    set("response_type", "code")
                    set("code_challenge_method", "S256")
                    set("code_challenge", challenge)
                    set("state", state)
                }
            }
            .build()

        return OAuthAuthorization(url = url, state = state, verifier = verifier)
    }
}

internal class DefaultOAuthAuthenticateUseCase(
    private val repository: OAuthRepository,
) : OAuthAuthenticateUseCase {

    override suspend fun invoke(
        authorization: OAuthAuthorization,
        method: AuthMethod.OAuthMethod,
    ): Result<BearerTokens> = authenticationCatching {
        repository.authenticate(authorization.state, authorization.verifier, method.data)
    }
}
