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
import io.ktor.http.Url
import org.apache.solr.ui.domain.AuthMethod

/**
 * An OAuth authorization request that uses the authorization code flow with PKCE.
 *
 * @property url The URL of the identity provider the user has to open for authorizing the app.
 * @property state The state value used in the authorization flow.
 * @property verifier The code verifier used in the authorization flow.
 */
data class OAuthAuthorization(
    val url: Url,
    val state: String,
    val verifier: String,
)

/**
 * Use case for preparing an OAuth authorization request.
 */
interface CreateOAuthAuthorizationUseCase {

    /**
     * Creates a new authorization request for the given [method].
     *
     * @param method The OAuth method to create the authorization request for.
     */
    operator fun invoke(method: AuthMethod.OAuthMethod): OAuthAuthorization
}

/**
 * Use case for authenticating with OAuth.
 */
interface OAuthAuthenticateUseCase {

    /**
     * Waits for the user to authorize the [authorization] request and exchanges the result for
     * tokens.
     *
     * @param authorization The authorization request the user is expected to authorize.
     * @param method The OAuth method the authorization request was created for.
     * @return The tokens if the user has successfully authenticated. Otherwise, a failure with the
     * error that occurred, where invalid credentials are reported as
     * [org.apache.solr.ui.errors.InvalidCredentialsException].
     */
    suspend operator fun invoke(
        authorization: OAuthAuthorization,
        method: AuthMethod.OAuthMethod,
    ): Result<BearerTokens>
}
