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
import io.ktor.client.call.body
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.client.plugins.timeout
import io.ktor.client.request.bearerAuth
import io.ktor.client.request.forms.formData
import io.ktor.client.request.forms.submitForm
import io.ktor.client.request.get
import io.ktor.client.request.parameter
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.http.parameters
import kotlinx.serialization.json.Json
import org.apache.solr.ui.components.auth.listenForOAuthCallback
import org.apache.solr.ui.components.auth.store.OAuthStoreProvider
import org.apache.solr.ui.data.AuthorizationResponse
import org.apache.solr.ui.data.AuthorizationTokenRequest
import org.apache.solr.ui.domain.OAuthData
import org.apache.solr.ui.errors.InvalidResponseException
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.errors.UnknownResponseException

/**
 * OAuth store implementation that uses a server instance for handling callbacks.
 *
 * @property httpClient A preconfigured HTTP client that has the base URL of a Solr instance
 * already set.
 */
class ServerOAuthStoreClient(private val httpClient: HttpClient) : OAuthStoreProvider.Client {
    override suspend fun authenticate(
        state: String,
        verifier: String,
        data: OAuthData,
    ): Result<BearerTokens> {
        val queryParams = listenForOAuthCallback()

        val code = queryParams["code"] ?: throw InvalidResponseException("code not retrieved but required")
        val responseState = queryParams["state"] ?: throw InvalidResponseException("state not retrieved but required")
        if (state != responseState) {
            return Result.failure(
                exception = InvalidResponseException(message = "Invalid state value received"),
            )
        }

        val authResponse = httpClient.submitForm(
            url = data.tokenEndpoint.toString(),
            formParameters = parameters {
                append("grant_type", "authorization_code")
                append("code", code)
                append("redirect_uri", "http://127.0.0.1:8088/callback")
                append("scope", data.scope)
                append("code_verifier", verifier)
                append("client_id", data.clientId)
            },
        ).body<AuthorizationResponse>()

        val accessToken = authResponse.accessToken
        val refreshToken = authResponse.refreshToken

        val response = httpClient.get("api/node/system") {
            timeout { connectTimeoutMillis = 5000 }
            bearerAuth(token = accessToken)
        }

        return when (response.status) {
            HttpStatusCode.OK -> Result.success(BearerTokens(accessToken, refreshToken))
            HttpStatusCode.Unauthorized -> {
                Result.failure(UnauthorizedException(message = "Invalid Credentials"))
            }

            else -> Result.failure(UnknownResponseException(response))
        }
    }
}
