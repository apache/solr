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
import io.ktor.client.request.forms.submitForm
import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import io.ktor.http.parameters
import kotlin.js.unsafeCast
import kotlinx.browser.window
import kotlinx.coroutines.suspendCancellableCoroutine
import org.apache.solr.ui.components.auth.store.OAuthStoreProvider
import org.apache.solr.ui.data.AuthorizationResponse
import org.apache.solr.ui.domain.OAuthData
import org.apache.solr.ui.errors.InvalidResponseException
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.errors.UnknownResponseException
import org.w3c.dom.MessageEvent

/**
 * OAuth store implementation that uses a server instance for handling callbacks.
 *
 * @property httpClient A preconfigured HTTP client that has the base URL of a Solr instance
 * already set.
 */
actual class PlatformOAuthStoreClient actual constructor(private val httpClient: HttpClient) : OAuthStoreProvider.Client {

    actual override suspend fun authenticate(
        state: String,
        verifier: String,
        data: OAuthData,
    ): Result<BearerTokens> {
        val queryParams = getQueryParams()

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
                append("redirect_uri", "http://127.0.0.1:8983/solr/ui/callback")
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

    /**
     * Retrieves the query params from the current page
     */
    private suspend fun getQueryParams(): Map<String, String> {
        return suspendCancellableCoroutine { continuation ->
            window.addEventListener("message", { event ->
                event as MessageEvent
                if (event.origin != window.origin) return@addEventListener // security check

                val params = event.data?.unsafeCast<JsString>().toString()
                    .substring(1) // remove leading ?
                    .split("&") // split params
                    .mapNotNull { param ->
                        // Map params key-value pairs
                        val parts = param.split("=")
                        if (parts.size == 2) {
                            parts[0] to parts[1]
                        } else null
                    }.toMap()

                continuation.resumeWith(Result.success(params))
            })
        }
    }
}
