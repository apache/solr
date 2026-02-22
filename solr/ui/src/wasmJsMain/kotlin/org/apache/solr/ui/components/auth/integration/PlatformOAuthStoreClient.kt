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

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.client.plugins.timeout
import io.ktor.client.request.bearerAuth
import io.ktor.client.request.forms.submitForm
import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import io.ktor.http.Url
import io.ktor.http.parameters
import kotlin.js.unsafeCast
import kotlinx.browser.window
import kotlinx.coroutines.suspendCancellableCoroutine
import org.apache.solr.ui.components.auth.getRedirectUri
import org.apache.solr.ui.components.auth.store.OAuthStoreProvider
import org.apache.solr.ui.data.AuthorizationResponse
import org.apache.solr.ui.domain.OAuthData
import org.apache.solr.ui.errors.InvalidResponseException
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.errors.UnknownResponseException
import org.w3c.dom.MessageEvent

private val logger = KotlinLogging.logger {}

/**
 * OAuth store implementation that uses event listener and postMessage for handling callbacks
 * from the identity provider after authentication / authorization.
 *
 * The basic flow on web looks as follows:
 * 1. User selects authentication with OAuth
 * 2. The current tab starts listening for messages
 * 2. A new tab is opened that opens the identity provider authentication / authorization page.
 * 3. The identity provide redirects in the new tab back to the app
 * 4. The new app instance checks the URL data and publishes the redirect data via postMessage
 * 5. The new tab is closed once the data is posted
 * 6. The initial app that was listening retrieves the data and proceeds with the authentication
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

        val code =
            queryParams["code"] ?: throw InvalidResponseException("code not retrieved but required")
        val responseState = queryParams["state"]
            ?: throw InvalidResponseException("state not retrieved but required")
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
                append("redirect_uri", pickRedirectUri(data.redirectUris))
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
     * Retrieves the query params from the current page.
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
                        } else {
                            null
                        }
                    }.toMap()

                continuation.resumeWith(Result.success(params))
            })
        }
    }

    /**
     * Picks a redirect URI based on provided redirect URIs and some criteria.
     *
     * The current criteria requires the redirect URI to end with "callback" (last path segment),
     * and to be of the same origin. If not all criteria are met for any of the provided
     * [redirectUris], a fallback URI is used.
     *
     * @param redirectUris A list of redirect URIs to try to use.
     * @return One of the [redirectUris] that match the criteria, or a fallback URI.
     */
    private fun pickRedirectUri(redirectUris: List<Url>): String = redirectUris.find {
        // We have in the Main.kt for web a handler that looks up for the last path segment
        // to send a postMessage, so that it can be captured here
        it.rawSegments.last() == "callback" &&
            // Add also current window's origin as filter
            it.toString().contains(window.location.origin)
    }?.toString() ?: run {
        // fallback to default URI and log non-matching redirect URIs
        logger.warn {
            "No matching redirect URI found that is of the same origin and ends with callback"
        }
        getRedirectUri()
    }
}
