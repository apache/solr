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
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.parameters
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.request.receiveParameters
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.coroutines.CompletableDeferred
import org.apache.solr.ui.components.auth.getRedirectUri
import org.apache.solr.ui.components.auth.store.OAuthStoreProvider
import org.apache.solr.ui.data.AuthorizationResponse
import org.apache.solr.ui.domain.OAuthData
import org.apache.solr.ui.errors.InvalidResponseException
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.errors.UnknownResponseException

/**
 * OAuth store implementation that uses a server instance for handling callbacks.
 *
 * The flow on desktop looks as follows:
 * 1. The application launches a server that listens on 127.0.0.1:8088
 * 2. The application opens the identity provider page in the browser
 * 3. After the user authenticates and authorizes the app, the identity provider redirects the user
 *    to the server that was launched by the app
 * 4. The server evaluates the query parameters passed with the redirect and generates a new token.
 *
 * There is a special handling for redirects that use the hash fragment instead of query parameters.
 * In that case, the server responds with a script that reloads the page with the hash fragment as
 * query parameters.
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
        val queryParams = listenForOAuthCallback()

        val code =
            queryParams["code"] ?: throw InvalidResponseException("code not retrieved but required")
        val responseState = queryParams["state"]
            ?: throw InvalidResponseException("state not retrieved but required")
        if (state != responseState) {
            return Result.failure(
                exception = InvalidResponseException(message = "Invalid state value received"),
            )
        }

        // TODO Add check for valid redirect URIs to make sure the current is fine
        val authResponse = httpClient.submitForm(
            url = data.tokenEndpoint.toString(),
            formParameters = parameters {
                append("grant_type", "authorization_code")
                append("code", code)
                append("redirect_uri", getRedirectUri())
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
     * Starts an embedded Ktor server on 127.0.0.1:3000 and waits for a single OAuth callback.
     *
     * Returns a map of captured parameters (e.g. "code", "state", or "access_token").
     *
     * The function suspends until the callback is received, then the server is stopped and the captured
     * parameters are returned.
     */
    private suspend fun listenForOAuthCallback(
        port: Int = 8088,
        host: String = "127.0.0.1",
        path: String = "/callback",
        timeoutMillis: Long = 120_000L,
    ): Map<String, String> {
        val result = CompletableDeferred<Map<String, String>>()

        val server = embeddedServer(CIO, host = host, port = port) {
            routing {
                get(path) {
                    val queryParams = call.request.queryParameters
                    val code = queryParams["code"]
                    val state = queryParams["state"]
                    // If query contains code or access_token, capture and finish
                    // val hasQueryToken = queryParams["code"] != null || queryParams["access_token"] != null || queryParams.names().isNotEmpty()
                    val hasQueryToken = code != null && state != null

                    if (hasQueryToken) {
                        // collect params into a map
                        val map = queryParams.names().associateWith { queryParams[it].orEmpty() }
                        // respond user-friendly page
                        call.respondText(
                            """
                        <!doctype html>
                        <html>
                          <head>
                            <meta charset="utf-8"/>
                            <title>Authentication Complete</title>
                            <meta name="viewport" content="width=device-width,initial-scale=1" />
                          </head>
                          <body>
                            <h2>Authentication complete</h2>
                            <p>You can now return to the application. You may close this window.</p>
                          </body>
                        </html>
                            """.trimIndent(),
                            ContentType.Text.Html,
                        )
                        // complete and stop the server
                        result.complete(map)
                    } else {
                        // No query parameters — maybe the provider returned a fragment (#access_token=...)
                        // Serve JS that converts the fragment into a query string and reloads the page.
                        call.respondText(
                            """
                        <!doctype html>
                        <html>
                          <head>
                            <meta charset="utf-8"/>
                            <title>Completing authentication...</title>
                            <meta name="viewport" content="width=device-width,initial-scale=1" />
                          </head>
                          <body>
                            <p>Completing authentication — please wait...</p>
                            <script>
                              // If there's a hash fragment, transform it into a query and reload:
                              (function() {
                                if (window.location.hash && window.location.hash.length > 1) {
                                  // Replace leading '#' with '?'
                                  const q = window.location.hash.replace(/^#/, '?');
                                  // Replace the current URL with the same path plus query (this sends it to the server)
                                  window.location.replace(window.location.pathname + q);
                                } else {
                                  document.body.innerHTML = '<p>No token found in URL. Please return to the app and try again.</p>';
                                }
                              })();
                            </script>
                          </body>
                        </html>
                            """.trimIndent(),
                            ContentType.Text.Html,
                        )
                    }
                }

                // Optional: provide a POST endpoint if you prefer the JS to post fragment via fetch
                post("/__callback_post") {
                    val params = call.receiveParameters()
                    val map = params.names().associateWith { params[it].orEmpty() }
                    call.respondText("OK", ContentType.Text.Plain)
                    if (!result.isCompleted) result.complete(map)
                }
            }
        }

        server.start(wait = false)

        // Wait until callback is received
        val captured = result.await()

        // shutdown server gracefully
        server.stop(gracePeriodMillis = 1000, timeoutMillis = 2000)
        return captured
    }
}
