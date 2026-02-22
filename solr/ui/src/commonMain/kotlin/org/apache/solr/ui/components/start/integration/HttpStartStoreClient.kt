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

package org.apache.solr.ui.components.start.integration

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.HttpClient
import io.ktor.client.plugins.timeout
import io.ktor.client.request.get
import io.ktor.http.Headers
import io.ktor.http.HttpStatusCode
import io.ktor.http.URLBuilder
import io.ktor.http.Url
import io.ktor.http.path
import kotlin.io.encoding.Base64
import kotlinx.serialization.json.Json
import org.apache.solr.ui.components.start.store.StartStoreProvider
import org.apache.solr.ui.data.SolrAuthData
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.errors.UnknownResponseException

private val logger = KotlinLogging.logger {}

/**
 * Client implementation of the [StartStoreProvider.Client] that makes use
 * of a preconfigured HTTP client for accessing the Solr API.
 *
 * @property httpClient HTTP client to use for accessing the API. The client has to be
 * configured with a default request that includes the host, port and schema. The client
 * should also include the necessary authentication data if authentication / authorization
 * is enabled.
 */
class HttpStartStoreClient(
    private val httpClient: HttpClient,
) : StartStoreProvider.Client {

    override suspend fun connect(url: Url): Result<Unit> {
        val url = URLBuilder(url).apply {
            // Try getting system data for checking if Solr host exists,
            // since there is no explicit endpoint for the connection establishment.
            // TODO Consider /api/admin/authentication
            path("api/node/system")
        }.build()

        val response = httpClient.get(url) {
            timeout { connectTimeoutMillis = 5000 }
        }

        return when (response.status) {
            HttpStatusCode.OK -> Result.success(Unit)
            HttpStatusCode.Unauthorized -> {
                val methods = getAuthMethodsFromHeader(response.headers)
                Result.failure(
                    UnauthorizedException(
                        url = url,
                        methods = methods,
                        message =
                        if (methods.isEmpty()) {
                            "Unauthorized response received with missing auth methods."
                        } else {
                            null
                        },
                    ),
                )
            }

            else -> Result.failure(UnknownResponseException(response))
        }
    }

    /**
     * Extracts the authentication methods from a response's headers and returns
     * a list of [AuthMethod]s.
     *
     * @param headers The headers to use for extracting the information.
     */
    private fun getAuthMethodsFromHeader(headers: Headers): List<AuthMethod> {
        // Note that on JVM headers.getAll() may return multiple values, whereas in WebAssembly/JS
        // it may merge multiple headers and separate them by comma
        val authHeaders = headers.getAll("Www-Authenticate") ?: emptyList()

        return authHeaders
            // Split by comma, as there is the chance that headers will be merged and separated by
            // comma (e.g. on web target)
            .flatMap { header -> header.split(",") }
            .map(String::trim)
            .mapIndexed { index, authHeader ->
                val (scheme, params) = parseWwwAuthenticate(authHeader)

                when (scheme.lowercase()) {
                    "basic", "xbasic" -> AuthMethod.BasicAuthMethod(realm = params["realm"])
                    "bearer" -> {
                        // There should be one X-Solr-Authdata header for each scheme
                        val base64AuthData = headers.getAll("X-Solr-Authdata")
                            // Note that there is a chance the header values are merged again
                            // and separated by comma, so we split again
                            ?.flatMap { headerValue -> headerValue.split(",") }
                            ?.getOrNull(index)

                        base64AuthData?.let {
                            val rawAuthData = Base64.decode(source = it)
                            val jsonAuthData = Json.decodeFromString<SolrAuthData>(rawAuthData.decodeToString())

                            AuthMethod.OAuthMethod(
                                data = jsonAuthData.toOAuthData(),
                                realm = params["realm"],
                            )
                        } ?: run {
                            // Without X-Solr-Authdata value we cannot authenticate with OAuth
                            logger.error {
                                "Missing X-Solr-Authdata for authentication scheme \"${scheme}\"."
                            }
                            AuthMethod.Unknown
                        }
                    }
                    else -> AuthMethod.Unknown
                }
            }
    }

    private fun parseWwwAuthenticate(headerValue: String): Pair<String, Map<String, String>> {
        val parts = headerValue.split(" ", limit = 2)
        val scheme = parts[0]

        // The below mapping is not supporting commas or spaces in the parameter values, nor
        // parameters separated by commas (only spaces)
        val params = parts.getOrNull(1)
            ?.split(" ")
            ?.map(String::trim)
            ?.associate {
                val (k, v) = it.split("=", limit = 2)
                k to v.trim('"')
            }
            ?: emptyMap()
        return scheme to params
    }
}
