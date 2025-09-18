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
import io.ktor.client.plugins.timeout
import io.ktor.client.request.basicAuth
import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import org.apache.solr.ui.components.auth.store.BasicAuthStoreProvider
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.errors.UnknownResponseException

/**
 * HTTP client implementation of the basic auth store.
 *
 * @property httpClient A preconfigured HTTP client that has the base URL of a Solr instance
 * already set.
 */
class HttpBasicAuthStoreClient(private val httpClient: HttpClient) : BasicAuthStoreProvider.Client {
    override suspend fun authenticate(
        username: String,
        password: String,
    ): Result<Unit> {
        // Use the same path that was used for the connection establishment that returned 401.
        val response = httpClient.get("api/node/system") {
            timeout { connectTimeoutMillis = 5000 }
            basicAuth(username, password)
        }

        return when (response.status) {
            HttpStatusCode.OK -> Result.success(Unit)
            HttpStatusCode.Unauthorized -> {
                Result.failure(UnauthorizedException(message = "Invalid Credentials"))
            }

            else -> Result.failure(UnknownResponseException(response))
        }
    }
}
