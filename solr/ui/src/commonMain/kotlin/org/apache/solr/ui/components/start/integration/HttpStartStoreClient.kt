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

import io.ktor.client.HttpClient
import io.ktor.client.plugins.timeout
import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import io.ktor.http.URLBuilder
import io.ktor.http.Url
import io.ktor.http.path
import org.apache.solr.ui.components.start.store.StartStoreProvider
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.errors.UnknownResponseException

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
            path("api/node/system")
        }.build()

        val response = httpClient.get(url) {
            timeout { connectTimeoutMillis = 5000 }
        }

        return when (response.status) {
            HttpStatusCode.OK -> Result.success(Unit)
            HttpStatusCode.Unauthorized -> Result.failure(UnauthorizedException())
            else -> Result.failure(UnknownResponseException(response))
        }
    }
}
