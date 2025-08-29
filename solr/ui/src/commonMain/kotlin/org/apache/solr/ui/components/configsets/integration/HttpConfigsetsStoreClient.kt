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

package org.apache.solr.ui.components.configsets.integration

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.http.isSuccess
import org.apache.solr.ui.components.configsets.data.ListConfigsets
import org.apache.solr.ui.components.configsets.store.ConfigsetsStoreProvider

/**
 * Client implementation of the [ConfigsetsStoreProvider.Client] that makes use
 * of a preconfigured HTTP client for accessing the Solr API.
 *
 * @property httpClient HTTP client to use for accessing the API. The client has to be
 * configured with a default request that includes the host, port and schema. The client
 * should also include the necessary authentication data if authentication / authorization
 * is enabled.
 */
class HttpConfigsetsStoreClient(
    private val httpClient: HttpClient,
) : ConfigsetsStoreProvider.Client {
    override suspend fun fetchConfigSets(): Result<ListConfigsets> {
        val response = httpClient.get("api/configsets")
        return when {
            response.status.isSuccess() -> Result.success(response.body())
            else -> Result.failure(Exception("Unknown Error"))
            // TODO Add proper error handling
        }
    }
}
