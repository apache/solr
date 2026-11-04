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

package org.apache.solr.ui.components.start.domain

import io.ktor.http.URLParserException
import io.ktor.http.parseUrl
import kotlin.coroutines.cancellation.CancellationException
import org.apache.solr.ui.components.start.repository.StartRepository
import org.apache.solr.ui.errors.UnauthorizedException
import org.apache.solr.ui.utils.defaultSolrUrl
import org.apache.solr.ui.utils.parseError

internal class DefaultConnectUseCase(
    private val repository: StartRepository,
) : ConnectUseCase {

    override suspend fun invoke(url: String): ConnectResult {
        val urlValue = url.ifBlank { defaultSolrUrl() }
        val solrUrl = parseUrl(urlValue) ?: return ConnectResult.Failure(
            URLParserException(
                urlString = urlValue,
                cause = Error("Invalid URL"),
            ),
        )

        return try {
            repository.connect(solrUrl).fold(
                // Solr server found with no auth active
                onSuccess = { ConnectResult.Connected(solrUrl) },
                onFailure = { error ->
                    if (error is UnauthorizedException && error.methods.isNotEmpty()) {
                        // Solr server found, but user is unauthorized
                        ConnectResult.AuthRequired(url = solrUrl, methods = error.methods)
                    } else {
                        // Includes Solr servers that responded with an unauthorized error that
                        // cannot be processed (supported method or missing information)
                        ConnectResult.Failure(error)
                    }
                },
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            // error thrown here is platform-specific and needs further parsing
            ConnectResult.Failure(parseError(e))
        }
    }
}
