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

import io.ktor.http.Url
import org.apache.solr.ui.domain.AuthMethod

/**
 * Use case for connecting to a Solr instance.
 */
interface ConnectUseCase {

    /**
     * Tries to connect to the Solr instance at the given [url].
     *
     * @param url The URL of the Solr instance. If blank, the default Solr URL is used.
     * @return The result of the connection attempt.
     */
    suspend operator fun invoke(url: String): ConnectResult
}

/**
 * Result of a connection attempt.
 */
sealed interface ConnectResult {

    /**
     * The connection was established and no authentication is required.
     *
     * @property url URL of the Solr instance.
     */
    data class Connected(val url: Url) : ConnectResult

    /**
     * A Solr server was found, but authentication is required.
     *
     * @property url URL of the Solr instance that requires authentication.
     * @property methods The supported authentication methods.
     */
    data class AuthRequired(val url: Url, val methods: List<AuthMethod>) : ConnectResult

    /**
     * The connection could not be established.
     *
     * @property error The error that occurred.
     */
    data class Failure(val error: Throwable) : ConnectResult
}
