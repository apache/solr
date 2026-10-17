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
 * Events emitted by the start screen that are meant to be handled by the parent.
 */
sealed interface StartEvent {

    /**
     * Emitted when a connection to a Solr instance has been
     * established and no authentication is required.
     *
     * @property url The URL the connection was established.
     */
    data class Connected(val url: Url) : StartEvent

    /**
     * Emitted when a connection to a Solr instance has been established and authentication
     * is needed.
     *
     * @property url The URL the connection was established but requires authentication.
     * @property methods List of authentication methods that can be used for authenticating.
     */
    data class AuthRequired(val url: Url, val methods: List<AuthMethod>) : StartEvent
}
