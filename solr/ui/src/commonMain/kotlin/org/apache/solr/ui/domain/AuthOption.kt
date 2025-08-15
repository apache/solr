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

package org.apache.solr.ui.domain

import io.ktor.http.Url
import kotlinx.serialization.Serializable

/**
 * Once the user authenticates, an authentication option can be created that holds all the necessary
 * information of a successful authentication.
 */
@Serializable
sealed interface AuthOption {

    /**
     * Authentication option that does not require anything (no authentication enabled).
     *
     * @property url The URL of the Solr instance that does not require any authentication.
     */
    @Serializable
    data class None(val url: Url) : AuthOption

    /**
     * Authentication option for authenticating with basic auth (credentials).
     *
     * @property method The basic auth method used for authentication (holds metadata).
     * @property url The URL of the instance that requires basic auth and the credentials of this
     * instance are for.
     * @property username The username to use for further authenticated requests.
     * @property password The password to use for further authenticated requests.
     */
    @Serializable
    data class BasicAuthOption(
        val method: AuthMethod.BasicAuthMethod,
        val url: Url,
        val username: String,
        val password: String,
    ) : AuthOption
}
