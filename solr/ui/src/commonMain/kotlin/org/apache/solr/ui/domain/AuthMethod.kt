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

import kotlinx.serialization.Serializable

/**
 * The authentication method is used for determining what methods of authenticating are available.
 * These methods hold information for displaying and trying to authenticate.
 *
 * @see AuthOption
 */
@Serializable
sealed interface AuthMethod {

    /**
     * Basic authentication method that uses username and password for
     * authenticating.
     *
     * @property realm The realm of the basic auth.
     */
    @Serializable
    data class BasicAuthMethod(val realm: String = "") : AuthMethod
}
