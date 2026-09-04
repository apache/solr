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

package org.apache.solr.ui.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class AuthorizationResponse(
    @SerialName("access_token")
    val accessToken: String = "",
    @SerialName("expires_in")
    val expiresIn: Long = 0,
    @SerialName("refresh_expires_in")
    val refreshExpiresIn: Long = 0,
    @SerialName("refresh_token")
    val refreshToken: String = "",
    @SerialName("token_type")
    val tokenType: String = "",
    @SerialName("id_token")
    val idToken: String? = null,
    @SerialName("not-before-policy")
    val notBeforePolicy: Int = -1,
    @SerialName("session_state")
    val sessionState: String = "",
    val scope: String = "",
)
