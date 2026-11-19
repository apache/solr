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

package org.apache.solr.ui.components.start.data

import io.ktor.http.Url
import org.apache.solr.ui.data.SolrAuthData
import org.apache.solr.ui.domain.AuthorizationFlow
import org.apache.solr.ui.domain.OAuthData

internal fun SolrAuthData.toOAuthData() = OAuthData(
    clientId = clientId,
    authorizationFlow = authorizationFlow.toAuthorizationFlow(),
    scope = scope,
    redirectUris = redirectUris.map { Url(urlString = it) },
    authorizationEndpoint = Url(urlString = authorizationEndpoint),
    tokenEndpoint = Url(urlString = tokenEndpoint),
)

/**
 * Maps a string to the corresponding AuthorizationFlow enum value.
 *
 * Note that only Code flow with Proof Key for Code Exchange (PKCE) is supported right now.
 */
private fun String.toAuthorizationFlow(): AuthorizationFlow = when (this) {
    "code_pkce" -> AuthorizationFlow.CodePKCE
    else -> AuthorizationFlow.Unknown
}
