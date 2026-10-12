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

package org.apache.solr.ui.components.auth.store

import io.ktor.client.plugins.auth.providers.BearerTokens
import org.apache.solr.ui.domain.OAuthData

/**
 * OAuth client for mocking the OAuth server responses.
 */
internal class FakeOAuthClient(
    private val result: suspend () -> Result<BearerTokens>,
) : OAuthStoreProvider.Client {

    var lastState: String? = null
    var lastVerifier: String? = null
    var lastData: OAuthData? = null

    override suspend fun authenticate(
        state: String,
        verifier: String,
        data: OAuthData,
    ): Result<BearerTokens> {
        lastState = state
        lastVerifier = verifier
        lastData = data
        return result()
    }
}
