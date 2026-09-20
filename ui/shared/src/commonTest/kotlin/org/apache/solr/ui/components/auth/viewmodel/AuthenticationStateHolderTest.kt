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

package org.apache.solr.ui.components.auth.viewmodel

import io.ktor.client.HttpClient
import io.ktor.http.Url
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.apache.solr.ui.TestDispatchers
import org.apache.solr.ui.components.auth.data.HttpBasicAuthRepository
import org.apache.solr.ui.components.auth.domain.DefaultBasicAuthenticateUseCase
import org.apache.solr.ui.components.auth.domain.DefaultCreateOAuthAuthorizationUseCase
import org.apache.solr.ui.components.auth.domain.DefaultOAuthAuthenticateUseCase
import org.apache.solr.ui.domain.AuthMethod

@OptIn(ExperimentalCoroutinesApi::class)
class AuthenticationStateHolderTest {

    @Test
    fun `GIVEN basic auth method THEN basicAuth populated`() = runTest {
        val stateHolder = createStateHolder(methods = listOf(AuthMethod.BasicAuthMethod(realm = "solr")))
        advanceUntilIdle()

        assertNotNull(stateHolder.basicAuth)
    }

    /**
     * Helper function for creating an instance of the [AuthenticationStateHolder].
     */
    private fun TestScope.createStateHolder(
        httpClient: HttpClient = HttpClient(),
        urlString: String = "",
        methods: List<AuthMethod> = emptyList(),
    ) = AuthenticationStateHolder(
        scope = backgroundScope,
        url = Url(urlString),
        methods = methods,
        basicAuthenticateUseCase = DefaultBasicAuthenticateUseCase(HttpBasicAuthRepository(httpClient)),
        createOAuthAuthorizationUseCase = DefaultCreateOAuthAuthorizationUseCase(),
        oAuthAuthenticateUseCase = DefaultOAuthAuthenticateUseCase(FakeOAuthRepository { Result.failure(Exception()) }),
        dispatchers = TestDispatchers(UnconfinedTestDispatcher(scheduler = testScheduler)),
    )
}
