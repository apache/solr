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

package org.apache.solr.ui.components.auth.integration

import com.arkivanov.mvikotlin.core.instancekeeper.getStore
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.labels
import com.arkivanov.mvikotlin.extensions.coroutines.stateFlow
import io.ktor.client.HttpClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import org.apache.solr.ui.components.auth.OAuthComponent
import org.apache.solr.ui.components.auth.OAuthComponent.Output
import org.apache.solr.ui.components.auth.store.OAuthStore
import org.apache.solr.ui.components.auth.store.OAuthStore.Intent
import org.apache.solr.ui.components.auth.store.OAuthStoreProvider
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.coroutineScope
import org.apache.solr.ui.utils.map

class DefaultOAuthComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    method: AuthMethod.OAuthMethod,
    private val output: (Output) -> Unit,
) : OAuthComponent,
    AppComponentContext by componentContext {

    private val mainScope = coroutineScope(SupervisorJob() + mainContext)
    private val ioScope = coroutineScope(SupervisorJob() + ioContext)

    private val store = instanceKeeper.getStore {
        OAuthStoreProvider(
            storeFactory = storeFactory,
            client = PlatformOAuthStoreClient(httpClient),
            mainContext = mainScope.coroutineContext,
            ioContext = ioScope.coroutineContext,
            method = method,
        ).provide()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override val model = store.stateFlow.map(mainScope, stateToOAuthModel)

    override val labels: Flow<OAuthStore.Label> = store.labels

    init {
        store.labels.onEach { label ->
            when (label) {
                is OAuthStore.Label.Authenticated -> output(
                    Output.Authenticated(
                        method = label.method,
                        accessToken = label.accessToken,
                        refreshToken = label.refreshToken,
                    ),
                )

                is OAuthStore.Label.AuthenticationFailed -> output(
                    Output.AuthenticationFailed(error = label.error),
                )

                is OAuthStore.Label.AuthenticationStarted -> {
                    output(Output.Authenticating)
                }
                is OAuthStore.Label.ErrorReset -> output(Output.ErrorReset)
            }
        }.launchIn(mainScope)
    }

    override fun onAuthenticate() = store.accept(Intent.Authenticate)
}
