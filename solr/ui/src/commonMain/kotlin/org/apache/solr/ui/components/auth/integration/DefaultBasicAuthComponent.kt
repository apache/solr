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
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import org.apache.solr.ui.components.auth.BasicAuthComponent
import org.apache.solr.ui.components.auth.BasicAuthComponent.Output
import org.apache.solr.ui.components.auth.store.BasicAuthStore
import org.apache.solr.ui.components.auth.store.BasicAuthStore.Intent
import org.apache.solr.ui.components.auth.store.BasicAuthStoreProvider
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.coroutineScope
import org.apache.solr.ui.utils.map

class DefaultBasicAuthComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    method: AuthMethod.BasicAuthMethod,
    private val output: (Output) -> Unit,
) : BasicAuthComponent,
    AppComponentContext by componentContext {

    private val mainScope = coroutineScope(SupervisorJob() + mainContext)
    private val ioScope = coroutineScope(SupervisorJob() + ioContext)

    private val store = instanceKeeper.getStore {
        BasicAuthStoreProvider(
            storeFactory = storeFactory,
            client = HttpBasicAuthStoreClient(httpClient),
            mainContext = mainScope.coroutineContext,
            ioContext = ioScope.coroutineContext,
            method = method,
        ).provide()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override val model = store.stateFlow.map(mainScope, stateToBasicAuthModel)

    init {
        store.labels.onEach { label ->
            when (label) {
                is BasicAuthStore.Label.Authenticated -> output(
                    Output.Authenticated(
                        method = label.method,
                        username = label.username,
                        password = label.password,
                    ),
                )

                is BasicAuthStore.Label.AuthenticationFailed -> output(
                    Output.AuthenticationFailed(error = label.error),
                )

                is BasicAuthStore.Label.AuthenticationStarted -> output(Output.Authenticating)
                is BasicAuthStore.Label.ErrorReset -> output(Output.ErrorReset)
            }
        }.launchIn(mainScope)
    }

    override fun onChangeUsername(username: String) = store.accept(Intent.UpdateUsername(username))

    override fun onChangePassword(password: String) = store.accept(Intent.UpdatePassword(password))

    override fun onAuthenticate() = store.accept(Intent.Authenticate)
}
