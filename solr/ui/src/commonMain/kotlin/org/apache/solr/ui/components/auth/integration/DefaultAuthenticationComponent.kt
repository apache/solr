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

import com.arkivanov.decompose.router.slot.ChildSlot
import com.arkivanov.decompose.router.slot.SlotNavigation
import com.arkivanov.decompose.router.slot.activate
import com.arkivanov.decompose.router.slot.childSlot
import com.arkivanov.decompose.value.Value
import com.arkivanov.essenty.lifecycle.doOnCreate
import com.arkivanov.mvikotlin.core.instancekeeper.getStore
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.stateFlow
import io.ktor.client.HttpClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import org.apache.solr.ui.components.auth.BasicAuthComponent
import org.apache.solr.ui.components.auth.AuthenticationComponent
import org.apache.solr.ui.components.auth.AuthenticationComponent.BasicAuthConfiguration
import org.apache.solr.ui.components.auth.AuthenticationComponent.Output
import org.apache.solr.ui.components.auth.store.AuthenticationStore.Intent
import org.apache.solr.ui.components.auth.store.AuthenticationStoreProvider
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.coroutineScope
import org.apache.solr.ui.utils.map

class DefaultAuthenticationComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    methods: List<AuthMethod>,
    private val output: (Output) -> Unit,
) : AuthenticationComponent, AppComponentContext by componentContext {

    private val mainScope = coroutineScope(SupervisorJob() + mainContext)
    private val ioScope = coroutineScope(SupervisorJob() + ioContext)

    private val store = instanceKeeper.getStore {
        AuthenticationStoreProvider(
            storeFactory = storeFactory,
            mainContext = mainScope.coroutineContext,
            ioContext = ioScope.coroutineContext,
            methods = methods,
        ).provide()
    }

    private val basicAuthNavigation = SlotNavigation<BasicAuthConfiguration>()

    @OptIn(ExperimentalCoroutinesApi::class)
    override val model = store.stateFlow.map(mainScope, authenticationStateToModel)

    override val basicAuthSlot: Value<ChildSlot<BasicAuthConfiguration, BasicAuthComponent>> =
        childSlot(
            source = basicAuthNavigation,
            serializer = BasicAuthConfiguration.serializer(),
            handleBackButton = true,
        ) { config, childComponentContext ->
            DefaultBasicAuthComponent(
                componentContext = childComponentContext,
                storeFactory = storeFactory,
                httpClient = httpClient,
                output = ::basicAuthOutput,
            )
        }

    init {
        lifecycle.doOnCreate {
            methods.forEach { method ->
                when(method) {
                    is AuthMethod.BasicAuthMethod ->
                        basicAuthNavigation.activate(configuration = BasicAuthConfiguration(method))
                }
            }
        }
    }

    fun basicAuthOutput(output: BasicAuthComponent.Output): Unit = when (output) {
        is BasicAuthComponent.Output.Authenticated -> output(Output.OnAuthenticatedWithBasicAuth(username = output.username, password = output.password))
        is BasicAuthComponent.Output.Authenticating -> store.accept(Intent.StartAuthenticating)
        is BasicAuthComponent.Output.AuthenticationFailed -> store.accept(Intent.FailAuthentication(output.error))
    }

    override fun onAbort() = output(Output.OnAbort)
}
