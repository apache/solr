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
import io.ktor.http.Url
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import org.apache.solr.ui.components.auth.AuthenticationComponent
import org.apache.solr.ui.components.auth.AuthenticationComponent.BasicAuthConfiguration
import org.apache.solr.ui.components.auth.AuthenticationComponent.Output
import org.apache.solr.ui.components.auth.AuthenticationComponent.Output.OnAuthenticated
import org.apache.solr.ui.components.auth.BasicAuthComponent
import org.apache.solr.ui.components.auth.store.AuthenticationStore.Intent
import org.apache.solr.ui.components.auth.store.AuthenticationStore.Intent.FailAuthentication
import org.apache.solr.ui.components.auth.store.AuthenticationStore.Intent.ResetError
import org.apache.solr.ui.components.auth.store.AuthenticationStoreProvider
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthOption
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.coroutineScope
import org.apache.solr.ui.utils.map

/**
 * Default implementation of the [AuthenticationComponent].
 *
 * @param componentContext Application component context.
 * @param storeFactory Store factory to pass to the store provider.
 * @param httpClient HTTP client to use for API interactions. The client has to be pre-configured
 * with a base URL pointing to a Solr instance.
 * @param methods A list of authentication methods that are supported by the Solr instance the
 * [httpClient] is pointing to.
 * @property output Output handler that is used to send outputs from this component.
 */
class DefaultAuthenticationComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    private val url: Url,
    methods: List<AuthMethod>,
    private val output: (Output) -> Unit,
) : AuthenticationComponent,
    AppComponentContext by componentContext {

    private val mainScope = coroutineScope(SupervisorJob() + mainContext)
    private val ioScope = coroutineScope(SupervisorJob() + ioContext)

    private val store = instanceKeeper.getStore {
        AuthenticationStoreProvider(
            storeFactory = storeFactory,
            mainContext = mainScope.coroutineContext,
            ioContext = ioScope.coroutineContext,
            url = url,
            methods = methods,
        ).provide()
    }

    private val basicAuthNavigation = SlotNavigation<BasicAuthConfiguration>()

    @OptIn(ExperimentalCoroutinesApi::class)
    override val model = store.stateFlow.map(mainScope, authStateToModel)

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
                method = config.method,
                output = ::basicAuthOutput,
            )
        }

    init {
        lifecycle.doOnCreate {
            methods.forEach { method ->
                when (method) {
                    is AuthMethod.BasicAuthMethod ->
                        basicAuthNavigation.activate(configuration = BasicAuthConfiguration(method))
                }
            }
        }
    }

    /**
     * Output handler function that handles the outputs from [BasicAuthComponent]s.
     *
     * @param output The output of the component to handle
     */
    private fun basicAuthOutput(output: BasicAuthComponent.Output): Unit = when (output) {
        is BasicAuthComponent.Output.Authenticated -> output(
            OnAuthenticated(
                option = AuthOption.BasicAuthOption(
                    url = url,
                    method = output.method,
                    username = output.username,
                    password = output.password,
                ),
            ),
        )

        is BasicAuthComponent.Output.Authenticating -> store.accept(Intent.StartAuthenticating)
        is BasicAuthComponent.Output.AuthenticationFailed ->
            store.accept(FailAuthentication(output.error))

        BasicAuthComponent.Output.ErrorReset ->
            store.accept(ResetError)
    }

    override fun onAbort() = output(Output.OnAbort)
}
