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

package org.apache.solr.ui.components.root.integration

import com.arkivanov.decompose.router.stack.ChildStack
import com.arkivanov.decompose.router.stack.StackNavigation
import com.arkivanov.decompose.router.stack.childStack
import com.arkivanov.decompose.router.stack.pop
import com.arkivanov.decompose.router.stack.pushNew
import com.arkivanov.decompose.router.stack.replaceAll
import com.arkivanov.decompose.value.Value
import com.arkivanov.mvikotlin.core.store.StoreFactory
import io.ktor.client.HttpClient
import io.ktor.http.Url
import kotlinx.serialization.Serializable
import org.apache.solr.ui.components.auth.AuthenticationComponent
import org.apache.solr.ui.components.auth.integration.DefaultAuthenticationComponent
import org.apache.solr.ui.components.main.MainComponent
import org.apache.solr.ui.components.main.integration.DefaultMainComponent
import org.apache.solr.ui.components.root.RootComponent
import org.apache.solr.ui.components.root.RootComponent.Child.Authentication
import org.apache.solr.ui.components.root.RootComponent.Child.Main
import org.apache.solr.ui.components.root.RootComponent.Child.Start
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.components.start.integration.DefaultStartComponent
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthOption
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.getDefaultClient
import org.apache.solr.ui.utils.getHttpClientWithAuthOption

/**
 * A simple root component implementation that does not check the user's access level and redirects
 * to the requested destination.
 *
 * This component is used only temporary and will be replaced in the future with an implementation
 * that checks the access level of the user before redirecting.
 */
class SimpleRootComponent(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    private val startComponent: (AppComponentContext, (StartComponent.Output) -> Unit) -> StartComponent,
    private val mainComponent: (AppComponentContext, AuthOption, (MainComponent.Output) -> Unit) -> MainComponent,
    private val authenticationComponent: AuthenticationComponentProducer,
) : RootComponent,
    AppComponentContext by componentContext {

    private val navigation = StackNavigation<Configuration>()
    private val stack = childStack(
        source = navigation,
        serializer = Configuration.serializer(),
        initialStack = { listOf(Configuration.Start) },
        handleBackButton = true,
        childFactory = ::createChild,
    )

    override val childStack: Value<ChildStack<*, RootComponent.Child>> = stack

    constructor(
        componentContext: AppComponentContext,
        storeFactory: StoreFactory,
        httpClient: HttpClient,
        destination: String? = null,
    ) : this(
        componentContext = componentContext,
        storeFactory = storeFactory,
        startComponent = { childContext, output ->
            DefaultStartComponent(
                componentContext = childContext,
                storeFactory = storeFactory,
                httpClient = httpClient,
                output = output,
            )
        },
        mainComponent = { childContext, authOption, output ->
            DefaultMainComponent(
                componentContext = childContext,
                storeFactory = storeFactory,
                httpClient = getHttpClientWithAuthOption(authOption),
                destination = destination,
                output = output,
            )
        },
        authenticationComponent = { childContext, url, methods, output ->
            DefaultAuthenticationComponent(
                componentContext = childContext,
                storeFactory = storeFactory,
                httpClient = getDefaultClient(url),
                url = url,
                methods = methods,
                output = output,
            )
        },
    )

    private fun createChild(
        configuration: Configuration,
        componentContext: AppComponentContext,
    ): RootComponent.Child = when (configuration) {
        is Configuration.Start -> Start(startComponent(componentContext, ::startOutput))
        is Configuration.Main -> Main(mainComponent(componentContext, configuration.authOption, ::mainOutput))

        is Configuration.Authentication -> Authentication(
            authenticationComponent(
                componentContext,
                configuration.url,
                configuration.methods,
            ) { output -> authenticationOutput(output) },
        )
    }

    /**
     * Output handler for any output returned by the [StartComponent].
     *
     * @param output The output returned by the start component implementation.
     */
    private fun startOutput(output: StartComponent.Output) = when (output) {
        is StartComponent.Output.OnAuthRequired -> navigation.pushNew(
            Configuration.Authentication(
                url = output.url,
                methods = output.methods,
            ),
        )

        is StartComponent.Output.OnConnected ->
            navigation.replaceAll(Configuration.Main(authOption = AuthOption.None(url = output.url)))
    }

    /**
     * Output handler for any output returned by the [MainComponent].
     *
     * @param output The output returned by the main component implementation.
     */
    private fun mainOutput(output: MainComponent.Output) = when (output) {
        is MainComponent.Output.UserLoggedOut -> navigation.replaceAll(Configuration.Start)
    }

    /**
     * Output handler for any output returned by the [AuthenticationComponent].
     *
     * @param output The output returned by the authentication component implementation.
     */
    private fun authenticationOutput(output: AuthenticationComponent.Output) = when (output) {
        is AuthenticationComponent.Output.OnAuthenticated ->
            navigation.replaceAll(Configuration.Main(authOption = output.option))

        is AuthenticationComponent.Output.OnAbort -> navigation.pop()
    }

    @Serializable
    private sealed interface Configuration {

        @Serializable
        data object Start : Configuration

        /**
         * Configuration for pending authentication actions.
         *
         * @property url The URL where the user is not authenticated.
         * @property methods List of methods that can be used to authenticate the user
         * again.
         */
        @Serializable
        data class Authentication(val url: Url, val methods: List<AuthMethod>) : Configuration

        @Serializable
        data class Main(val authOption: AuthOption) : Configuration
    }
}

/**
 * The authentication component producer (alias)
 */
private typealias AuthenticationComponentProducer = (
    AppComponentContext,
    Url,
    List<AuthMethod>,
    (AuthenticationComponent.Output) -> Unit,
) -> AuthenticationComponent
