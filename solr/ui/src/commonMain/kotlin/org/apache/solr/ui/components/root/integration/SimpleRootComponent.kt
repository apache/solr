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
import kotlinx.serialization.Serializable
import org.apache.solr.ui.components.auth.UnauthenticatedComponent
import org.apache.solr.ui.components.main.MainComponent
import org.apache.solr.ui.components.main.integration.DefaultMainComponent
import org.apache.solr.ui.components.root.RootComponent
import org.apache.solr.ui.components.root.RootComponent.Child.*
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.components.start.integration.DefaultStartComponent
import org.apache.solr.ui.utils.AppComponentContext

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
    private val mainComponent: (AppComponentContext) -> MainComponent,
    private val unauthenticatedComponent: (AppComponentContext, (UnauthenticatedComponent.Output) -> Unit) -> UnauthenticatedComponent,
) : RootComponent, AppComponentContext by componentContext {

    private val navigation = StackNavigation<Configuration>()
    private val stack = childStack(
        source = navigation,
        serializer = Configuration.serializer(),
        initialStack = { listOf(Configuration.Start) },
        handleBackButton = true,
        childFactory = ::createChild
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
        mainComponent = { childContext ->
            DefaultMainComponent(
                componentContext = childContext,
                storeFactory = storeFactory,
                httpClient = httpClient,
                destination = destination,
            )
        },
        unauthenticatedComponent = { childContext, output ->
            TODO("Not yet implemented")
        }
    )

    private fun createChild(
        configuration: Configuration,
        componentContext: AppComponentContext,
    ): RootComponent.Child = when (configuration) {
        Configuration.Start -> Start(startComponent(componentContext, ::startOutput))
        Configuration.Main -> Main(mainComponent(componentContext))
        Configuration.Unauthenticated -> Unauthenticated(
            unauthenticatedComponent(
                componentContext,
                ::unauthenticatedOutput
            )
        )
    }

    /**
     * Output handler for any output returned by the [StartComponent].
     *
     * @param output The output returned by the start component implementation.
     */
    private fun startOutput(output: StartComponent.Output) = when (output) {
        StartComponent.Output.OnAuthRequired -> navigation.pushNew(Configuration.Unauthenticated)
        StartComponent.Output.OnConnected ->
            navigation.replaceAll(Configuration.Main)
    }

    /**
     * Output handler for any output returned by the [UnauthenticatedComponent].
     *
     * @param output The output returned by the unauthenticated component implementation.
     */
    private fun unauthenticatedOutput(output: UnauthenticatedComponent.Output) = when (output) {
        UnauthenticatedComponent.Output.OnConnected -> navigation.replaceAll(Configuration.Main)
        UnauthenticatedComponent.Output.OnAbort -> navigation.pop()
    }

    @Serializable
    private sealed interface Configuration {

        @Serializable
        data object Start : Configuration

        @Serializable
        data object Unauthenticated : Configuration

        @Serializable
        data object Main : Configuration
    }
}
