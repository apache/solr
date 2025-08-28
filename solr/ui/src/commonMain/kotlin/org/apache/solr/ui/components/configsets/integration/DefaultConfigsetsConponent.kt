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

package org.apache.solr.ui.components.configsets.integration

import com.arkivanov.decompose.router.stack.ChildStack
import com.arkivanov.decompose.router.stack.StackNavigation
import com.arkivanov.decompose.router.stack.childStack
import com.arkivanov.decompose.value.Value
import com.arkivanov.mvikotlin.core.instancekeeper.getStore
import com.arkivanov.mvikotlin.core.store.StoreFactory
import com.arkivanov.mvikotlin.extensions.coroutines.stateFlow
import io.ktor.client.HttpClient
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import org.apache.solr.ui.components.configsets.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.ConfigsetsComponent.Child
import org.apache.solr.ui.components.configsets.overview.OverviewComponent
import org.apache.solr.ui.components.configsets.overview.integration.DefaultOverviewComponent
import org.apache.solr.ui.components.configsets.store.ConfigsetsStoreProvider
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.utils.coroutineScope
import org.apache.solr.ui.utils.map

/**
 * Default implementation of the [ConfigsetsComponent].
 */
class DefaultConfigsetsConponent internal constructor(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    httpClient: HttpClient,
    destination: String? = null,
    private val overviewComponent: (AppComponentContext) -> OverviewComponent,
) : ConfigsetsComponent,
    AppComponentContext by componentContext {

    private val navigation = StackNavigation<Configuration>()
    private val stack = childStack(
        source = navigation,
        serializer = Configuration.serializer(),
        initialStack = { calculateInitialStack(destination) },
        handleBackButton = true,
        childFactory = ::createChild,
    )

    override val childStack: Value<ChildStack<*, Child>> = stack
    constructor(
        componentContext: AppComponentContext,
        storeFactory: StoreFactory,
        httpClient: HttpClient,
    ) : this (
        componentContext = componentContext,
        storeFactory = storeFactory,
        httpClient = httpClient,
        destination = null,
        overviewComponent = { childContext ->
            DefaultOverviewComponent(
                componentContext = childContext,
                storeFactory = storeFactory,
                httpClient = httpClient,
            )
        }
    )
    @Serializable
    private sealed interface Configuration {
        @Serializable
        data object Overview: Configuration
    }

    private fun createChild(
        configuration: Configuration,
        componentContext: AppComponentContext,
    ): Child = when (configuration) {
        Configuration.Overview -> Child.Overview(overviewComponent(componentContext))
    }

    /**
     * Calculates the initial stack based on the destination provided.
     */
    private fun calculateInitialStack(destination: String?): List<Configuration> = listOf(
        when (destination) {
            "Overview" -> Configuration.Overview
            else -> Configuration.Overview
        },
    )

    private val mainScope = coroutineScope(SupervisorJob() + mainContext)
    private val ioScope = coroutineScope(SupervisorJob() + ioContext)

    private val store = instanceKeeper.getStore {
        ConfigsetsStoreProvider(
            storeFactory = storeFactory,
            client = HttpEnvironmentStoreClient(httpClient),
            mainContext = mainScope.coroutineContext,
            ioContext = ioScope.coroutineContext,
        ).provide()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    override val model = store.stateFlow.map(mainScope, configsetsStateToModel)
}
