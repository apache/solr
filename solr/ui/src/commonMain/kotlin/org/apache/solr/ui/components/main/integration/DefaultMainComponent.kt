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

package org.apache.solr.ui.components.main.integration

import com.arkivanov.decompose.router.stack.ChildStack
import com.arkivanov.decompose.router.stack.StackNavigation
import com.arkivanov.decompose.router.stack.bringToFront
import com.arkivanov.decompose.router.stack.childStack
import com.arkivanov.decompose.value.Value
import com.arkivanov.mvikotlin.core.store.StoreFactory
import io.ktor.client.HttpClient
import kotlinx.serialization.Serializable
import org.apache.solr.ui.components.environment.EnvironmentComponent
import org.apache.solr.ui.components.environment.integration.DefaultEnvironmentComponent
import org.apache.solr.ui.components.logging.LoggingComponent
import org.apache.solr.ui.components.logging.integration.DefaultLoggingComponent
import org.apache.solr.ui.components.main.MainComponent
import org.apache.solr.ui.components.main.MainComponent.Child
import org.apache.solr.ui.components.main.MainComponent.Output
import org.apache.solr.ui.utils.AppComponentContext
import org.apache.solr.ui.views.navigation.MainMenu

class DefaultMainComponent internal constructor(
    componentContext: AppComponentContext,
    storeFactory: StoreFactory,
    destination: String? = null,
    private val environmentComponent: (AppComponentContext) -> EnvironmentComponent,
    private val loggingComponent: (AppComponentContext) -> LoggingComponent,
    private val output: (Output) -> Unit,
) : MainComponent,
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
        destination: String? = null,
        output: (Output) -> Unit,
    ) : this(
        componentContext = componentContext,
        storeFactory = storeFactory,
        destination = destination,
        output = output,
        environmentComponent = { childContext ->
            DefaultEnvironmentComponent(
                componentContext = childContext,
                storeFactory = storeFactory,
                httpClient = httpClient,
            )
        },
        loggingComponent = { childContext ->
            DefaultLoggingComponent(
                componentContext = childContext,
                storeFactory = storeFactory,
            )
        },
    )

    override fun onNavigate(menuItem: MainMenu) = navigation.bringToFront(menuItem.toConfiguration())

    override fun onNavigateBack() {
        TODO("Not yet implemented")
    }

    override fun onLogout() = output(Output.UserLoggedOut)

    /**
     * Calculates the initial stack based on the destination provided.
     */
    private fun calculateInitialStack(destination: String?): List<Configuration> = listOf(
        when (destination) {
            "environment" -> Configuration.Environment
            "logging" -> Configuration.Logging
            else -> Configuration.Environment
        },
    )

    private fun createChild(
        configuration: Configuration,
        componentContext: AppComponentContext,
    ): Child = when (configuration) {
        // TODO Uncomment once Dashboard available
        // Configuration.Dashboard ->
        //     NavigationComponent.Child.Dashboard(dashboardComponent(componentContext))

        // TODO Uncomment once Metrics available
        // Configuration.Metrics ->
        //     NavigationComponent.Child.Metrics(metricsComponent(componentContext))

        // TODO Uncomment once Cluster available
        // Configuration.Cluster ->
        //     NavigationComponent.Child.Cluster(clusterComponent(componentContext))

        // TODO Uncomment once Security available
        // Configuration.Security ->
        //     NavigationComponent.Child.Security(securityComponent(componentContext))

        // TODO Uncomment once Configsets available
        // Configuration.Configsets ->
        //     NavigationComponent.Child.Configsets(configsetsComponent(componentContext))

        // TODO Uncomment once Collections available
        // Configuration.Collections ->
        //     NavigationComponent.Child.Collections(collectionsComponent(componentContext))

        // TODO Uncomment once QueriesAndOperations available
        // Configuration.QueriesAndOperations ->
        //     NavigationComponent.Child.QueriesAndOperations(queriesAndOperationsComponent(componentContext))

        Configuration.Environment -> Child.Environment(environmentComponent(componentContext))

        Configuration.Logging -> Child.Logging(loggingComponent(componentContext))

        // TODO Uncomment once ThreadDump available
        // Configuration.ThreadDump ->
        //     NavigationComponent.Child.ThreadDump(threadDumpComponent(componentContext))

        // TODO Remove else case once all destinations are available
        else -> throw NotImplementedError("Navigation to $configuration not implemented yet.")
    }

    /**
     * Configuration that provides information for navigating to supported destinations.
     *
     * This configuration is used by both UI and business logic and therefore publicly available.
     * Note that configurations are usually private to the implementation and not available in the
     * interface level (therefore neither in the UI). Providing this class in the interface
     * simplifies additional mappings that would otherwise be necessary. Leaking the configuration
     * with the component's interface prevents the implementations from storing sensible /
     * component-internal information in the configurations. Therefore, if any implementation in the
     * future needs to store information in the configuration, this interface should be replaced
     * with an enum and mapped accordingly per implementation.
     *
     * Use this practice only if really necessary.
     *
     * Changes to this interface have impact on both UI and business logic.
     */
    @Serializable
    private sealed interface Configuration {

        @Serializable
        data object Dashboard : Configuration

        @Serializable
        data object Metrics : Configuration

        @Serializable
        data object Cluster : Configuration

        @Serializable
        data object Security : Configuration

        @Serializable
        data object Configsets : Configuration

        @Serializable
        data object Collections : Configuration

        @Serializable
        data object QueriesAndOperations : Configuration

        @Serializable
        data object Environment : Configuration

        @Serializable
        data object Logging : Configuration

        @Serializable
        data object ThreadDump : Configuration
    }

    private fun MainMenu.toConfiguration(): Configuration = when (this) {
        MainMenu.Dashboard -> Configuration.Dashboard
        MainMenu.Metrics -> Configuration.Metrics
        MainMenu.Cluster -> Configuration.Cluster
        MainMenu.Security -> Configuration.Security
        MainMenu.Configsets -> Configuration.Configsets
        MainMenu.Collections -> Configuration.Collections
        MainMenu.QueriesAndOperations -> Configuration.QueriesAndOperations
        MainMenu.Environment -> Configuration.Environment
        MainMenu.Logging -> Configuration.Logging
        MainMenu.ThreadDump -> Configuration.ThreadDump
    }
}
