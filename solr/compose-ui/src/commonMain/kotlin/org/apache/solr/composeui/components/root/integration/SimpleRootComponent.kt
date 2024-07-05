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

package org.apache.solr.composeui.components.root.integration

import com.arkivanov.decompose.router.stack.ChildStack
import com.arkivanov.decompose.router.stack.StackNavigation
import com.arkivanov.decompose.router.stack.childStack
import com.arkivanov.decompose.value.Value
import com.arkivanov.mvikotlin.core.store.StoreFactory
import kotlinx.serialization.Serializable
import org.apache.solr.composeui.components.main.MainComponent
import org.apache.solr.composeui.components.main.integration.DefaultMainComponent
import org.apache.solr.composeui.components.root.RootComponent
import org.apache.solr.composeui.utils.AppComponentContext

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
    private val mainComponent: (AppComponentContext) -> MainComponent,
) : RootComponent, AppComponentContext by componentContext {

    private val navigation = StackNavigation<Configuration>()
    private val stack = childStack(
        source = navigation,
        serializer = Configuration.serializer(),
        initialStack = { listOf(Configuration.Main) },
        handleBackButton = true,
        childFactory = ::createChild
    )

    override val childStack: Value<ChildStack<*, RootComponent.Child>> = stack

    constructor(
        componentContext: AppComponentContext,
        storeFactory: StoreFactory,
        destination: String? = null,
    ) : this(
        componentContext = componentContext,
        storeFactory = storeFactory,
        mainComponent = { childContext ->
            DefaultMainComponent(
                componentContext = childContext,
                storeFactory = storeFactory,
                destination = destination,
            )
        },
    )

    private fun createChild(
        configuration: Configuration,
        componentContext: AppComponentContext,
    ): RootComponent.Child = when (configuration) {
        Configuration.Main -> RootComponent.Child.Main(mainComponent(componentContext))
    }

    @Serializable
    private sealed interface Configuration {

        @Serializable
        data object Main : Configuration
    }
}
