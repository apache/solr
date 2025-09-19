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

package org.apache.solr.ui.components.navigation.integration

import com.arkivanov.decompose.router.slot.ChildSlot
import com.arkivanov.decompose.router.slot.SlotNavigation
import com.arkivanov.decompose.router.slot.activate
import com.arkivanov.decompose.router.slot.childSlot
import com.arkivanov.decompose.value.Value
import com.arkivanov.essenty.lifecycle.doOnCreate
import com.arkivanov.essenty.lifecycle.doOnResume
import kotlinx.serialization.KSerializer
import org.apache.solr.ui.components.navigation.TabNavigationComponent
import org.apache.solr.ui.components.navigation.TabNavigationComponent.Configuration
import org.apache.solr.ui.utils.AppComponentContext

class DefaultTabNavigationComponent<T : Enum<T>, C : Any>(
    componentContext: AppComponentContext,
    initialTab: T,
    tabSerializer: KSerializer<T>,
    childFactory: (Configuration<T>, AppComponentContext) -> C,
) : TabNavigationComponent<T, C>,
    AppComponentContext by componentContext {

    private val navigation = SlotNavigation<Configuration<T>>()

    override val tabSlot: Value<ChildSlot<Configuration<T>, C>> = childSlot(
        source = navigation,
        serializer = Configuration.serializer(tabSerializer),
        handleBackButton = true,
        childFactory = childFactory,
    )

    init {
        navigation.activate(configuration = Configuration(initialTab))
    }

    override fun onNavigate(tab: T) = navigation.activate(configuration = Configuration(tab))
}
