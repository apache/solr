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

package org.apache.solr.ui.views.auth

import com.arkivanov.decompose.Child.Created
import com.arkivanov.decompose.router.slot.ChildSlot
import com.arkivanov.decompose.value.MutableValue
import com.arkivanov.decompose.value.Value
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.auth.AuthenticationComponent
import org.apache.solr.ui.components.auth.AuthenticationComponent.BasicAuthConfiguration
import org.apache.solr.ui.components.auth.AuthenticationComponent.Model
import org.apache.solr.ui.components.auth.BasicAuthComponent

internal class TestAuthenticationComponent(
    model: Model = Model(),
    basicAuthConfig: BasicAuthConfiguration = BasicAuthConfiguration(),
    basicAuthComponent: BasicAuthComponent = TestBasicAuthComponent(),
) : AuthenticationComponent {
    var onAbortClicked = false
    override val model: StateFlow<Model> = MutableStateFlow(model)
    override val basicAuthSlot: Value<ChildSlot<BasicAuthConfiguration, BasicAuthComponent>> =
        MutableValue(
            ChildSlot(
                child = Created(
                    configuration = basicAuthConfig,
                    instance = basicAuthComponent,
                ),
            ),
        )

    override fun onAbort() {
        onAbortClicked = true
    }
}
