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

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.auth.BasicAuthComponent
import org.apache.solr.ui.components.auth.BasicAuthComponent.Model

internal class TestBasicAuthComponent(model: Model = Model()) : BasicAuthComponent {
    var onChangeUsernameClicked = false
    var onChangePasswordClicked = false
    var onAuthenticateClicked = false

    override val model: StateFlow<Model> = MutableStateFlow(model)

    override fun onChangeUsername(username: String) {
        onChangeUsernameClicked = true
    }

    override fun onChangePassword(password: String) {
        onChangePasswordClicked = true
    }

    override fun onAuthenticate() {
        onAuthenticateClicked = true
    }
}
