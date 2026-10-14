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

package org.apache.solr.ui.components.root

import io.ktor.http.Url
import org.apache.solr.ui.components.auth.AuthenticationComponent
import org.apache.solr.ui.components.main.MainComponent
import org.apache.solr.ui.components.root.viewmodel.RootViewModel
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthOption

/**
 * Root component used by each target as an entry point to the application.
 *
 * This component provides the components of the screens the user is redirected to, depending on
 * the information available at start time. Implementations may check user session, access level,
 * destination and more.
 */
interface RootComponent {

    /**
     * Factory method to create a [RootViewModel] instance.
     */
    fun createRootViewModel(): RootViewModel

    /**
     * Factory method to create the component of the start screen.
     */
    fun createStartComponent(): StartComponent

    /**
     * Factory method to create the component of the authentication screen.
     *
     * @param url The URL of the Solr instance the user is authenticating against.
     * @param methods The authentication methods that can be used to authenticate the user.
     */
    fun createAuthenticationComponent(url: Url, methods: List<AuthMethod>): AuthenticationComponent

    /**
     * Factory method to create the component of the main screen.
     *
     * @param authOption The option that was used to authenticate the user.
     */
    fun createMainComponent(authOption: AuthOption): MainComponent
}
