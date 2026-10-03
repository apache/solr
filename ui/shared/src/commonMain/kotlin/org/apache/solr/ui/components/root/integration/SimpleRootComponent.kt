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

import io.ktor.client.HttpClient
import io.ktor.http.Url
import org.apache.solr.ui.components.auth.AuthenticationComponent
import org.apache.solr.ui.components.auth.integration.DefaultAuthenticationComponent
import org.apache.solr.ui.components.main.MainComponent
import org.apache.solr.ui.components.main.integration.DefaultMainComponent
import org.apache.solr.ui.components.root.RootComponent
import org.apache.solr.ui.components.root.viewmodel.RootViewModel
import org.apache.solr.ui.components.start.StartComponent
import org.apache.solr.ui.components.start.integration.DefaultStartComponent
import org.apache.solr.ui.domain.AuthMethod
import org.apache.solr.ui.domain.AuthOption
import org.apache.solr.ui.utils.getDefaultClient
import org.apache.solr.ui.utils.getHttpClientWithAuthOption

/**
 * A simple root component implementation that does not check the user's access level and redirects
 * to the requested destination.
 *
 * This component is used only temporary and will be replaced in the future with an implementation
 * that checks the access level of the user before redirecting.
 *
 * @param httpClient The pre-configured HTTP client to use for connecting to a Solr instance.
 * @param destination The section to show initially after the user has access, if any.
 */
class SimpleRootComponent(
    private val httpClient: HttpClient,
    private val destination: String? = null,
) : RootComponent {

    override fun createRootViewModel(): RootViewModel = RootViewModel()

    override fun createStartComponent(): StartComponent = DefaultStartComponent(httpClient = httpClient)

    override fun createAuthenticationComponent(
        url: Url,
        methods: List<AuthMethod>,
    ): AuthenticationComponent = DefaultAuthenticationComponent(
        httpClient = getDefaultClient(url),
        url = url,
        methods = methods,
    )

    override fun createMainComponent(authOption: AuthOption): MainComponent = DefaultMainComponent(
        httpClient = getHttpClientWithAuthOption(authOption),
        destination = destination,
    )
}
