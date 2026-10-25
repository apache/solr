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

package org.apache.solr.ui

import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.Surface
import androidx.compose.ui.Modifier
import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application
import androidx.compose.ui.window.rememberWindowState
import java.awt.Dimension
import org.apache.solr.ui.components.root.RootComponent
import org.apache.solr.ui.components.root.integration.SimpleRootComponent
import org.apache.solr.ui.utils.getDefaultClient
import org.apache.solr.ui.views.root.RootContent
import org.apache.solr.ui.views.theme.SolrTheme

/**
 * Entry point of the Compose application for all JVM-based (desktop) targets.
 *
 * This function instantiates the [RootComponent] that is used by all screens.
 */
fun main() {
    // TODO Let the user provide the URL via input field on desktop
    val httpClient = getDefaultClient()

    val root: RootComponent = SimpleRootComponent(httpClient = httpClient)

    application {
        val windowState = rememberWindowState()

        Window(
            onCloseRequest = ::exitApplication,
            state = windowState,
            title = "Solr Admin UI",
        ) {
            window.minimumSize = Dimension(720, 560)

            SolrTheme(useDarkTheme = false) {
                Surface(modifier = Modifier.fillMaxSize()) {
                    RootContent(root)
                }
            }
        }
    }
}
