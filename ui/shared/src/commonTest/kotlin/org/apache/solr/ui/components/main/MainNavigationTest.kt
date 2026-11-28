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

package org.apache.solr.ui.components.main

import kotlin.test.Test
import kotlin.test.assertEquals
import org.apache.solr.ui.components.main.viewmodel.MainScene
import org.apache.solr.ui.components.main.viewmodel.MainViewModel
import org.apache.solr.ui.components.main.viewmodel.menuItem
import org.apache.solr.ui.views.navigation.MainMenu

class MainNavigationTest {
    @Test
    fun indexQueryAndConfigsetsAreSeparateScenes() {
        val model = MainViewModel()
        assertEquals(MainScene.Configsets, model.backStack.last())

        model.navigate(MainMenu.IndexAndQuery)
        assertEquals(MainScene.IndexQuery, model.backStack.last())
        assertEquals(MainMenu.IndexAndQuery, model.backStack.last().menuItem)

        model.navigate(MainMenu.Configsets)
        assertEquals(MainScene.Configsets, model.backStack.last())
        model.navigateBack()
        assertEquals(MainScene.IndexQuery, model.backStack.last())
    }

    @Test
    fun indexQueryCanBeTheInitialDestination() {
        val model = MainViewModel(destination = "index-query")
        assertEquals(MainScene.IndexQuery, model.backStack.last())
        assertEquals(MainMenu.IndexAndQuery, model.backStack.last().menuItem)
    }
}
