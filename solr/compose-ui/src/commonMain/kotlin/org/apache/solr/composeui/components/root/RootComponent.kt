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

package org.apache.solr.composeui.components.root

import com.arkivanov.decompose.router.stack.ChildStack
import com.arkivanov.decompose.value.Value
import org.apache.solr.composeui.components.main.MainComponent

/**
 * Root component used by each target as an entry point to the application.
 *
 * This component checks the information available at start time and redirects the user accordingly.
 * Implementations may check user session, access level, destination and more.
 */
interface RootComponent {

    val childStack: Value<ChildStack<*, Child>>

    sealed interface Child {

        data class Main(val component: MainComponent): Child

        // TODO Add child once authentication is checked
        // data class Unauthenticated(val component: UnauthenticatedComponent): Child
    }
}
