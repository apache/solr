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

package org.apache.solr.ui.views.navigation

/**
 * An enum class that holds all the menu items of the main navigation.
 *
 * This enum is used as abstraction layer between UI and business logic. This is important because
 * many UI-related elements, like icons and string resources, will depend on this enum and not a
 * specific class of the business logic.
 */
enum class MainMenu {
    Dashboard,
    Metrics,
    Cluster,
    Security,
    Configsets,
    Collections,
    QueriesAndOperations,
    Environment,
    Logging,
    ThreadDump,
}
