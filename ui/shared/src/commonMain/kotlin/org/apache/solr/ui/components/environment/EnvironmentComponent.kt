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

package org.apache.solr.ui.components.environment

import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.components.environment.data.JavaProperty
import org.apache.solr.ui.components.environment.data.JvmData
import org.apache.solr.ui.components.environment.data.Versions

/**
 * Component interface that represents the environment section.
 */
interface EnvironmentComponent {

    val model: StateFlow<Model>

    /**
     * State class that holds values of the [EnvironmentComponent]'s sate.
     */
    data class Model(
        val versions: Versions = Versions(),
        val jvm: JvmData = JvmData(),
        val javaProperties: List<JavaProperty> = emptyList(),
    )
}
