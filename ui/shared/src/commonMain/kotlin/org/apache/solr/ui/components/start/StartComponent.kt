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

package org.apache.solr.ui.components.start

import org.apache.solr.ui.components.start.domain.ConnectUseCase
import org.apache.solr.ui.components.start.repository.StartRepository
import org.apache.solr.ui.components.start.viewmodel.StartViewModel

/**
 * Component interface that represents the start screen.
 */
interface StartComponent {

    /**
     * Dependencies provided by the application.
     */
    val startRepository: StartRepository

    /**
     * Use case responsible for connecting to a Solr instance.
     */
    val connectUseCase: ConnectUseCase

    /**
     * Factory method to create a [StartViewModel] instance.
     */
    fun createStartViewModel(): StartViewModel
}
