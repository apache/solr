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

package org.apache.solr.ui.components.environment.repository

import org.apache.solr.ui.components.environment.data.JavaProperty
import org.apache.solr.ui.components.environment.data.SystemData

/**
 * Repository interface for fetching environment information.
 */
interface EnvironmentRepository {

    /**
     * Fetches a set of system data.
     *
     * @return Result with the system data fetched.
     */
    suspend fun getSystemData(): Result<SystemData>

    /**
     * Fetches the configured java properties.
     *
     * @return Result with a list of [JavaProperty]s.
     */
    suspend fun getJavaProperties(): Result<List<JavaProperty>>
}
