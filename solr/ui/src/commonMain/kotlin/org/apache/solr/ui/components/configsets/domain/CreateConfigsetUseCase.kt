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

package org.apache.solr.ui.components.configsets.domain

import org.apache.solr.ui.domain.Configset

/**
 * Use case for creating a new configset.
 */
interface CreateConfigsetUseCase {
    /**
     * Default invocation for creating a new configset with user inputs.
     *
     * @param configsetName The configset name to use.
     * @param baseConfigset The configset to use as base for the new configset.
     *
     * @return A Result containing the Configset of the newly created configset or an error if
     * creation failed.
     */
    suspend operator fun invoke(
        configsetName: String,
        baseConfigset: String? = null,
    ): CreateConfigsetResult
}

sealed interface CreateConfigsetResult {
    data class Success(val configset: Configset) : CreateConfigsetResult

    data class ValidationFailure(val error: Error) : CreateConfigsetResult

    data class UnexpectedFailure(val cause: Throwable) : CreateConfigsetResult

    enum class Error {
        /**
         * Error for indicating an invalid configset name.
         */
        InvalidConfigsetName,

        /**
         * Error for indicating that the configset with the given name already exists.
         */
        DuplicateConfigset,
    }
}
