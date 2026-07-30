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
import org.apache.solr.ui.domain.PickedFile

/**
 * Use case for importing a configset from a file.
 */
interface ImportConfigsetUseCase {
    /**
     * Default invocation for importing a configset from a simple zip file.
     *
     * @param configsetName The name of the configset to use.
     * @param file The configset file to import.
     *
     * @return A Result containing the Configset of the created configset or an error if the import
     * failed.
     */
    suspend operator fun invoke(configsetName: String, file: PickedFile): ImportConfigsetResult
}

sealed interface ImportConfigsetResult {
    data class Success(val configset: Configset) : ImportConfigsetResult

    data class ValidationFailure(val error: Error) : ImportConfigsetResult

    data class UnexpectedFailure(val cause: Throwable) : ImportConfigsetResult

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
