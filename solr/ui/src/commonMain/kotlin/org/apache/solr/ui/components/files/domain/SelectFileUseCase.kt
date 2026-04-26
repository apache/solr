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

package org.apache.solr.ui.components.files.domain

import org.apache.solr.ui.domain.PickedFile

interface SelectFileUseCase {
    /**
     * Default invocation for selecting a file.
     *
     * @param extensions The allowed file extensions to filter.
     *
     * @return A Result containing the file that was selected.
     */
    suspend operator fun invoke(
        extensions: List<String>,
        maxSize: Int? = null,
    ): SelectFileResult
}

sealed interface SelectFileResult {
    data class Success(val file: PickedFile) : SelectFileResult

    data object Aborted: SelectFileResult

    data class ValidationFailure(val error: Error) : SelectFileResult

    data class UnexpectedFailure(val cause: Throwable) : SelectFileResult

    enum class Error {

        /**
         * Error for indicating that no file has been selected.
         */
        FileNotSelected,
        FileTooLarge,
        UnsupportedFormat,
    }
}
