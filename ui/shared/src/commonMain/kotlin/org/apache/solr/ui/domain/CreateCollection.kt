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

package org.apache.solr.ui.domain

data class CreateCollectionInput(
    val name: String,
    val configset: String,
    val shards: Int,
    val nrtReplicas: Int,
    val tlogReplicas: Int,
    val pullReplicas: Int,
    val router: String,
    val routeField: String?,
    val async: String?,
    val properties: Map<String, String>,
)

sealed interface CreateCollectionResult {
    data object Created : CreateCollectionResult

    data object NameExists : CreateCollectionResult

    data class Failed(val message: String?) : CreateCollectionResult
}
