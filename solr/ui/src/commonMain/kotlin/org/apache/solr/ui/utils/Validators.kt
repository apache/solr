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

package org.apache.solr.ui.utils

/**
 * Regex that configset names must match in order to be valid.
 *
 * Note that this regex is only used in the UI and does not represent the actual allowed
 * name regex for configsets, as this has not yet been defined.
 */
internal val configsetNameRegex = "[a-zA-Z0-9._-]+".toRegex()

/**
 * The maximum length a configset name may have.
 *
 * Note that this is a maximum length used in the UI only and does not represent the actual allowed
 * length for configset names.
 */
internal const val MAX_CONFIGSET_NAME_LENGTH = 256
