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

package org.apache.solr.ui.components.auth.repository

/**
 * Repository interface for authenticating with basic authentication.
 */
interface BasicAuthRepository {

    /**
     * Authenticates the user with the current Solr instance.
     *
     * @param username The username to use.
     * @param password The password to use.
     * @return Returns success results iff the credentials authenticated the user.
     */
    suspend fun authenticate(username: String, password: String): Result<Unit>
}
