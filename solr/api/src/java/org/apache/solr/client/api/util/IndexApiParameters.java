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
package org.apache.solr.client.api.util;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.solr.client.api.model.IndexType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.apache.solr.client.api.util.Constants.INDEX_NAME_PATH_PARAMETER;
import static org.apache.solr.client.api.util.Constants.INDEX_TYPE_PATH_PARAMETER;

/**
 * Concisely collects the parameters shared by APIs that interact with contents of a specific core/collection.
 *
 * Used primarily as a way to avoid duplicating these parameter definitions on each relevant interface method in {@link org.apache.solr.client.api.endpoint}
 */
// TODO NOCOMMIT Reconsider naming here - do we have a generic term to refer to both collections and cores.  "Index" is waaaay too overloaded.
@Target({ElementType.METHOD, ElementType.TYPE, ElementType.PARAMETER, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Parameter(name = INDEX_TYPE_PATH_PARAMETER, in = ParameterIn.PATH, schema = @Schema(implementation = IndexType.class, enumAsRef = true))
@Parameter(name = INDEX_NAME_PATH_PARAMETER, in = ParameterIn.PATH)
public @interface IndexApiParameters {}