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
package org.apache.solr.languagemodels.model;

import java.util.Map;

/** Base class for Solr-managed wrappers around langchain4j used in {@code language-models} module */
public abstract class SolrLanguageModel {

  // common parameters
  protected static final String TIMEOUT_PARAM = "timeout";
  protected static final String MAX_RETRIES_PARAM = "maxRetries";

  protected final String name;
  protected final Map<String, Object> params;

  protected SolrLanguageModel(String name, Map<String, Object> params) {
    this.name = name;
    this.params = params;
  }

  public String getName() {
    return name;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  /** Returns the class name of the underlying langchain4j model instance. */
  public abstract String getModelClassName();
}
