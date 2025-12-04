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
package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.SolrClient;

/**
 * A config hook for post-configuration of a {@linkplain SolrClient} by its builder. It is not
 * supported by all builders.
 *
 * @param <T> type of SolrClient this customizer targets
 * @see org.apache.solr.client.solrj.jetty.HttpJettySolrClient#CLIENT_CUSTOMIZER_SYSPROP
 * @lucene.experimental
 */
public interface SolrClientCustomizer<T extends SolrClient> {

  void setup(T client);
}
