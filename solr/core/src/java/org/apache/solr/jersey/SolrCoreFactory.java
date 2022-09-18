/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.jersey;

import org.apache.solr.core.SolrCore;
import org.glassfish.hk2.api.Factory;

/**
 * Allows the SolrCore germane to a particular request to be injected into individual resource
 * instances at call-time.
 */
public class SolrCoreFactory implements Factory<SolrCore> {

  private final SolrCore solrCore;

  public SolrCoreFactory(SolrCore solrCore) {
    this.solrCore = solrCore;
  }

  @Override
  public SolrCore provide() {
    return solrCore;
  }

  @Override
  public void dispose(SolrCore instance) {
    /* No-op */
  }
}
