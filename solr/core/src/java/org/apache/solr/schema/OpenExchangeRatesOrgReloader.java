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
package org.apache.solr.schema;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.AbstractSolrEventListener;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * An event listener to reload currency rates for all CurrencyFieldType fields that use OpenExchangeRatesOrgProvider.
 * Opening a new IndexSearcher will reload the currency rates.
 * To use it, set up event listeners in your solrconfig.xml:
 *
 * <pre>
 *   &lt;listener event="newSearcher" class="org.apache.solr.schema.OpenExchangeRatesOrgReloader"/&gt;
 *   &lt;listener event="firstSearcher" class="org.apache.solr.schema.OpenExchangeRatesOrgReloader"/&gt;
 * </pre>
 * <p>
 * The rates will be reloaded after each commit for all CurrencyFieldType fields in your schema that have
 * providerClass="solr.OpenExchangeRatesOrgProvider".
 */
public class OpenExchangeRatesOrgReloader extends AbstractSolrEventListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public OpenExchangeRatesOrgReloader(SolrCore core) {
    super(core);
  }

  @Override
  public void init(NamedList<?> args) {
    reloadCurrencyRates(getCore().getLatestSchema());
  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    log.debug("newSearcher");
    reloadCurrencyRates(newSearcher.getSchema());
  }

  public void reloadCurrencyRates(IndexSchema schema) {
    log.debug("reloadCurrencyRates");
    for (SchemaField field : schema.getFields().values()) {
      FieldType type = field.getType();
      if (type instanceof CurrencyFieldType) {
        CurrencyFieldType fieldType = (CurrencyFieldType) type;
        if (fieldType.getProvider() instanceof OpenExchangeRatesOrgProvider) {
          OpenExchangeRatesOrgProvider provider = (OpenExchangeRatesOrgProvider) fieldType.getProvider();
          provider.reloadIfExpired();
        }
      }
    }
  }
}
