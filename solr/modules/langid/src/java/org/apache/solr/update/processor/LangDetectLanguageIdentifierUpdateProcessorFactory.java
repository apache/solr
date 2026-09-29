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
package org.apache.solr.update.processor;

import io.github.azagniotov.language.LanguageDetectionOrchestrator;
import io.github.azagniotov.language.LanguageDetectionSettings;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Identifies the language of a set of input fields using
 * https://github.com/azagniotov/language-detection
 *
 * <p>The UpdateProcessorChain config entry can take a number of parameters which may also be passed
 * as HTTP parameters on the update request and override the defaults. Here is the simplest
 * processor config possible:
 *
 * <pre class="prettyprint" >
 * &lt;processor class=&quot;org.apache.solr.update.processor.LangDetectLanguageIdentifierUpdateProcessorFactory&quot;&gt;
 *   &lt;str name=&quot;langid.fl&quot;&gt;title,text&lt;/str&gt;
 *   &lt;str name=&quot;langid.langField&quot;&gt;language_s&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * See <a
 * href="https://solr.apache.org/guide/solr/latest/indexing-guide/language-detection.html">https://solr.apache.org/guide/solr/latest/indexing-guide/language-detection.html</a>
 *
 * @since 3.5
 */
public class LangDetectLanguageIdentifierUpdateProcessorFactory
    extends UpdateRequestProcessorFactory implements SolrCoreAware, LangIdParams {

  protected SolrParams defaults;
  protected SolrParams appends;
  protected SolrParams invariants;

  // Built once for the JVM lifetime; LanguageDetectionOrchestrator is thread-safe and
  // stateless after construction, so sharing it across all factory instances is safe.
  static final LanguageDetectionOrchestrator ORCHESTRATOR =
      LanguageDetectionOrchestrator.fromSettings(
          LanguageDetectionSettings.fromAllIsoCodes639_1().build());

  @Override
  public void inform(SolrCore core) {}

  /**
   * The UpdateRequestProcessor may be initialized in solrconfig.xml similarly to a RequestHandler,
   * with defaults, appends and invariants.
   *
   * @param args a NamedList with the configuration parameters
   */
  @Override
  public void init(NamedList<?> args) {

    if (args != null) {
      Object o;
      o = args.get("defaults");
      if (o instanceof NamedList) {
        defaults = ((NamedList<?>) o).toSolrParams();
      } else {
        defaults = args.toSolrParams();
      }
      o = args.get("appends");
      if (o instanceof NamedList) {
        appends = ((NamedList<?>) o).toSolrParams();
      }
      o = args.get("invariants");
      if (o instanceof NamedList) {
        invariants = ((NamedList<?>) o).toSolrParams();
      }
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    if (req != null) {
      SolrPluginUtils.setDefaults(req, defaults, appends, invariants);
    }
    return new LangDetectLanguageIdentifierUpdateProcessor(req, rsp, next, ORCHESTRATOR);
  }
}
