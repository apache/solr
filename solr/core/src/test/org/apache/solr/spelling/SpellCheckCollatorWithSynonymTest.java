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
package org.apache.solr.spelling;

import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.SpellCheckComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class SpellCheckCollatorWithSynonymTest extends SolrTestCaseJ4 {

  // ./gradlew -p solr/core test --tests SpellCheckCollatorWithSynonymTest

  // TODO o.a.s.s.DirectSolrSpellChecker does not seem to handle synonyms boost correctly: synonym
  // input is `panthera pardus, leopard|0.6`
  //
  // 1. One scenario generates a single token `leopard|0.6`
  // (value,frequency)
  // title_en:leopard|0.6: 0
  // title_en:leopard: 3
  // Q. should these be the same frequency?
  //
  // 2. Another scenario a list `leopard`,`0`,`6`
  // (value,frequency):
  // spelltest_t:leopard: 3
  // spelltest_t:0: 1
  // spelltest_t:6: 1
  // Q. should `0` and `6` be here? they introduce a lot of noise in the collation
  //
  // Generated tokens with
  // TokenizerChain(org.apache.solr.analysis.MockTokenizerFactory@258bab26,
  // org.apache.lucene.analysis.synonym.SynonymGraphFilterFactory@3c8e9991,
  // org.apache.lucene.analysis.core.StopFilterFactory@985a564,
  // org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilterFactory@5fe66db9,
  // org.apache.lucene.analysis.core.LowerCaseFilterFactory@274c3ff8,
  // org.apache.lucene.analysis.miscellaneous.KeywordMarkerFilterFactory@6fc67891,
  // org.apache.lucene.analysis.en.PorterStemFilterFactory@4b4c5e88,
  // org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilterFactory@272b79fd):

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-collapseqparser.xml", "schema11.xml");

    assertU(adoc("id", "1", "spelltest_t", "Lorem panthera pardus ipsum 6 dolor sit amet"));
    assertU(adoc("id", "2", "spelltest_t", "Lorem big cat pardus ipsum 0 dolor sit amet leopard"));
    assertU(commit());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testCollationWithHypens() {
    SolrCore core = h.getCore();
    SearchComponent speller = core.getSearchComponent("spellcheck");
    assertNotNull("speller is null and it shouldn't be", speller);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(SpellCheckComponent.COMPONENT_NAME, "true");
    params.add(SpellingParams.SPELLCHECK_BUILD, "true");
    params.add(SpellingParams.SPELLCHECK_COUNT, "10");
    params.add(SpellingParams.SPELLCHECK_COLLATE, "true");
    params.add(SpellingParams.SPELLCHECK_ALTERNATIVE_TERM_COUNT, "5");
    params.add("df", "spelltest_t");
    params.add(SpellCheckComponent.SPELLCHECK_DICT, "direct_spelltest_t");
    params.add(SpellCheckComponent.SPELLCHECK_Q, "panthera pardus cats");

    SolrRequestHandler handler = core.getRequestHandler("/spellCheckCompRH_Direct");
    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.addResponseHeader(new SimpleOrderedMap());
    SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
    handler.handleRequest(req, rsp);
    req.close();

    NamedList values = rsp.getValues();
    NamedList spellCheck = (NamedList) values.get("spellcheck");
    NamedList collationHolder = (NamedList) spellCheck.get("collations");
    List<String> collations = collationHolder.getAll("collation");

    assertEquals(1, collations.size());
    String collation = collations.iterator().next();
    assertEquals("Incorrect collation: " + collation, "leopard cat", collation);
  }
}
