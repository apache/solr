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
package org.apache.solr.languagemodels.store.rest;

import org.apache.solr.languagemodels.TestLanguageModelBase;
import org.junit.After;
import org.junit.Test;

public class TestLargeLanguageModelStoreInitialization extends TestLanguageModelBase {

  @After
  public void cleanUp() throws Exception {
    afterTest();
  }

  @Test
  public void largeLanguageModelStore_whenUpdateRequestComponentConfigured_shouldBeInitialized()
      throws Exception {
    setupTest("solrconfig-document-enrichment.xml", "schema-language-models.xml", false, false);

    assertJQ(LargeLanguageModelStore.REST_END_POINT, "/responseHeader/status==0");
    assertJQ(LargeLanguageModelStore.REST_END_POINT, "/models==[]");
  }

  /*
   * This test verifies that the LargeLanguageModelStore REST managed resource is not registered when the Solr core has
   * no component that needs it.
   * LargeLanguageModelStore isn't wired up unconditionally — it's registered on demand by
   * DocumentEnrichmentUpdateProcessorFactory.inform() (DocumentEnrichmentUpdateProcessorFactory.java:165), which
   * calls LargeLanguageModelStore.registerManagedLargeLanguageModelStore(...).
   * That factory only gets instantiated if a solrconfig.xml actually declares an updateRequestProcessorChain using it.
   */
  @Test
  public void largeLanguageModelStore_whenNoUpdateRequestProcessorChain_shouldNotBeInitialized()
      throws Exception {
    setupTest(
        "solrconfig-language-models-no-components.xml", "schema-language-models.xml", false, false);
    assertJQ(
        LargeLanguageModelStore.REST_END_POINT,
        "/responseHeader/status==400",
        "/error/msg=='No REST managed resource registered for path "
            + LargeLanguageModelStore.REST_END_POINT
            + "'");
  }
}
