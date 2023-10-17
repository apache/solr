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

package org.apache.solr.ltr.model;

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FieldValueFeature;
import org.apache.solr.ltr.feature.ValueFeature;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAlternativeFormatWrapperModel extends TestRerankBase {

  private static final String featureStoreName = "test";

  static List<Feature> features = null;

  @Before
  public void setupBeforeClass() throws Exception {
    setuptest(false);
    assertU(adoc("id", "1", "title", "w1", "description", "w1", "popularity", "1"));
    assertU(adoc("id", "2", "title", "w2", "description", "w2", "popularity", "2"));
    assertU(adoc("id", "3", "title", "w3", "description", "w3", "popularity", "3"));
    assertU(adoc("id", "4", "title", "w4", "description", "w4", "popularity", "4"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity", "5"));
    assertU(commit());

    loadFeature(
        "popularity", FieldValueFeature.class.getName(), "test", "{\"field\":\"popularity\"}");
    loadFeature("const", ValueFeature.class.getName(), "test", "{\"value\":5}");
    features = new ArrayList<>();
    features.add(getManagedFeatureStore().getFeatureStore("test").get("popularity"));
    features.add(getManagedFeatureStore().getFeatureStore("test").get("const"));

    // TODO: anything else here?
  }

  @After
  public void cleanup() throws Exception {
    features = null;
    aftertest();
  }

  @Test
  public void testMissingFormat() throws Exception {
    // TODO
  }

  @Test
  public void testMissingContent() throws Exception {
    // TODO
  }

  @Test
  public void testSupportedFormat() throws Exception {
    // TODO
  }

  @Test
  public void testSupportedFormatBadContent() throws Exception {
    // TODO
  }

  @Test
  public void testUnsupportedFormat() throws Exception {
    // TODO
  }

  @Test
  public void testSomethingElse() throws Exception {
    // TODO
  }
}
