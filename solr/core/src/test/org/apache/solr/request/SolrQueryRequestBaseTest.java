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
package org.apache.solr.request;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

public class SolrQueryRequestBaseTest extends SolrTestCase {

  public void testParamStringIsOriginalParams() {
    SolrParams original = new ModifiableSolrParams().set("q", "original");
    SolrQueryRequestBase req = new SolrQueryRequestBase(null, original) {};

    assertEquals("q=original", req.getParamString());

    req.setParams(new ModifiableSolrParams().set("q", "changed").set("rows", "5"));

    assertEquals("changed", req.getParams().get("q"));
    assertEquals("original", req.getOriginalParams().get("q"));
    assertEquals(
        "getParamString documents the original params, not the ones set later",
        "q=original",
        req.getParamString());
  }
}
