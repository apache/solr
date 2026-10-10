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
package org.apache.solr.servlet;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequestBase;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Method-level test for {@link HttpSolrCall#addCollectionParamIfNeeded(List)}: a {@code collection}
 * parameter supplied in a POST form body must not be replaced by the list of collections derived
 * from the request path when that list holds more than one collection.
 */
public class HttpSolrCallCollectionParamTest extends SolrTestCase {

  @BeforeClass
  public static void setUpOnce() {
    assumeWorkingMockito();
  }

  @Test
  public void testAddCollectionParamIfNeededKeepsBodyValueForTwoCollectionPath() {
    HttpSolrCall call = callWithBodyCollection("bodycoll");

    // The request path names an alias that resolves to two collections.
    call.addCollectionParamIfNeeded(List.of("emptycollection", "doccollection"));

    assertEquals("bodycoll", call.solrReq.getParams().get(COLLECTION_PROP));
  }

  @Test
  public void testAddCollectionParamIfNeededKeepsNonexistentBodyValue() {
    HttpSolrCall call = callWithBodyCollection("nosuchcoll");

    call.addCollectionParamIfNeeded(List.of("emptycollection", "doccollection"));

    // A name that is not an alias resolves to itself, so the value is kept as sent and the
    // request fails later, as a request for a missing collection, the same as a URL collection
    // parameter naming a missing collection. On the base code the value was replaced by the
    // joined path list here, and the request silently used the path collections instead.
    assertEquals("nosuchcoll", call.solrReq.getParams().get(COLLECTION_PROP));
  }

  private static HttpSolrCall callWithBodyCollection(String bodyCollection) {
    CoreContainer cores = mock(CoreContainer.class);
    when(cores.isZooKeeperAware()).thenReturn(true);
    when(cores.getAliases()).thenReturn(Aliases.EMPTY);

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getServletPath()).thenReturn("/bothalias");
    when(request.getPathInfo()).thenReturn("/select");
    HttpServletResponse response = mock(HttpServletResponse.class);

    HttpSolrCall call = new HttpSolrCall(cores, request, response, false);
    // The URL query string carries no collection parameter...
    call.queryParams = SolrRequestParsers.parseQueryString("q=*:*");
    // ...but the parsed request, which merges in the POST form body, carries one.
    ModifiableSolrParams requestParams = new ModifiableSolrParams();
    requestParams.set(COLLECTION_PROP, bodyCollection);
    requestParams.set("q", "*:*");
    call.solrReq = new SolrQueryRequestBase(null, requestParams);
    return call;
  }
}
