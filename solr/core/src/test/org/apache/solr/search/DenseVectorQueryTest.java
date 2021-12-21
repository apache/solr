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
package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DenseVectorQueryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-dense-vector.xml", "schema-densevector.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
    assertU(adoc("id", "1", "vector", "[1.1, 2.3, 1.0, 2.3]"));
    assertU(adoc("id", "2", "vector", "[20.1, 211.3, 1.0, 2.3]"));
    assertU(adoc("id", "3", "vector", "[1.1, 10.3, 1.0, 2.3]"));
    assertU(adoc("id", "4", "vector", "[1.1, 2.3, 4.0, 2.3]"));
    assertU(adoc("id", "5", "vector", "[2.1, 2.3, 1.0, 2.3]"));
    assertU(adoc("id", "6", "vector", "[21.1, 223.3, 1.0, 2.3]"));
    assertU(adoc("id", "7", "vector", "[24.1, 211.3, 1.0, 2.3]"));
    assertU(commit());
  }

  @Test
  public void storedVectorAreReturned() {
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add("q", "id:1", "fl", "vector", "rows", "1");

    assertQ(req(params, "indent", "on"), "*[count(//doc)=1]",
            "//result/doc[1]/str[@name='vector'][.='[1.1, 2.3, 1.0, 2.3]']"
    );
  }

  @Test
  public void testDenseVectorQuery() {
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add("q", "{!knn f=vector v=[21,200,1,2] k=3}");

    assertQ(req(params, "indent", "on"), "*[count(//doc)=3]",
            "//result/doc[1]/str[@name='id'][.='2']",
            "//result/doc[2]/str[@name='id'][.='7']",
            "//result/doc[3]/str[@name='id'][.='6']"
    );
  }
}
