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
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestModulusFunctions extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema15.xml");
  }

  @After
  public void cleanup() {
    clearIndex();
    assertU(optimize());
  }

  @Test
  public void testModulus() {
    int l = 1 << 24;
    int[] a = new int[] {l - 3, l - 2, l - 1, l, l + 1, l + 2};

    // The existing implementation casts int to float, with 24bit mantissa
    float[] f = new float[] {1, 2, 0, 1, 1, 0}; // these...

    // The newer implementation casts int to double, with 52bit mantissa
    double[] d = new double[] {1, 2, 0, 1, 2, 0}; // ...differ

    for (int i = 0; i < a.length; i++) {
      assertEquals("int -> float modulus", f[i], (float) a[i] % 3, 0.0);
      assertEquals("int -> double modulus", d[i], (double) a[i] % 3, 0.0);
    }
  }

  @Test
  public void testMod() throws Exception {
    int a = 1 << 24;
    for (int i = 0, b = a - 3; b < a + 3; b++, i++) {
      assertU(adoc("id", Integer.toString(i), "foo_i", Integer.toString(b)));
    }

    assertU(commit());
    assertJQ(
        req(
            "defType", "lucene",
            "q", "*:*",
            "fl", "id,foo_i,m:mod(foo_i,3)"),
        "/response/docs/[0]/foo_i==16777213",
        "/response/docs/[0]/m==1.0",
        "/response/docs/[1]/foo_i==16777214",
        "/response/docs/[1]/m==2.0",
        "/response/docs/[2]/foo_i==16777215",
        "/response/docs/[2]/m==0.0",
        "/response/docs/[3]/foo_i==16777216",
        "/response/docs/[3]/m==1.0",
        "/response/docs/[4]/foo_i==16777217",
        "/response/docs/[4]/m==2.0",
        "/response/docs/[5]/foo_i==16777218",
        "/response/docs/[5]/m==0.0");
  }
}
