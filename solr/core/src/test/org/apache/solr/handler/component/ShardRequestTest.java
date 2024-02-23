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
package org.apache.solr.handler.component;

import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class ShardRequestTest extends SolrTestCase {

  @Test
  public void testDefaultPurposeIsZero() {
    final ShardRequest sr = new ShardRequest();
    assertEquals(0, sr.purpose);
  }

  /**
   * Test that the constant stays constant. The constant's value is used in various places, ideally
   * directly via ShardRequest.PURPOSE_PRIVATE but possibly also indirectly via magic '1'
   * hard-coding. If the constant's value needs to change please carefully account for the code
   * impacted by that.
   */
  @Test
  public void testPurposePrivateIsOne() {
    assertEquals(1, ShardRequest.PURPOSE_PRIVATE);
  }

  @Test
  public void testToStringWithDefaultPurpose() {
    final ShardRequest sr = new ShardRequest();
    assertEquals("ShardRequest:{params=null, purpose=0, nResponses =0}", sr.toString());
  }

  @Test
  public void testToStringWithPurposePrivate() {
    final ShardRequest sr = new ShardRequest();
    sr.purpose = ShardRequest.PURPOSE_PRIVATE;
    assertEquals("ShardRequest:{params=null, purpose=1, nResponses =0}", sr.toString());
  }
}
