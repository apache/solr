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
package org.apache.solr.common.util;

import org.apache.solr.SolrTestCase;

public class TestGlobPatternUtil extends SolrTestCase {

  public void testMatches() {
    assertTrue(GlobPatternUtil.matches("*_str", "user_str"));
    assertFalse(GlobPatternUtil.matches("*_str", "str_user"));
    assertTrue(GlobPatternUtil.matches("str_*", "str_user"));
    assertFalse(GlobPatternUtil.matches("str_*", "user_str"));
    assertTrue(GlobPatternUtil.matches("str?", "str1"));
    assertFalse(GlobPatternUtil.matches("str?", "str_user"));
    assertTrue(GlobPatternUtil.matches("user_*_str", "user_type_str"));
    assertFalse(GlobPatternUtil.matches("user_*_str", "user_str"));
  }
}
