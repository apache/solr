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
package org.apache.solr.security;

import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;

public class PermissionTest extends SolrTestCaseJ4 {

  public void testLoad() {
    // Valid, currently predefined permissions
    for (String name : Set.of("read", "zk-read")) {
      Permission.load(Map.of("name", name, "role", "admin"));
    }
    // Invalid custom permissions (some of which were old valid predefined)
    for (String name :
        Set.of("metrics-history-read", "autoscaling-read", "autoscaling-write", "invalid-custom")) {
      SolrException e =
          assertThrows(
              SolrException.class, () -> Permission.load(Map.of("name", name, "role", "admin")));
      assertTrue(
          e.getMessage()
              .contains(
                  "Permission with name "
                      + name
                      + " is neither a pre-defined permission nor qualifies as a custom permission"));
    }
    // Valid custom permissions
    for (String name : Set.of("valid-custom")) {
      Permission.load(Map.of("name", name, "role", "admin", "path", "/foo"));
    }
  }
}
