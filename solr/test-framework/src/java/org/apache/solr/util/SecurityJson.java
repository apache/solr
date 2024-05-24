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
package org.apache.solr.util;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;

import java.util.Map;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.BasicAuthPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;

/**
 * Provides security.json constants for use in tests that enable security.
 *
 * <p>Many tests require a simple security.json with one admin user and the BasicAuthPlugin enabled;
 * such a configuration is represented by the SIMPLE constant here. Other variants of security.json
 * can be moved to this class if and when they are duplicated by two or more tests.
 */
public final class SecurityJson {

  private SecurityJson() {}

  public static final String USER = "solr";
  public static final String PASS = "SolrRocksAgain";
  public static final String USER_PASS = USER + ":" + PASS;

  public static final String SIMPLE =
      Utils.toJSONString(
          Map.of(
              "authorization",
              Map.of(
                  "class",
                  RuleBasedAuthorizationPlugin.class.getName(),
                  "user-role",
                  singletonMap(USER, "admin"),
                  "permissions",
                  singletonList(Map.of("name", "all", "role", "admin"))),
              "authentication",
              Map.of(
                  "class",
                  BasicAuthPlugin.class.getName(),
                  "blockUnknown",
                  true,
                  "credentials",
                  singletonMap(USER, getSaltedHashedValue(PASS)))));
}
