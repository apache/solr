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
package org.apache.solr.cloud.api.collections;

import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.junit.After;
import org.junit.Test;

public class CollectionHandlingUtilsTest extends SolrTestCase {

  private static final String MESSAGE_PARAM = "waitForFinalState";
  private static final String ENV_PROP = "solr.cloud.waitForFinalStateEnvFallbackTest.enabled";

  @After
  public void clearProperty() {
    System.clearProperty(ENV_PROP);
  }

  @Test
  public void testMessageUnsetEnvUnsetFallsBackToDefault() {
    ZkNodeProps message = new ZkNodeProps(Map.of());
    assertTrue(
        CollectionHandlingUtils.getBoolWithEnvFallback(message, MESSAGE_PARAM, ENV_PROP, true));
    assertFalse(
        CollectionHandlingUtils.getBoolWithEnvFallback(message, MESSAGE_PARAM, ENV_PROP, false));
  }

  @Test
  public void testMessageUnsetEnvSetFalseOverridesDefaultTrue() {
    System.setProperty(ENV_PROP, "false");
    ZkNodeProps message = new ZkNodeProps(Map.of());
    assertFalse(
        CollectionHandlingUtils.getBoolWithEnvFallback(message, MESSAGE_PARAM, ENV_PROP, true));
  }

  @Test
  public void testMessageUnsetEnvSetTrueOverridesDefaultFalse() {
    System.setProperty(ENV_PROP, "true");
    ZkNodeProps message = new ZkNodeProps(Map.of());
    assertTrue(
        CollectionHandlingUtils.getBoolWithEnvFallback(message, MESSAGE_PARAM, ENV_PROP, false));
  }

  @Test
  public void testExplicitMessageParamWinsOverEnvFallback() {
    System.setProperty(ENV_PROP, "true");
    ZkNodeProps messageFalse = new ZkNodeProps(Map.of(MESSAGE_PARAM, "false"));
    assertFalse(
        CollectionHandlingUtils.getBoolWithEnvFallback(
            messageFalse, MESSAGE_PARAM, ENV_PROP, false));

    System.setProperty(ENV_PROP, "false");
    ZkNodeProps messageTrue = new ZkNodeProps(Map.of(MESSAGE_PARAM, "true"));
    assertTrue(
        CollectionHandlingUtils.getBoolWithEnvFallback(messageTrue, MESSAGE_PARAM, ENV_PROP, true));
  }
}
