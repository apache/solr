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
package org.apache.solr.update.processor;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

public class NumFieldLimitingUpdateRequestProcessorFactoryTest extends SolrTestCase {

  private NumFieldLimitingUpdateRequestProcessorFactory factory = null;

  @Before
  public void initFactory() {
    factory = new NumFieldLimitingUpdateRequestProcessorFactory();
  }

  @Test
  public void testReportsErrorIfMaximumFieldsNotProvided() {
    final var initArgs = new NamedList<>();
    final IllegalArgumentException thrown =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              factory.init(initArgs);
            });
    assertThat(thrown.getMessage(), Matchers.containsString("maxFields parameter is required"));
    assertThat(thrown.getMessage(), Matchers.containsString("no value was provided"));
  }

  @Test
  public void testReportsErrorIfMaximumFieldsIsInvalid() {
    final var initArgs = new NamedList<>();
    initArgs.add("maxFields", "nonIntegerValue");
    IllegalArgumentException thrown =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              factory.init(initArgs);
            });
    assertThat(
        thrown.getMessage(),
        Matchers.containsString("maxFields must be configured as a non-null <int>"));

    initArgs.clear();
    initArgs.add("maxFields", Integer.valueOf(-5));
    thrown =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              factory.init(initArgs);
            });
    assertThat(
        thrown.getMessage(), Matchers.containsString("maxFields must be a positive integer"));
  }

  @Test
  public void testCorrectlyParsesAllConfigurationParams() {
    final var initArgs = new NamedList<>();
    initArgs.add("maxFields", Integer.valueOf(123));
    initArgs.add("warnOnly", Boolean.TRUE);

    factory.init(initArgs);

    assertEquals(123, factory.getFieldThreshold());
    assertEquals(true, factory.getWarnOnly());
  }
}
