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

package org.apache.solr.handler.admin.api;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.LoggerInfo;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for the functionality offered in {@link NodeLoggingAPI} */
@SuppressWarnings({"unchecked", "rawtypes"})
public class NodeLoggingAPITest extends SolrTestCase {

  private CoreContainer mockCoreContainer;
  private LogWatcher mockLogWatcher;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setUpMocks() {
    mockCoreContainer = mock(CoreContainer.class);
    mockLogWatcher = mock(LogWatcher.class);
    when(mockCoreContainer.getLogging()).thenReturn(mockLogWatcher);
  }

  @Test
  public void testReliesOnLogWatcherToListLogLevels() {
    when(mockLogWatcher.getAllLevels())
        .thenReturn(List.of("ERROR", "WARN", "INFO", "DEBUG", "TRACE"));
    when(mockLogWatcher.getAllLoggers())
        .thenReturn(List.of(logInfo("org.a.s.Foo", "WARN", true), logInfo("org", null, false)));
    final var responseBody = new NodeLoggingAPI(mockCoreContainer).listAllLoggersAndLevels();

    assertEquals(5, responseBody.levels.size());
    MatcherAssert.assertThat(
        responseBody.levels, containsInAnyOrder("ERROR", "WARN", "INFO", "DEBUG", "TRACE"));

    assertEquals(2, responseBody.loggers.size());
    final var firstLogger = responseBody.loggers.get(0);
    assertEquals("org", firstLogger.name);
    assertEquals(null, firstLogger.level);
    assertFalse("Expected logger info to report 'unset'", firstLogger.set);
    final var secondLogger = responseBody.loggers.get(1);
    assertEquals("org.a.s.Foo", secondLogger.name);
    assertEquals("WARN", secondLogger.level);
    assertTrue("Expected logger info to report 'set'", secondLogger.set);
  }

  @Test
  public void testReliesOnLogWatcherToModifyLogLevels() {
    final var responseBody =
        new NodeLoggingAPI(mockCoreContainer)
            .modifyLocalLogLevel(List.of(new NodeLoggingAPI.LogLevelChange("o.a.s.Foo", "WARN")));

    assertNotNull(responseBody);
    assertNull("Expected error to be null but was " + responseBody.error, responseBody.error);
    verify(mockLogWatcher).setLogLevel("o.a.s.Foo", "WARN");
  }

  private SolrDocumentList logMessageDocList(String... logMessages) {
    final var docList = new SolrDocumentList();
    for (String logMessage : logMessages) {
      final var doc = new SolrDocument();
      doc.addField("message_s", logMessage);
      docList.add(doc);
    }
    return docList;
  }

  @Test
  public void testReliesOnLogWatcherToFetchLogMessages() {
    when(mockLogWatcher.getHistory(eq(123L), any()))
        .thenReturn(logMessageDocList("logmsg1", "logmsg2"));
    when(mockLogWatcher.getLastEvent()).thenReturn(123456L);
    when(mockLogWatcher.getHistorySize()).thenReturn(321);

    final var responseBody = new NodeLoggingAPI(mockCoreContainer).fetchLocalLogMessages(123L);

    assertNotNull(responseBody);
    assertNull("Expected error to be null but was " + responseBody.error, responseBody.error);
    assertNotNull("Expected 'info' subobject to be set, but was null", responseBody.info);
    assertEquals(Long.valueOf(123), responseBody.info.boundingTimeMillis);
    assertEquals(123456, responseBody.info.lastRecordTimestampMillis);
    assertEquals(321, responseBody.info.buffer);
    assertEquals(2, responseBody.docs.size());
    assertEquals("logmsg1", responseBody.docs.get(0).getFieldValue("message_s"));
    assertEquals("logmsg2", responseBody.docs.get(1).getFieldValue("message_s"));
  }

  @Test
  public void testReliesOnLogWatcherToSetMessageThreshold() {
    final var responseBody =
        new NodeLoggingAPI(mockCoreContainer)
            .setMessageThreshold(new NodeLoggingAPI.SetThresholdRequestBody("WARN"));

    assertNotNull(responseBody);
    assertNull("Expected error to be null but was " + responseBody.error, responseBody.error);
    verify(mockLogWatcher).setThreshold("WARN");
  }

  // The v1 logging APIs accept log level changes in the form set=logger:level&set=logger2:level2...
  // This test ensures that we can convert that format to that used by our v2 APIs.
  @Test
  public void testCorrectlyParsesV1LogLevelChanges() {
    final var levelChanges =
        NodeLoggingAPI.LogLevelChange.createRequestBodyFromV1Params(
            new String[] {"o.a.s.Foo:WARN", "o.a.s.Bar:INFO"});

    assertEquals(2, levelChanges.size());
    assertEquals("o.a.s.Foo", levelChanges.get(0).logger);
    assertEquals("WARN", levelChanges.get(0).level);
    assertEquals("o.a.s.Bar", levelChanges.get(1).logger);
    assertEquals("INFO", levelChanges.get(1).level);
  }

  private static class StubLoggerInfo extends LoggerInfo {
    private final boolean set;

    public StubLoggerInfo(String logger, String level, boolean set) {
      super(logger);
      this.level = level;
      this.set = set;
    }

    @Override
    public boolean isSet() {
      return set;
    }
  }

  private LoggerInfo logInfo(String logger, String level, boolean set) {
    return new StubLoggerInfo(logger, level, set);
  }
}
