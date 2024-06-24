/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.monitor;

import static org.apache.solr.monitor.MonitorConstants.WRITE_TO_DOC_LIST_KEY;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.stream.IntStream;
import org.apache.lucene.monitor.MonitorFields;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SingleCoreMonitorSolrTest extends SolrTestCaseJ4 {

  private final String monitorChain = "monitor";

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-single-core.xml", "schema-aliasing.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void coexistWithRegularDocumentsTest() throws Exception {
    String regularChain = "regular-ole-document";
    addDoc(adoc("id", "0", "content_s", "some unremarkable content"), regularChain);
    addDoc(commit(), regularChain);
    addDoc(adoc("id", "1", MonitorFields.MONITOR_QUERY, "content_s:test"), monitorChain);
    addDoc(commit(), monitorChain);
    URL url = getClass().getResource("/monitor/multi-doc-batch.json");
    String json = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);

    String[] params =
        new String[] {
          CommonParams.SORT,
          "id desc",
          CommonParams.JSON,
          json,
          CommonParams.QT,
          "/reverseSearch",
          WRITE_TO_DOC_LIST_KEY,
          "true"
        };

    assertQ(req(params), "//*[@numFound='1']");
  }

  @Test
  public void queryStringIdTest() throws Exception {
    addDoc(adoc("id", "1", MonitorFields.MONITOR_QUERY, "id:4"), monitorChain);
    addDoc(adoc("id", "2", MonitorFields.MONITOR_QUERY, "id:4"), monitorChain);
    addDoc(commit(), monitorChain);
    URL url = getClass().getResource("/monitor/multi-doc-batch.json");
    String json = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);

    String[] params =
        new String[] {
          CommonParams.SORT,
          "id desc",
          CommonParams.JSON,
          json,
          CommonParams.QT,
          "/reverseSearch",
          WRITE_TO_DOC_LIST_KEY,
          "true"
        };

    assertQ(req(params), "//*[@numFound='2']");
  }

  @Test
  public void missingQueryFieldTest() {
    var ex = assertThrows(SolrException.class, () -> addDoc(adoc("id", "1"), monitorChain));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    String lowerCaseMessage = ex.getMessage().toLowerCase(Locale.ROOT);
    assertTrue(lowerCaseMessage.contains("missing"));
    assertTrue(lowerCaseMessage.contains("mandatory"));
    assertTrue(ex.getMessage().contains(MonitorFields.MONITOR_QUERY));
    assertTrue(lowerCaseMessage.contains("field"));
  }

  @Test
  public void unsupportedFieldTest() {
    String unsupportedField1 = "unsupported2_s";
    String unsupportedField2 = "unsupported2_s";
    var ex =
        assertThrows(
            SolrException.class,
            () ->
                addDoc(
                    adoc(
                        "id",
                        "1",
                        MonitorFields.MONITOR_QUERY,
                        "content_s:test",
                        unsupportedField1,
                        "a",
                        unsupportedField2,
                        "b"),
                    monitorChain));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    String lowerCaseMessage = ex.getMessage().toLowerCase(Locale.ROOT);
    assertTrue(lowerCaseMessage.contains("unsupported"));
    assertTrue(lowerCaseMessage.contains("fields"));
    assertTrue(lowerCaseMessage.contains(unsupportedField1));
    assertTrue(lowerCaseMessage.contains(unsupportedField2));
  }

  @Test
  public void multiPassPresearcherTest() throws Exception {
    addDoc(
        adoc(
            "id", "0", MonitorFields.MONITOR_QUERY, "content0_s:\"elevator stairs and escalator\""),
        monitorChain);
    addDoc(
        adoc("id", "1", MonitorFields.MONITOR_QUERY, "content0_s:\"elevator test\""), monitorChain);
    addDoc(
        adoc("id", "2", MonitorFields.MONITOR_QUERY, "content0_s:\"stairs test\""), monitorChain);
    addDoc(
        adoc("id", "3", MonitorFields.MONITOR_QUERY, "content0_s:\"elevator stairs\""),
        monitorChain);
    addDoc(commit(), monitorChain);
    URL url = getClass().getResource("/monitor/single-doc-batch.json");
    String json = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);

    String[] params =
        new String[] {
          CommonParams.SORT, "id desc", CommonParams.JSON, json, CommonParams.QT, "/reverseSearch"
        };

    assertQ(
        req(params),
        "/response/lst[@name=\"monitor\"]/int[@name=\"queriesRun\"]/text()='1'",
        "/response/lst[@name=\"monitor\"]/lst[@name=\"monitorDocuments\"]/lst[@name=\"0\"]/int[@name=\"monitorDocument\"]/text()='0'",
        "/response/lst[@name=\"monitor\"]/lst[@name=\"monitorDocuments\"]/lst[@name=\"0\"]/arr[@name=\"queries\"]/str/text()='3'");
  }

  @Test
  public void manySegmentsQuery() throws Exception {
    int count = 10_000;
    IntStream.range(0, count)
        .forEach(
            i -> {
              try {
                addDoc(
                    adoc(
                        "id",
                        Integer.toString(i),
                        MonitorFields.MONITOR_QUERY,
                        "content_s:\"elevator stairs\""),
                    monitorChain);
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    addDoc(commit(), monitorChain);
    URL url = getClass().getResource("/monitor/multi-doc-batch.json");
    String json = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);

    String[] params =
        new String[] {
          CommonParams.SORT, "id desc", CommonParams.JSON, json, CommonParams.QT, "/reverseSearch"
        };

    assertQ(
        req(params),
        "/response/lst[@name=\"monitor\"]/int[@name=\"queriesRun\"]/text()='" + count + "'",
        "/response/lst[@name=\"monitor\"]/lst[@name=\"monitorDocuments\"]/lst[@name=\"0\"]/int[@name=\"monitorDocument\"]/text()='0'",
        "/response/lst[@name=\"monitor\"]/lst[@name=\"monitorDocuments\"]/lst[@name=\"0\"]/arr[@name=\"queries\"]/str/text()='0'",
        "count(/response/lst[@name=\"monitor\"]/lst[@name=\"monitorDocuments\"]/lst[@name=\"0\"]/arr[@name=\"queries\"]/str)="
            + count);

    IntStream.range(count / 2, count)
        .forEach(
            i -> {
              try {
                addDoc(
                    adoc(
                        "id",
                        Integer.toString(i),
                        MonitorFields.MONITOR_QUERY,
                        "content_s:\"x y\""),
                    monitorChain);
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    addDoc(commit(), monitorChain);
    assertQ(
        req(params),
        "/response/lst[@name=\"monitor\"]/int[@name=\"queriesRun\"]/text()='" + (count / 2) + "'",
        "/response/lst[@name=\"monitor\"]/lst[@name=\"monitorDocuments\"]/lst[@name=\"0\"]/int[@name=\"monitorDocument\"]/text()='0'",
        "/response/lst[@name=\"monitor\"]/lst[@name=\"monitorDocuments\"]/lst[@name=\"0\"]/arr[@name=\"queries\"]/str/text()='0'",
        "count(/response/lst[@name=\"monitor\"]/lst[@name=\"monitorDocuments\"]/lst[@name=\"0\"]/arr[@name=\"queries\"]/str)="
            + (count / 2));
  }
}
