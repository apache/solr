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

import static org.apache.solr.monitor.MonitorConstants.QUERY_MATCH_TYPE_KEY;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.lucene.monitor.MonitorFields;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParallelMonitorSolrQueryTest extends MonitorSolrQueryTest {

  @BeforeClass
  public static void beforeSuperClass() {
    configString = "solrconfig-parallel.xml";
    schemaString = "schema-aliasing.xml";
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void manySegmentsQuery() throws Exception {
    int count = 10_000;
    IntStream.range(0, count)
        .forEach(
            i -> {
              try {
                index(
                    id,
                    Integer.toString(i),
                    MonitorFields.MONITOR_QUERY,
                    "content_s:\"elevator stairs\"");
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch",
          QUERY_MATCH_TYPE_KEY,
          "simple"
        };

    QueryResponse response = query(params);
    validate(response, 0, 0, "0");
    assertEquals(count, ((Map) response.getResponse().get("monitor")).get("queriesRun"));
    assertEquals(
        count,
        ((List)
                ((Map)
                        ((Map)
                                ((Map) response.getResponse().get("monitor"))
                                    .get("monitorDocuments"))
                            .get(0))
                    .get("queries"))
            .size());

    IntStream.range(count / 2, count)
        .forEach(
            i -> {
              try {
                index(id, Integer.toString(i), MonitorFields.MONITOR_QUERY, "content_s:\"x y\"");
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    commit();

    response = query(params);
    validate(response, 0, 0, "0");
    assertEquals(count / 2, ((Map) response.getResponse().get("monitor")).get("queriesRun"));
    assertEquals(
        count / 2,
        ((List)
                ((Map)
                        ((Map)
                                ((Map) response.getResponse().get("monitor"))
                                    .get("monitorDocuments"))
                            .get(0))
                    .get("queries"))
            .size());
  }

  @Test
  public void coexistWithRegularDocumentsTest() throws Exception {
    index(id, "0", "content_s", "some unremarkable content");
    index(
        id,
        "1",
        "________________________________monitor_alias_content_s_0",
        "some more unremarkable content");
    commit();
    index(id, "2", MonitorFields.MONITOR_QUERY, "content_s:test");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/multi-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch",
          QUERY_MATCH_TYPE_KEY,
          "simple"
        };

    QueryResponse response = query(params);
    validate(response, 4, 0, "2");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void multiPassPresearcherTest() throws Exception {
    index(id, "0", MonitorFields.MONITOR_QUERY, "content0_s:\"elevator stairs and escalator\"");
    index(id, "1", MonitorFields.MONITOR_QUERY, "content0_s:\"elevator test\"");
    index(id, "2", MonitorFields.MONITOR_QUERY, "content0_s:\"stairs test\"");
    index(id, "3", MonitorFields.MONITOR_QUERY, "content0_s:\"elevator stairs\"");
    commit();
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    Object[] params =
        new Object[] {
          CommonParams.SORT,
          id + " desc",
          CommonParams.JSON,
          read("/monitor/single-doc-batch.json"),
          CommonParams.QT,
          "/reverseSearch",
          QUERY_MATCH_TYPE_KEY,
          "simple"
        };

    QueryResponse response = query(params);
    assertEquals(1, ((Map<String, ?>) response.getResponse().get("monitor")).get("queriesRun"));
    validate(response, 0, 0, "3");
  }

  @Override
  protected boolean supportsWriteToDocList() {
    return false;
  }
}
