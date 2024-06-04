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

import org.apache.lucene.monitor.MonitorFields;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.solr.monitor.MonitorConstants.QUERY_MATCH_TYPE_KEY;
import static org.apache.solr.monitor.MonitorConstants.WRITE_TO_DOC_LIST_KEY;

public class NonDistributedMonitorTest extends SolrTestCaseJ4 {

    @BeforeClass
    public static void beforeTests() throws Exception {
        initCore("solrconfig-not-distributed.xml", "schema-aliasing.xml");
    }

    @Test
    public void coexistWithRegularDocumentsTest() throws Exception {
        String regularChain = "regular-ole-document";
        addDoc(adoc("id", "0", "content_s", "some unremarkable content"), regularChain);
        addDoc(commit(), regularChain);
        String monitorChain = "monitor";
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
                        QUERY_MATCH_TYPE_KEY,
                        "simple",
                        WRITE_TO_DOC_LIST_KEY,
                        "true"
                };

        assertQ(req(params), "//*[@numFound='1']");
    }

    @Test
    public void queryStringIdTest() throws Exception {
        String monitorChain = "monitor";
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
                        QUERY_MATCH_TYPE_KEY,
                        "simple",
                        WRITE_TO_DOC_LIST_KEY,
                        "true"
                };

        assertQ(req(params), "//*[@numFound='2']");
    }
}
