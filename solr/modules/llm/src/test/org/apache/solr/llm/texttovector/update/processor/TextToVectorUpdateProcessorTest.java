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
package org.apache.solr.llm.texttovector.update.processor;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.llm.TestLlmBase;
import org.apache.solr.llm.texttovector.store.rest.ManagedTextToVectorModelStore;
import org.junit.BeforeClass;
import org.junit.Test;


public class TextToVectorUpdateProcessorTest extends TestLlmBase {

    @BeforeClass
    public static void init() throws Exception {
        setupTest("solrconfig-llm-indexing.xml", "schema.xml", false, false);

    }

    @Test
    public void processAdd_inputField_shouldVectoriseInputField()
            throws Exception {
        loadModel("dummy-model.json");
        assertU(adoc("id", "99", "_text_", "Vegeta is the saiyan prince."));
        assertU(adoc("id", "98", "_text_", "Vegeta is the saiyan prince."));
        assertU(commit());

        final String solrQuery = "*:*";
        final SolrQuery query = new SolrQuery();
        query.setQuery(solrQuery);
        query.add("fl", "id,vector");

        assertJQ(
                "/query" + query.toQueryString(),
                "/response/numFound==2]",
                "/response/docs/[0]/id=='99'",
                "/response/docs/[0]/vector==[1.0, 2.0, 3.0, 4.0]",
                "/response/docs/[1]/id=='98'",
                "/response/docs/[1]/vector==[1.0, 2.0, 3.0, 4.0]");

        restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/dummy-1");
    }

    /*
    This test looks for the 'dummy-1' model, but such model is not loaded, the model store is empty, so the update fails
     */
    @Test
    public void processAdd_modelNotFound_shouldRaiseException() {
        assertFailedU("This update should fail but actually succeeded", adoc("id", "99", "_text_", "Vegeta is the saiyan prince."));

        checkUpdateU(adoc("id", "99", "_text_", "Vegeta is the saiyan prince."),
                "/response/lst[@name='error']/str[@name='msg']=\"The model requested 'dummy-1' can't be found in the store: /schema/text-to-vector-model-store\"",
                "/response/lst[@name='error']/int[@name='code']='400'");
    }

    @Test
    public void processAdd_emptyInputField_shouldLogAndIndexWithNoVector() throws Exception {
        loadModel("dummy-model.json");
        assertU(adoc("id", "99", "_text_", ""));
        assertU(adoc("id", "98", "_text_", "Vegeta is the saiyan prince."));
        assertU(commit());

        final String solrQuery = "*:*";
        final SolrQuery query = new SolrQuery();
        query.setQuery(solrQuery);
        query.add("fl", "id,vector");

        assertJQ(
                "/query" + query.toQueryString(),
                "/response/numFound==2]",
                "/response/docs/[0]/id=='99'",
                "!/response/docs/[0]/vector==", //no vector field for the document 99
                "/response/docs/[1]/id=='98'",
                "/response/docs/[1]/vector==[1.0, 2.0, 3.0, 4.0]");

        restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/dummy-1");
    }

    @Test
    public void processAdd_nullInputField_shouldLogAndIndexWithNoVector() throws Exception {
        loadModel("dummy-model.json");
        assertU(adoc("id", "99", "_text_", "Vegeta is the saiyan prince."));
        assertU(adoc("id", "98"));
        assertU(commit());

        final String solrQuery = "*:*";
        final SolrQuery query = new SolrQuery();
        query.setQuery(solrQuery);
        query.add("fl", "id,vector");

        assertJQ(
                "/query" + query.toQueryString(),
                "/response/numFound==2]",
                "/response/docs/[0]/id=='99'",
                "/response/docs/[0]/vector==[1.0, 2.0, 3.0, 4.0]",
                "/response/docs/[1]/id=='98'",
                "!/response/docs/[1]/vector==");//no vector field for the document 98

        restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/dummy-1");
    }


}
