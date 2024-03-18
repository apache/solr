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
package org.apache.solr.analysis;

import static org.hamcrest.core.IsIterableContaining.hasItems;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.security.AllowListUrlChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test which asserts that shards.info=true works even if several shards are down It must return one
 * unique key per shard. See SOLR-14892
 */
public class TestShardsInfoResponse2 extends SolrTestCaseJ4 {



    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());



    @ClassRule public static final SolrJettyTestRule solrRule = new SolrJettyTestRule();

    private static SolrClient clientCore1;
    private static String shard1;



    @BeforeClass
    public static void setupSolr() throws Exception {
        System.setProperty(AllowListUrlChecker.DISABLE_URL_ALLOW_LIST, "true");

        Path configSet = createTempDir("configSet");
        copyMinConf(configSet.toFile());
        solrRule.startSolr(LuceneTestCase.createTempDir());

        solrRule.newCollection("core1").withConfigSet(configSet.toString()).create();
        clientCore1 = solrRule.getSolrClient("core1");

        String urlCore1 = solrRule.getBaseUrl() + "/" + "core1";
        shard1 = urlCore1.replaceAll("https?://", "");

        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("id", "1");
        doc.setField("subject", "batman");
        doc.setField("title", "foo bar");
        clientCore1.add(doc);
        clientCore1.commit();

    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (null != clientCore1) {
            clientCore1.close();
            clientCore1 = null;
        }
    }


    @Test
    @SuppressWarnings("unchecked")
    public void searchingWithShardsInfoMustNotReturnEmptyOrDuplicateKeys() throws  SolrServerException, IOException {
        try {
            SolrQuery query = new SolrQuery();
            query.setQuery("subject:batman");
            query.addField("id");
            query.addField("subject");
            //query.set("distrib", "true");
            query.set("shards", shard1);
            //query.set("shards", shard1 + "," + shard2);
            query.setRows(10)
                    .setParam(ShardParams.SHARDS_TOLERANT, true)
                    .setParam(ShardParams.SHARDS_INFO, true);


            // Test normal behavior
            QueryResponse response = clientCore1.query(query);
			log.info(response.toString());
            SolrDocumentList results = response.getResults();
            int size = results.size();
            assertEquals("should have 1 result", 1, size);


            // Test with 2 wrong shards added to it
            String shard2 = (solrRule.getBaseUrl() + "/" + "core2").replaceAll("https?://", "");
            String shard3 = (solrRule.getBaseUrl() + "/" + "core3").replaceAll("https?://", "");


            query.set("shards", shard1 + "," + shard2 + "," + shard3);
            QueryResponse response2 = clientCore1.query(query);
            log.info(response2.toString());
            SolrDocumentList results2 = response2.getResults();
            int size2 = results2.size();
            assertEquals("should have 1 result", 1, size2);




            var shardsInfo = response2.getResponse().get("shards.info");
            // We verify that there are no duplicate keys in case of 2 shards in error
            assertEquals(3, ((NamedList)shardsInfo).size());

            Collection<String> keys = new ArrayList<>();
            keys.add(((NamedList)shardsInfo).getName(0));
            keys.add(((NamedList)shardsInfo).getName(1));
            keys.add(((NamedList)shardsInfo).getName(2));

            MatcherAssert.assertThat(
                    (Iterable<String>) keys, hasItems("unknown_shard_1", "unknown_shard_2"));

        } catch (SolrServerException|IOException e) {
            e.printStackTrace();
        }
    }

}
