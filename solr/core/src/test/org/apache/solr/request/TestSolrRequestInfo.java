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
package org.apache.solr.request;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSolrRequestInfo extends SolrTestCaseJ4 {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("solrconfig.xml","schema11.xml");
    }

    public void testCloseHookTwice(){
        final SolrRequestInfo info = new SolrRequestInfo(
                new LocalSolrQueryRequest(h.getCore(), new MapSolrParams(Map.of())),
                new SolrQueryResponse());
        AtomicInteger counter = new AtomicInteger();
        info.addCloseHook(counter::incrementAndGet);
        SolrRequestInfo.setRequestInfo(info);
        SolrRequestInfo.setRequestInfo(info);
        SolrRequestInfo.clearRequestInfo(false);// that's what pool does.
        assertNotNull(SolrRequestInfo.getRequestInfo());
        SolrRequestInfo.clearRequestInfo();
        assertEquals("hook should be closed only once", 1, counter.get());
        assertNull(SolrRequestInfo.getRequestInfo());
    }

    public void testthreadPool() throws InterruptedException {
        final SolrRequestInfo info = new SolrRequestInfo(
                new LocalSolrQueryRequest(h.getCore(), new MapSolrParams(Map.of())),
                new SolrQueryResponse());
        AtomicInteger counter = new AtomicInteger();
        info.addCloseHook(counter::incrementAndGet);
        SolrRequestInfo.setRequestInfo(info);
        ExecutorUtil.MDCAwareThreadPoolExecutor pool = new ExecutorUtil.MDCAwareThreadPoolExecutor(1, 1, 1,
                TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
        AtomicBoolean run = new AtomicBoolean(false);
        pool.execute(new Runnable() {
            @Override
            public void run() {
                final SolrRequestInfo poolInfo = SolrRequestInfo.getRequestInfo();
                assertSame(info, poolInfo);
                run.set(true);
            }
        });
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.MINUTES);
        assertTrue(run.get());
        SolrRequestInfo.clearRequestInfo();
        SolrRequestInfo.reset();
        assertEquals("hook should be closed only once", 1, counter.get());
        assertNull(SolrRequestInfo.getRequestInfo());
    }
}
