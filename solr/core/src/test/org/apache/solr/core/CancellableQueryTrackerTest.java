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
package org.apache.solr.core;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.search.CancellableCollector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class CancellableQueryTrackerTest {
    private static int expiry;

    @Before
    public void setUp() {
        assumeWorkingMockito();
        expiry = CancellableQueryTracker.expiryTimeInSeconds;
        CancellableQueryTracker.expiryTimeInSeconds = 3;
    }

    @After
    public void tearDown() {
        CancellableQueryTracker.expiryTimeInSeconds = expiry;
    }

    @Test
    public void TestGenerateQueryID() {
        CancellableQueryTracker tracker = new CancellableQueryTracker();
        Map<String, String[]> args = new HashMap<>();
        LocalSolrQueryRequest sr = new LocalSolrQueryRequest(null, args);
        // if we generate a UUID it shouldn't be our test string
        String uuid1 = tracker.generateQueryID(sr);
        assertNotEquals("myQueryID", uuid1);
        // if we generate another UUID it also should be our test string, or the first UUID
        String uuid2 = tracker.generateQueryID(sr);
        assertNotEquals("myQueryID", uuid2);
        assertNotEquals(uuid1, uuid2);
        // if we set an explicit ID it should be used
        args.put("queryUUID", new String[]{"myQueryID"});
        sr = new LocalSolrQueryRequest(null, args);
        assertEquals("myQueryID", tracker.generateQueryID(sr));
        // but if we try to use it again, it should throw an exception
        Consumer<LocalSolrQueryRequest> localSolrQueryRequestConsumer = (LocalSolrQueryRequest sq) -> {
            try {
                tracker.generateQueryID(sq);
            } catch (IllegalArgumentException e) {
                assertEquals("Duplicate query UUID given", e.getMessage());
                return;
            }
            fail("Should have thrown IllegalArgumentException");
        };
        localSolrQueryRequestConsumer.accept(sr);
        // unless we mark it complete
        tracker.releaseQueryID("myQueryID");
        assertEquals("myQueryID", tracker.generateQueryID(sr));
    }

    @Test
    public void activeCancellableQueryTimesOut() throws InterruptedException {
        CancellableQueryTracker tracker = new CancellableQueryTracker();
        CancellableCollector cc = mock(CancellableCollector.class);
        tracker.addShardLevelActiveQuery("myQueryID", cc);
        assertNotNull(tracker.activeCancellableQueries.getIfPresent("myQueryID"));
        TimeUnit.SECONDS.sleep(5);
        assertNull(tracker.activeCancellableQueries.getIfPresent("myQueryID"));
        tracker.activeCancellableQueries.cleanUp();
        Mockito.verify(cc, Mockito.times(1)).cancel();
    }

    @Test
    public void queryIDIsReleased() {
        CancellableQueryTracker tracker = new CancellableQueryTracker();
        SolrParams mockParams = mock(SolrParams.class);
        Mockito.when(mockParams.get("queryUUID")).thenReturn("myQueryID");
        LocalSolrQueryRequest req = mock(LocalSolrQueryRequest.class);
        Mockito.when(req.getParams()).thenReturn(mockParams);
        String queryID = tracker.generateQueryID(req);
        assertNotNull(tracker.activeQueriesGenerated.getIfPresent(queryID));
        tracker.releaseQueryID(queryID);
        assertNull(tracker.activeQueriesGenerated.getIfPresent(queryID));
    }

    @Test
    public void canFetchCancellableQuery() {
        CancellableQueryTracker tracker = new CancellableQueryTracker();
        CancellableCollector cc = mock(CancellableCollector.class);
        tracker.addShardLevelActiveQuery("myQueryID", cc);
        assertNotNull(tracker.activeCancellableQueries.getIfPresent("myQueryID"));
        assertEquals(cc, tracker.getCancellableTask("myQueryID"));
    }

    @Test
    public void canFetchQueryList() {
        CancellableQueryTracker tracker = new CancellableQueryTracker();
        String[] queries = new String[5];
        SolrParams mockParams = mock(SolrParams.class);
        LocalSolrQueryRequest req = mock(LocalSolrQueryRequest.class);
        for (int i = 0; i < 5; i++) {
            Mockito.when(req.getParams()).thenReturn(mockParams);
            queries[i]= tracker.generateQueryID(req);
        }
        Iterator<Map.Entry<String, String>> iterator = tracker.getActiveQueriesGenerated();
        int count = 0;
        String[] results = new String[5];
        while (iterator.hasNext()) {
            results[count] = iterator.next().getKey();
            count++;
        }
        Arrays.sort(results);
        Arrays.sort(queries);
        assertArrayEquals(queries, results);
    }
}