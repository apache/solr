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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.CancellableCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.common.params.CommonParams.QUERY_UUID;

/**
 * Tracks metadata for active queries and provides methods for access
 */
public class CancellableQueryTracker {
    /**
     * expiryTimeInSeconds is the amount of time before a key will be regarded as
     * stale and discarded. It can be set with the solr.cancellableQueryTracker.expiryTime
     * property.
     */
    static protected int expiryTimeInSeconds = Integer.parseInt(System.getProperty("solr.cancellableQueryTracker.expiryTime", "3600"));
    /**
     * maxSize is the maximum number of tasks to track before discarding the least recently used.
     * It can be set using the solr.cancellableQueryTracker.maxSize property.
     */
    static protected int maxSize = Integer.parseInt(System.getProperty("solr.cancellableQueryTracker.maxSize", "10000"));
    /**
     * cleanupTime is the tick time for the cleanup for the caches
     */
    static protected int cleanupTime = Integer.parseInt(System.getProperty("solr.cancellableQueryTracker.cleanupTime", "60"));

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    protected final Cache<String, CancellableCollector> activeCancellableQueries;
    protected final Cache<String, String> activeQueriesGenerated;

    public CancellableQueryTracker() {
        activeQueriesGenerated = CacheBuilder.
                newBuilder().maximumSize(maxSize).
                expireAfterWrite(expiryTimeInSeconds, TimeUnit.SECONDS).
                removalListener(notification -> {
                    if (log.isTraceEnabled() && notification.wasEvicted()) {
                        log.trace("CQT: Removed task {} from activeQueriesGenerated due to {}", notification.getKey(), notification.getCause()); //nowarn
                    }
                }).build();
        activeCancellableQueries = CacheBuilder.
                newBuilder().
                maximumSize(maxSize).
                expireAfterWrite(expiryTimeInSeconds, TimeUnit.SECONDS).
                removalListener(notification -> {
                    if (notification.wasEvicted()) {
                        if (log.isWarnEnabled()) {
                            log.warn("CQT: Cancelling stalled task {} as evicted from query tracker due to {}", notification.getKey(), notification.getCause()); //nowarn
                        }
                        assert notification.getValue() != null;
                        CancellableCollector asCC = (CancellableCollector) notification.getValue();
                        asCC.cancel();
                    }
                }).build();
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        exec.scheduleAtFixedRate(() -> {
            long beforeACQ = 0;
            long beforeAQG = 0;
            if (log.isTraceEnabled()) {
                beforeACQ = activeCancellableQueries.size();
                beforeAQG = activeQueriesGenerated.size();
            }
            activeCancellableQueries.cleanUp();
            activeQueriesGenerated.cleanUp();
            if (log.isTraceEnabled()) {
                log.trace("CQT: Ran cleanup. ACQ before {} now {}. AQG before {} now {}.",
                        beforeACQ, activeCancellableQueries.size(), beforeAQG, activeQueriesGenerated.size());
            }
        }, cleanupTime, cleanupTime, TimeUnit.SECONDS);
    }

    /**
     * Generates a UUID for the given query or if the user provided a UUID for this query, uses that.
     */
    public String generateQueryID(SolrQueryRequest req) {
        String queryID;
        String customQueryUUID = req.getParams().get(QUERY_UUID, null);

        if (customQueryUUID != null) {
            queryID = customQueryUUID;
        } else {
            // TODO: Use a different generator
            queryID = UUID.randomUUID().toString();
        }

        if (activeQueriesGenerated.getIfPresent(queryID) != null) {
            if (customQueryUUID != null) {
                throw new IllegalArgumentException("Duplicate query UUID given");
            } else {
                while (activeQueriesGenerated.getIfPresent(queryID) != null) {
                    queryID = UUID.randomUUID().toString();
                }
            }
        }
        String qs = "";
        if (req.getHttpSolrCall() != null) {
            qs = req.getHttpSolrCall().getReq().getQueryString();
        }
        activeQueriesGenerated.put(queryID, qs);

        return queryID;
    }

    /**
     * Releases a queryID allocated by generateQueryID()
     */
    public void releaseQueryID(String inputQueryID) {
        if (inputQueryID == null) {
            return;
        }

        activeQueriesGenerated.invalidate(inputQueryID);
    }

    /**
     * Determines if a queryID allocated by generateQueryID() is currently present
     */
    public boolean isQueryIdActive(String queryID) {
        return activeQueriesGenerated.getIfPresent(queryID) != null;
    }

    /**
     * Adds a queryID to the list of cancellable queries
     */
    public void addShardLevelActiveQuery(String queryID, CancellableCollector collector) {
        log.trace("CQT: Marking query {} as cancellable", queryID);
        if (queryID == null) {
            return;
        }

        activeCancellableQueries.put(queryID, collector);
    }

    /**
     * Gets the cancellation task for a cancellable queryID
     */
    public CancellableCollector getCancellableTask(String queryID) {
        if (queryID == null) {
            throw new IllegalArgumentException("Input queryID is null");
        }

        return activeCancellableQueries.getIfPresent(queryID);
    }

    /**
     * Removes a cancellable query from the list of outstanding cancellable queries
     */
    public void removeCancellableQuery(String queryID) {
        if (queryID == null) {
            // Some components, such as CaffeineCache, use the searcher to fire internal queries which are
            // not tracked
            return;
        }

        activeCancellableQueries.invalidate(queryID);
    }

    /**
     * Returns the list of allocated and unreleased queryIDs allocated by generateQueryID()
     */
    public Iterator<Map.Entry<String, String>> getActiveQueriesGenerated() {
        return activeQueriesGenerated.asMap().entrySet().iterator();
    }
}
