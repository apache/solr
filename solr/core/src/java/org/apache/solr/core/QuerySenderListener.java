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

import static org.apache.solr.common.params.CommonParams.DISTRIB;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.EventParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class QuerySenderListener extends AbstractSolrEventListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public QuerySenderListener(SolrCore core) {
    super(core);
  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    final long startTimeNanos = System.nanoTime();

    final SolrIndexSearcher searcher = newSearcher;
    log.debug("QuerySenderListener sending requests to {}", newSearcher);
    @SuppressWarnings("unchecked")
    List<NamedList<Object>> allLists =
        convertQueriesToList((ArrayList<Object>) getArgs().getAll("queries"));
    if (allLists == null) return;
    final int threads = getThreadsParam();

    if (0 == threads || 1 == threads || allLists.isEmpty()) { // Non-threaded code path
      for (NamedList<Object> nlst : allLists) {
        runQuery(newSearcher, currentSearcher, nlst);
      }
    } else { // Multi-threaded code path
      final int nThreads = threads > 0 ? threads : Runtime.getRuntime().availableProcessors();

      // Since we are using a SynchronousQueue with no capacity we need to make sure we fallback to
      // a blocking put() when adding to the queue fails due to lack of capacity or no thread ready
      // to execute the Runnable.
      final RejectedExecutionHandler rejectedHandler =
          new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
              if (executor.isShutdown()) return;
              try {
                executor.getQueue().put(r);
              } catch (InterruptedException ex) {
                throw new RejectedExecutionException(ex);
              }
            }
          };

      final ExecutorService executor =
          new ExecutorUtil.MDCAwareThreadPoolExecutor(
              nThreads,
              nThreads,
              0L,
              TimeUnit.MILLISECONDS,
              new SynchronousQueue<>(), // Avoid an unbounded queue to save memory in case the auto
              // warm count is large
              new NamedThreadFactory("Query Sender Listener"),
              rejectedHandler);

      try {
        for (NamedList<Object> nlst : allLists) {
          executor.execute(() -> runQuery(newSearcher, currentSearcher, nlst));
        }
      } finally {
        executor.shutdown();

        try {
          executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
          // Ignore
        }
      }
    }

    log.info(
        "QuerySenderListener done. Time: {} ms",
        TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS));
  }

  private int getThreadsParam() {
    final Object threads = getArgs().get(EventParams.THREADS);

    if (null == threads) return 0;
    if (threads instanceof String) return Integer.parseInt((String) threads);
    if (threads instanceof Number) return ((Number) threads).intValue();
    return 0;
  }

  private void runQuery(
      SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher, NamedList<Object> nlst) {
    try {
      // bind the request to a particular searcher (the newSearcher)
      NamedList<Object> params = addEventParms(currentSearcher, nlst);
      // for this, we default to distrib = false
      if (params.get(DISTRIB) == null) {
        params.add(DISTRIB, false);
      }
      SolrQueryRequest req =
          new LocalSolrQueryRequest(getCore(), params) {
            @Override
            public SolrIndexSearcher getSearcher() {
              return newSearcher;
            }

            @Override
            public void close() {}
          };
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      try {
        getCore()
            .execute(getCore().getRequestHandler(req.getParams().get(CommonParams.QT)), req, rsp);

        // Retrieve the Document instances (not just the ids) to warm
        // the OS disk cache, and any Solr document cache.  Only the top
        // level values in the NamedList are checked for DocLists.
        NamedList<?> values = rsp.getValues();
        for (int i = 0; i < values.size(); i++) {
          Object o = values.getVal(i);
          if (o instanceof ResultContext) {
            o = ((ResultContext) o).getDocList();
          }
          if (o instanceof DocList) {
            DocList docs = (DocList) o;
            for (DocIterator iter = docs.iterator(); iter.hasNext(); ) {
              newSearcher.doc(iter.nextDoc());
            }
          }
        }
      } finally {
        try {
          req.close();
        } finally {
          SolrRequestInfo.clearRequestInfo();
        }
      }
    } catch (Exception e) {
      // do nothing... we want to continue with the other requests.
      // the failure should have already been logged.
    }
  }

  protected static List<NamedList<Object>> convertQueriesToList(ArrayList<Object> queries) {

    List<NamedList<Object>> allLists = new ArrayList<NamedList<Object>>();

    for (Object o : queries) {
      if (o instanceof ArrayList) {
        // XML config from solrconfig.xml triggers this path
        for (Object o2 : (ArrayList) o) {
          if (o2 instanceof NamedList) {
            @SuppressWarnings("unchecked")
            NamedList<Object> o3 = (NamedList<Object>) o2;
            allLists.add(o3);
          } else {
            // this is triggered by unexpected <str> elements
            // (unexpected <arr> is ignored)
            // also by nested lists in JSON from Config API
            log.warn("ignoring unsupported warming config ({})", o2);
          }
        }
      } else if (o instanceof NamedList) {
        // JSON config from Config API triggers this path
        @SuppressWarnings("unchecked")
        NamedList<Object> o3 = (NamedList<Object>) o;
        allLists.add(o3);
      } else {
        // NB different message format to above so messages can be differentiated
        log.warn("ignoring unsupported warming config - {}", o);
      }
    }

    return allLists;
  }
}
