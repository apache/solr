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

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.solr.common.params.CommonParams;
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

import static org.apache.solr.common.params.CommonParams.DISTRIB;

/**
 *
 */
public class QuerySenderListener extends AbstractSolrEventListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public QuerySenderListener(SolrCore core) {
    super(core);
  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    final SolrIndexSearcher searcher = newSearcher;
    log.debug("QuerySenderListener sending requests to {}", newSearcher);

    Object queries = getArgs().get("queries");

    List<NamedList<Object>> allLists = new ArrayList<NamedList<Object>>();

    if (queries instanceof List) {
      @SuppressWarnings("unchecked")
      List<NamedList<Object>> queriesLists = (List<NamedList<Object>>)queries;

      if (queriesLists == null) return;

      log.info("queriesLists: " + queriesLists.toString());

      // this works for XML list, but throws if we provide JSON with nested array
      // java.lang.ClassCastException: class java.util.ArrayList cannot be cast to class org.apache.solr.common.util.NamedList

      // if (queriesLists.length() == 0) return;

      //if (queriesLists.get(0) instanceof NamedList) {
      try {
        for (NamedList<Object> nlst : queriesLists) {
          log.warn("nlst is a " + nlst.getClass().toString()  + ": " + nlst.toString());
          allLists.add(nlst);
        }
      } catch(java.lang.ClassCastException ccex) {
        log.warn(ccex.toString());
        for (Object nlst : queriesLists) {
          log.warn("nlst is a " + nlst.getClass().toString()  + ": " + nlst.toString());

          for (Object nlst2 : (ArrayList)nlst) {

            // nlst2 is a class java.util.LinkedHashMap: {q=xxx, fq=env:live}
            log.warn("nlst2 is a " + nlst2.getClass().toString()  + ": " + nlst2.toString());

            // I guess I need to construct a NamedList?

            if (nlst2 instanceof LinkedHashMap) {
              
              @SuppressWarnings("unchecked")
              LinkedHashMap<String, Object> lhm = (LinkedHashMap<String, Object>)nlst2;

              log.warn("lhm is a " + lhm.getClass().toString()  + ": " + lhm.toString());
              
              NamedList<Object> nlst3 = new NamedList<Object>();

              lhm.forEach((key, value)-> nlst3.add(key, value));

              log.warn("nlst3 is a " + nlst3.getClass().toString()  + ": " + nlst3.toString());

              allLists.add(nlst3);
            }
          }
        }
        // for (ArrayList alst : queriesLists) {
        //   for (NamedList<Object> nlst : alst) {
        //     allLists.add(nlst);
        //   }
        // }
      }
      // } else if (queriesLists.get(0) instanceof ArrayList) {
      //   log.info("going deep: " + queriesLists.toString());
      //   // for (NamedList<Object> nlst : queriesLists[0]) {
      //   //   allLists.add(nlst);
      //   // }
      // }

         // for (Object lst : queriesLists) {

        //   if (lst instanceof NamedList) {
        //     log.info("lst is NamedList: " + lst.toString());
  
        //     @SuppressWarnings("unchecked")
        //     allLists.add((NamedList<Object>)lst);
  
        //   } else if (lst instanceof List) {
        //     log.info("lst is List: " + lst.toString());
        //     for (Object item : (List)lst) {
        //       log.info("item: " + item.toString());
  
        //       @SuppressWarnings("unchecked")
        //       allLists.add((NamedList<Object>)item);
              
        //     }
        //   } else {
        //     log.warn("lst is " + lst.getClass().toString() + " " + lst.toString());
        //   }
        // }
    } else if (queries instanceof NamedList) {

      // this only receives first entry in JSON list

      log.info("queries: " + queries.toString());

      @SuppressWarnings("unchecked")
      NamedList<Object> query = (NamedList<Object>)queries;

      log.info("query: " + query.toString());
      
      allLists.add(query);
      
    } else {
      log.warn("\u26A0 queries is a " + queries.getClass().toString());
      log.info("queries: " + queries.toString());
      // class org.apache.solr.common.util.NamedList
    }

    if (allLists == null) return;

    log.info("allLists: " + allLists.toString());

    for (NamedList<Object> nlst : allLists) {
      try {
        // bind the request to a particular searcher (the newSearcher)
        NamedList<Object> params = addEventParms(currentSearcher, nlst);
        // for this, we default to distrib = false
        if (params.get(DISTRIB) == null) {
          params.add(DISTRIB, false);
        }
        SolrQueryRequest req = new LocalSolrQueryRequest(getCore(),params) {
          @Override public SolrIndexSearcher getSearcher() { return searcher; }
          @Override public void close() { }
        };
        SolrQueryResponse rsp = new SolrQueryResponse();
        SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
        try {
          getCore().execute(getCore().getRequestHandler(req.getParams().get(CommonParams.QT)), req, rsp);

          // Retrieve the Document instances (not just the ids) to warm
          // the OS disk cache, and any Solr document cache.  Only the top
          // level values in the NamedList are checked for DocLists.
          NamedList<?> values = rsp.getValues();
          for (int i=0; i<values.size(); i++) {
            Object o = values.getVal(i);
            if (o instanceof ResultContext) {
              o = ((ResultContext)o).getDocList();
            }
            if (o instanceof DocList) {
              DocList docs = (DocList)o;
              for (DocIterator iter = docs.iterator(); iter.hasNext();) {
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
    log.info("QuerySenderListener done.");
  }


}
