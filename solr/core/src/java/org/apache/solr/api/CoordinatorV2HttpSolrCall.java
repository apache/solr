package org.apache.solr.api;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.servlet.CoordinatorHttpSolrCall;
import org.apache.solr.servlet.SolrDispatchFilter;

public class CoordinatorV2HttpSolrCall extends V2HttpCall{
  public CoordinatorV2HttpSolrCall(SolrDispatchFilter solrDispatchFilter, CoreContainer cc, HttpServletRequest request,
                                   HttpServletResponse response, boolean retry) {
    super(solrDispatchFilter, cc, request, response, retry);
  }
  @Override
  protected SolrCore getCoreByCollection(String collectionName, boolean isPreferLeader) {
    SolrCore core = super.getCoreByCollection(collectionName, isPreferLeader);
    if (core != null) return core;
    return CoordinatorHttpSolrCall.getCore(this, collectionName, isPreferLeader);
  }
}
