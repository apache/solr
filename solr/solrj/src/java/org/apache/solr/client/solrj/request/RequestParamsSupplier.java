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
package org.apache.solr.client.solrj.request;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;

/** Interface to coordinate param parsing logic when solrj and solrcore both use a parameter */
// TODO: I've just realized that the reason I introduced this class (getShardsTolerantAsBool
//  in ShardParams) is never actually used in solrj code, and only used in server side code
//  despite living in solrj... so maybe I'll just move that method instead...
public interface RequestParamsSupplier {

  // TODO: Think about use cases, maybe get rid of sysprop?
  //  For per-index/collection usage setting the allowPartialResults request param in defaults
  //  for the handler is preferred, but for large installations with thousands of tenants, possibly
  // with
  //  variations in their solrconfg.xml that prevent simple search/replace this would be
  //  impractical. Thus a means of setting a global default seems appropriate. System property is
  //  a quick and dirty way but In "user managed" mode  it could possibly be a solr.xml (user's
  //  responsibility to manage, similar to a system property), and in cloud mode this maybe should
  //  hinge on a  cluster property since there is no conceivable reason to have individual nodes
  //  with differing defaults. For the cloud case we probably would want to ignore the setting in
  //  solr.xml to guard against inconsistent shard behavior due to misconfiguration. A further
  //  consideration is how things like a docker compose or kubernetes system ought to be able
  //  to initialize cluster properties like this for consistent non-interactive deployment...
  String SOLR_DISLIKE_PARTIAL_RESULTS = "solr.dislike.partial.results";

  /**
   * Users can set {@link #SOLR_DISLIKE_PARTIAL_RESULTS} system property to true, and solr will fail
   * requests where any shard fails due query exedution limits (time, cpu etc). Setting this will
   * prevent solr from collecting partial results by default, adding performance in applications
   * where partial results are not typically useful. This setting can be overridden (in either
   * direction) on a per-request basis with &amp;allowPartialResults=false
   */
  boolean DISLIKE_PARTIAL_RESULTS = Boolean.getBoolean(SOLR_DISLIKE_PARTIAL_RESULTS);

  /**
   * Tests if the partials for the request should be discarded. Examines {@link
   * RequestParamsSupplier#DISLIKE_PARTIAL_RESULTS} sysprop and {@link
   * CommonParams#ALLOW_PARTIAL_RESULTS} request param. The Request Parameter takes precedence
   *
   * @return true if partials should be discarded.
   */
  default boolean shouldDiscardPartials() {
    Boolean userParamAllowPartial = this.getParams().getBool(CommonParams.ALLOW_PARTIAL_RESULTS);
    if (userParamAllowPartial != null) {
      return !userParamAllowPartial;
    } else {
      return DISLIKE_PARTIAL_RESULTS;
    }
  }

  /** returns the current request parameters */
  SolrParams getParams();
}
