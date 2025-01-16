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
package org.apache.solr.client.solrj.response;

import java.util.Date;
import org.apache.solr.common.util.NamedList;

/**
 * @since solr 1.3
 */
public class CoreAdminResponse extends SolrResponseBase {

  // TODO NOCOMMIT This is a cause of several test failures, and needs resolved somehow for both
  // 10.0 and branch_9x
  //  In javabin at least, we end up as a LinkedHashMap and not a NamedList.  This comes up
  // periodically in v2 API transitions, but idk if I've seen a case yet where NamedList is declared
  // in a method signature itself, which introduces some backcompat concerns.  The ideal way to fix
  // this I think would be to (1) introduce a new method returning the POJO instead of a NL, (2)
  // switch usage over to the new method, (3) deprecate the old method on branch_9x, and (4) remove
  // the old method on main.  In the interim though (at least on branch_9x) we'll need to
  // reimplement this method to swap out maps for NamedLists, so that we can obey the method
  // signature.
  @SuppressWarnings("unchecked")
  public NamedList<NamedList<Object>> getCoreStatus() {
    return (NamedList<NamedList<Object>>) getResponse().get("status");
  }

  public NamedList<Object> getCoreStatus(String core) {
    return getCoreStatus().get(core);
  }

  public Date getStartTime(String core) {
    NamedList<Object> v = getCoreStatus(core);
    if (v == null) {
      return null;
    }
    return (Date) v.get("startTime");
  }

  public Long getUptime(String core) {
    NamedList<Object> v = getCoreStatus(core);
    if (v == null) {
      return null;
    }
    return (Long) v.get("uptime");
  }
}
