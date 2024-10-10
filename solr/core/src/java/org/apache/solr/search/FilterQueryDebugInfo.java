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
package org.apache.solr.search;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.util.SimpleOrderedMap;

public class FilterQueryDebugInfo {
  String query;
  long elapsed = -1;
  long fqMergeElapsed = -1;
  String filter;
  Map<String, Object> info; // additional information
  final List<FilterQueryDebugInfo> children;

  Map<String, Object> reqDescription;

  public FilterQueryDebugInfo() {
    children = new ArrayList<>();
    info = new LinkedHashMap<>();
  }

  public void addChild(FilterQueryDebugInfo child) {
    children.add(child);
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public void setElapsed(long elapsed) {
    this.elapsed = elapsed;
  }

  public void setFqMergeElapsed(long fqMergeElapsed) {
    this.fqMergeElapsed = fqMergeElapsed;
  }

  public void setReqDescription(Map<String, Object> reqDescription) {
    this.reqDescription = reqDescription;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public void putInfoItem(String key, Object value) {
    info.put(key, value);
  }

  public Map<String, Object> getInfo() {
    return info;
  }

  public SimpleOrderedMap<Object> getDebugInfo() {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();

    if (filter != null) info.add("filter", filter);
    if (query != null) info.add("query", query);
    if (elapsed != -1) info.add("elapsed", elapsed);
    if (fqMergeElapsed != -1) info.add("fqMergeElapsed", fqMergeElapsed);
    if (reqDescription != null) {
      info.addAll(reqDescription);
    }
    info.addAll(this.info);

    if (children != null && children.size() > 0) {
      List<Object> subFQ = new ArrayList<>();
      info.add("sub-filter-query", subFQ);
      for (FilterQueryDebugInfo child : children) {
        subFQ.add(child.getDebugInfo());
      }
    }
    return info;
  }

  @Override
  public String toString() {
    return "filter query debug info: query " + query + " elapsed " + elapsed + "ms";
  }
}
