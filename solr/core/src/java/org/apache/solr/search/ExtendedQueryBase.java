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

import org.apache.lucene.search.Query;

public abstract class ExtendedQueryBase extends Query implements ExtendedQuery {
  public static final boolean DEFAULT_CACHE = true;
  public static final int DEFAULT_COST = 0;

  private final boolean cache;
  private final int cost;

  protected ExtendedQueryBase() {
    this(DEFAULT_CACHE, DEFAULT_COST);
  }

  protected ExtendedQueryBase(boolean cache) {
    this(cache, DEFAULT_COST);
  }

  protected ExtendedQueryBase(int cost) {
    this(DEFAULT_CACHE, cost);
  }

  protected ExtendedQueryBase(boolean cache, int cost) {
    this.cache = cache;
    this.cost = cost;
  }

  @Override
  public boolean getCache() {
    return cache;
  }

  @Override
  public int getCost() {
    return cost;
  }

  public String getOptions() {
    return getOptionsString(this);
  }

  public static String getOptionsString(ExtendedQuery q) {
    StringBuilder sb = new StringBuilder();
    if (!q.getCache()) {
      sb.append("{!cache=false");
      int cost = q.getCost();
      if (cost != 0) {
        sb.append(" cost=");
        sb.append(q.getCost());
      }
      sb.append("}");
    }
    return sb.toString();
  }

  @Override
  public String toString(String field) {
    return getOptions();
  }
}
