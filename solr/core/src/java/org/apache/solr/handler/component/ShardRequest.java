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
package org.apache.solr.handler.component;

import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.common.params.ModifiableSolrParams;

// todo... when finalized make accessors
public class ShardRequest {
  public static final String[] ALL_SHARDS = null;

  public static final int PURPOSE_PRIVATE = 0x01;
  public static final int PURPOSE_GET_TERM_DFS = 0x02;
  public static final int PURPOSE_GET_TOP_IDS = 0x04;
  public static final int PURPOSE_REFINE_TOP_IDS = 0x08;
  public static final int PURPOSE_GET_FACETS = 0x10;
  public static final int PURPOSE_REFINE_FACETS = 0x20;
  public static final int PURPOSE_GET_FIELDS = 0x40;
  public static final int PURPOSE_GET_HIGHLIGHTS = 0x80;
  public static final int PURPOSE_GET_DEBUG = 0x100;
  public static final int PURPOSE_GET_STATS = 0x200;
  public static final int PURPOSE_GET_TERMS = 0x400;
  public static final int PURPOSE_GET_TOP_GROUPS = 0x800;
  public static final int PURPOSE_GET_MLT_RESULTS = 0x1000;
  public static final int PURPOSE_REFINE_PIVOT_FACETS = 0x2000;
  public static final int PURPOSE_SET_TERM_STATS = 0x4000;
  public static final int PURPOSE_GET_TERM_STATS = 0x8000;

  public int purpose; // the purpose of this request

  public String[] shards; // the shards this request should be sent to, null for all

  public ModifiableSolrParams params;

  /** list of responses... filled out by framework */
  public List<ShardResponse> responses = new ArrayList<>();

  /** actual shards to send the request to, filled out by framework */
  public String[] actualShards;

  /** may be null */
  public String nodeName;

  /** Not null but may implement {@link io.opentracing.noop.NoopTracer}. */
  public final Tracer tracer = GlobalTracer.get();

  // TODO: one could store a list of numbers to correlate where returned docs
  // go in the top-level response rather than looking up by id...
  // this would work well if we ever transitioned to using internal ids and
  // didn't require a uniqueId

  @Override
  public String toString() {
    return "ShardRequest:{params="
        + params
        + ", purpose="
        + Integer.toHexString(purpose)
        + ", nResponses ="
        + responses.size()
        + "}";
  }
}
