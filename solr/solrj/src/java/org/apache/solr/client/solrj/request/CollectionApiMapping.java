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


import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.Pair;

import java.util.*;
import java.util.stream.Stream;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.PER_COLLECTION_PER_SHARD_PER_REPLICA_DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;

/**
 * Stores the mapping of v1 API parameters to v2 API parameters
 * for the collection API and the configset API.
 */
public class CollectionApiMapping {

  public enum Meta implements CommandMeta {
    DELETE_REPLICA(PER_COLLECTION_PER_SHARD_PER_REPLICA_DELETE, DELETE, DELETEREPLICA);

    public final String commandName;
    public final EndPoint endPoint;
    public final SolrRequest.METHOD method;
    public final CollectionAction action;

    //bi-directional mapping of v1 http param name to v2 json attribute
    public final Map<String, String> paramsToAttrs; // v1 -> v2
    public final Map<String, String> attrsToParams; // v2 -> v1
    //mapping of old prefix to new for instance properties.a=val can be substituted with property:{a:val}
    public final Map<String, String> prefixParamsToAttrs; // v1 -> v2

    public SolrRequest.METHOD getMethod() {
      return method;
    }


    Meta(EndPoint endPoint, SolrRequest.METHOD method, CollectionAction action) {
      this(endPoint, method, action, null, null);
    }

    Meta(EndPoint endPoint, SolrRequest.METHOD method, CollectionAction action,
         String commandName,
         Map<String, String> paramsToAttrs) {
      this(endPoint, method, action, commandName, paramsToAttrs, Collections.emptyMap());
    }

    Meta(EndPoint endPoint, SolrRequest.METHOD method, CollectionAction action,
         String commandName,
         Map<String, String> paramsToAttrs,
         Map<String, String> prefixParamsToAttrs) {
      this.action = action;
      this.commandName = commandName;
      this.endPoint = endPoint;
      this.method = method;

      this.paramsToAttrs = paramsToAttrs == null ? Collections.emptyMap() : Collections.unmodifiableMap(paramsToAttrs);
      this.attrsToParams = Collections.unmodifiableMap(reverseMap(this.paramsToAttrs));
      this.prefixParamsToAttrs = prefixParamsToAttrs == null ? Collections.emptyMap() : Collections.unmodifiableMap(prefixParamsToAttrs);
    }

    private static Map<String, String> reverseMap(Map<String, String> input) { // swap keys and values
      Map<String, String> attrToParams = new HashMap<>(input.size());
      for (Map.Entry<String, String> entry :input.entrySet()) {
        final String existing = attrToParams.put(entry.getValue(), entry.getKey());
        if (existing != null) {
          throw new IllegalArgumentException("keys and values must collectively be unique");
        }
      }
      return attrToParams;
    }

    @Override
    public String getName() {
      return commandName;
    }

    @Override
    public SolrRequest.METHOD getHttpMethod() {
      return method;
    }

    @Override
    public V2EndPoint getEndPoint() {
      return endPoint;
    }

    // Returns iterator of v1 "params".
    @Override
    public Iterator<String> getParamNamesIterator(CommandOperation op) {
      Collection<String> paramNames = getParamNames_(op, this);
      Stream<String> pStream = paramNames.stream();
      if (!attrsToParams.isEmpty()) {
        pStream = pStream.map(paramName -> attrsToParams.getOrDefault(paramName, paramName));
      }
      if (!prefixParamsToAttrs.isEmpty()) {
        pStream = pStream.map(paramName -> {
          for (Map.Entry<String, String> e : prefixParamsToAttrs.entrySet()) {
            final String prefixV1 = e.getKey();
            final String prefixV2 = e.getValue();
            if (paramName.startsWith(prefixV2)) {
              return prefixV1 + paramName.substring(prefixV2.length()); // replace
            }
          }
          return paramName;
        });
      }
      return pStream.iterator();
    }

    // returns params (v1) from an underlying v2, with param (v1) input
    @Override
    public String getParamSubstitute(String param) {//input is v1
      for (Map.Entry<String, String> e : prefixParamsToAttrs.entrySet()) {
        final String prefixV1 = e.getKey();
        final String prefixV2 = e.getValue();
        if (param.startsWith(prefixV1)) {
          return prefixV2 + param.substring(prefixV1.length()); // replace
        }
      }
      return paramsToAttrs.getOrDefault(param, param);
    }

    // TODO document!
    public Object getReverseParamSubstitute(String param) {//input is v1
      for (Map.Entry<String, String> e : prefixParamsToAttrs.entrySet()) {
        final String prefixV1 = e.getKey();
        final String prefixV2 = e.getValue();
        if (param.startsWith(prefixV1)) {
          return new Pair<>(prefixV2.substring(0, prefixV2.length() - 1), param.substring(prefixV1.length()));
        }
      }
      return paramsToAttrs.getOrDefault(param, param);
    }

  }

  public enum EndPoint implements V2EndPoint {
    PER_COLLECTION_PER_SHARD_PER_REPLICA_DELETE("collections.collection.shards.shard.replica.delete");
    final String specName;


    EndPoint(String specName) {
      this.specName = specName;
    }

    @Override
    public String getSpecName() {
      return specName;
    }
  }

  public interface V2EndPoint {

    String getSpecName();
  }

  private static Collection<String> getParamNames_(CommandOperation op, CommandMeta command) {
    Object o = op.getCommandData();
    if (o instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String,Object> map = (Map<String, Object>) o;
      List<String> result = new ArrayList<>();
      collectKeyNames(map, result, "");
      return result;
    } else {
      return Collections.emptySet();
    }
  }

  @SuppressWarnings({"unchecked"})
  public static void collectKeyNames(Map<String, Object> map, List<String> result, String prefix) {
    for (Map.Entry<String, Object> e : map.entrySet()) {
      if (e.getValue() instanceof Map) {
        collectKeyNames((Map<String, Object>) e.getValue(), result, prefix + e.getKey() + ".");
      } else {
        result.add(prefix + e.getKey());
      }
    }
  }
  public interface CommandMeta {
    String getName();

    /**
     * the http method supported by this command
     */
    SolrRequest.METHOD getHttpMethod();

    V2EndPoint getEndPoint();

    default Iterator<String> getParamNamesIterator(CommandOperation op) {
      return getParamNames_(op, CommandMeta.this).iterator();
    }

    /** Given a v1 param, return the v2 attribute (possibly a dotted path). */
    default String getParamSubstitute(String name) {
      return name;
    }
  }
}
