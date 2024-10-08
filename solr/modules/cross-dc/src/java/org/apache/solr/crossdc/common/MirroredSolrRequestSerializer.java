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
package org.apache.solr.crossdc.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked", "rawtypes"})
public class MirroredSolrRequestSerializer
    implements Serializer<MirroredSolrRequest<?>>, Deserializer<MirroredSolrRequest<?>> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Configure this class.
   *
   * @param configs configs in key/value pairs
   * @param isKey whether is for key or value
   */
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public MirroredSolrRequest<?> deserialize(String topic, byte[] data) {
    Map<String, Object> requestMap;
    try (JavaBinCodec codec = new JavaBinCodec()) {
      ByteArrayInputStream bais = new ByteArrayInputStream(data);

      try {
        requestMap = (Map) codec.unmarshal(bais);

        if (log.isTraceEnabled()) {
          log.trace(
              "Deserialized class={} solrRequest={}", requestMap.getClass().getName(), requestMap);
        }

      } catch (Exception e) {
        log.error("Exception unmarshalling JavaBin", e);
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      log.error("Error in deserialize", e);
      throw new RuntimeException(e);
    }
    MirroredSolrRequest.Type type = MirroredSolrRequest.Type.get((String) requestMap.get("type"));
    SolrRequest<?> request;
    int attempt = Integer.parseInt(String.valueOf(requestMap.getOrDefault("attempt", "-1")));
    long submitTimeNanos =
        Long.parseLong(String.valueOf(requestMap.getOrDefault("submitTimeNanos", "-1")));
    ModifiableSolrParams params;
    if (requestMap.get("params") != null) {
      params =
          ModifiableSolrParams.of(
              new MapSolrParams((Map<String, String>) requestMap.get("params")));
    } else {
      params = new ModifiableSolrParams();
    }
    if (type == MirroredSolrRequest.Type.UPDATE) {
      request = new UpdateRequest();
      UpdateRequest updateRequest = (UpdateRequest) request;
      List docs = (List) requestMap.get("docs");
      if (docs != null) {
        updateRequest.add(docs);
      } else {
        updateRequest.add("id", "1"); // TODO huh?
        updateRequest.getDocumentsMap().clear();
      }

      List<String> deletes = (List<String>) requestMap.get("deletes");
      if (deletes != null) {
        updateRequest.deleteById(deletes);
      }

      List<String> deletesQuery = (List<String>) requestMap.get("deleteQuery");
      if (deletesQuery != null) {
        for (String delQuery : deletesQuery) {
          updateRequest.deleteByQuery(delQuery);
        }
      }
      updateRequest.setParams(params);
    } else if (type == MirroredSolrRequest.Type.ADMIN) {
      CollectionParams.CollectionAction action =
          CollectionParams.CollectionAction.get(params.get(CoreAdminParams.ACTION));
      if (action == null) {
        throw new RuntimeException("Missing 'action' parameter! " + requestMap);
      }
      request = new MirroredSolrRequest.MirroredAdminRequest(action, params);
      if (log.isDebugEnabled()) {
        log.debug(
            "Mirrored Admin request={}",
            ((MirroredSolrRequest.MirroredAdminRequest) request).jsonStr());
      }
    } else if (type == MirroredSolrRequest.Type.CONFIGSET) {
      List<ContentStream> contentStreams = null;
      String m = (String) requestMap.get("method");
      SolrRequest.METHOD method = SolrRequest.METHOD.valueOf(m);
      List<String> csNames = new ArrayList<>();
      List<Map<String, Object>> streamsList =
          (List<Map<String, Object>>) requestMap.get("contentStreams");
      if (streamsList != null) {
        contentStreams = new ArrayList<>();
        for (Map<String, Object> streamMap : streamsList) {
          String contentType = (String) streamMap.get("contentType");
          String name = (String) streamMap.get("name");
          csNames.add(name);
          String sourceInfo = (String) streamMap.get("sourceInfo");
          byte[] content = (byte[]) streamMap.get("content");
          MirroredSolrRequest.ExposedByteArrayContentStream ecs =
              new MirroredSolrRequest.ExposedByteArrayContentStream(
                  content, sourceInfo, contentType);
          ecs.setName(name);
          contentStreams.add(ecs);
        }
      }
      request = new MirroredSolrRequest.MirroredConfigSetRequest(method, params, contentStreams);
      if (log.isDebugEnabled()) {
        log.debug(
            "Mirrored configSet method={}, req={}, streams={}",
            request.getMethod(),
            request.getParams(),
            csNames);
      }
    } else {
      throw new RuntimeException("Unknown request type: " + requestMap);
    }

    return new MirroredSolrRequest<>(type, attempt, request, submitTimeNanos);
  }

  /**
   * Convert {@code data} into a byte array.
   *
   * @param topic topic associated with data
   * @param request MirroredSolrRequest that needs to be serialized
   * @return serialized bytes
   */
  @Override
  public byte[] serialize(String topic, MirroredSolrRequest<?> request) {
    // TODO: add checks
    SolrRequest<?> solrRequest = request.getSolrRequest();

    if (log.isTraceEnabled()) {
      log.trace("serialize request={}", request);
    }

    try (JavaBinCodec codec = new JavaBinCodec(null)) {

      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
      Map<String, Object> map = CollectionUtil.newHashMap(8);
      map.put("attempt", request.getAttempt());
      map.put("submitTimeNanos", request.getSubmitTimeNanos());
      map.put("params", solrRequest.getParams());
      map.put("type", request.getType().toString());
      if (solrRequest instanceof UpdateRequest) {
        UpdateRequest update = (UpdateRequest) solrRequest;
        map.put("docs", update.getDocuments());
        map.put("deletes", update.getDeleteById());
        map.put("deleteQuery", update.getDeleteQuery());
      } else if (solrRequest instanceof MirroredSolrRequest.MirroredConfigSetRequest) {
        MirroredSolrRequest.MirroredConfigSetRequest config =
            (MirroredSolrRequest.MirroredConfigSetRequest) solrRequest;
        map.put("method", config.getMethod().toString());
        if (config.getContentStreams() != null) {
          List<Map<String, Object>> streamsList = new ArrayList<>();
          for (ContentStream cs : config.getContentStreams()) {
            Map<String, Object> streamMap = new HashMap<>();
            streamMap.put("name", cs.getName());
            streamMap.put("contentType", cs.getContentType());
            streamMap.put("sourceInfo", cs.getSourceInfo());
            MirroredSolrRequest.ExposedByteArrayContentStream ecs =
                MirroredSolrRequest.ExposedByteArrayContentStream.of(cs);
            streamMap.put("content", ecs.byteArray());
            streamsList.add(streamMap);
          }
          map.put("contentStreams", streamsList);
        }
      }

      codec.marshal(map, baos);

      return baos.byteArray();

    } catch (IOException e) {
      log.error("Error in serialize", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Close this serializer.
   *
   * <p>This method must be idempotent as it may be called multiple times.
   */
  @Override
  public final void close() {
    Serializer.super.close();
  }

  private static final class ExposedByteArrayOutputStream extends ByteArrayOutputStream {
    ExposedByteArrayOutputStream() {
      super();
    }

    byte[] byteArray() {
      return buf;
    }
  }
}
