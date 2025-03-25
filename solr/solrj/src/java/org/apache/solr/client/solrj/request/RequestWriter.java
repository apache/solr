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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.XML;

/**
 * A RequestWriter is used to write requests to Solr.
 *
 * <p>A subclass can override the methods in this class to supply a custom format in which a request
 * can be sent.
 *
 * @since solr 1.4
 */
public class RequestWriter {

  public interface ContentWriter {

    void write(OutputStream os) throws IOException;

    String getContentType();
  }

  /**
   * To be implemented by subclasses to serialize update requests into the appropriate format.
   *
   * <p>If this method returns null, {@link
   * org.apache.solr.client.solrj.request.RequestWriter#getContentStreams(SolrRequest)} is then
   * invoked to get content.
   */
  public ContentWriter getContentWriter(SolrRequest<?> req) {
    if (req instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) req;
      if (isEmpty(updateRequest)) return null;
      return new RequestWriter.ContentWriter() {
        @Override
        public void write(OutputStream os) throws IOException {
          OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8);
          writeXML(updateRequest, writer);
          writer.flush();
        }

        @Override
        public String getContentType() {
          return ClientUtils.TEXT_XML;
        }
      };
    }
    return req.getContentWriter(ClientUtils.TEXT_XML);
  }

  /**
   * @deprecated Use {@link #getContentWriter(SolrRequest)}.
   */
  @Deprecated
  public Collection<ContentStream> getContentStreams(SolrRequest<?> req) throws IOException {
    if (req instanceof UpdateRequest) {
      return null;
    }
    return req.getContentStreams();
  }

  protected boolean isEmpty(UpdateRequest updateRequest) {
    return isNull(updateRequest.getDocuments())
        && isNull(updateRequest.getDeleteByIdMap())
        && isNull(updateRequest.getDeleteQuery())
        && updateRequest.getDocIterator() == null;
  }

  @Deprecated // SOLR-17256 Slated for removal in Solr 10; only used internally
  public String getPath(SolrRequest<?> req) {
    return req.getPath();
  }

  public void write(SolrRequest<?> request, OutputStream os) throws IOException {
    if (request instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) request;
      BufferedWriter writer =
          new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
      writeXML(updateRequest, writer);
      writer.flush();
    }
  }

  public String getUpdateContentType() {
    return ClientUtils.TEXT_XML;
  }

  public static class StringPayloadContentWriter implements ContentWriter {
    public final String payload;
    public final String type;

    public StringPayloadContentWriter(String payload, String type) {
      this.payload = payload;
      this.type = type;
    }

    @Override
    public void write(OutputStream os) throws IOException {
      if (payload == null) return;
      os.write(payload.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String getContentType() {
      return type;
    }
  }

  protected boolean isNull(List<?> l) {
    return l == null || l.isEmpty();
  }

  protected boolean isNull(Map<?, ?> l) {
    return l == null || l.isEmpty();
  }

  public void writeXML(UpdateRequest request, Writer writer) throws IOException {
    List<Map<SolrInputDocument, Map<String, Object>>> getDocLists = getDocLists(request);

    for (Map<SolrInputDocument, Map<String, Object>> docs : getDocLists) {

      if (docs != null && !docs.isEmpty()) {
        Map.Entry<SolrInputDocument, Map<String, Object>> firstDoc =
            docs.entrySet().iterator().next();
        Map<String, Object> map = firstDoc.getValue();
        Integer cw = null;
        Boolean ow = null;
        if (map != null) {
          cw = (Integer) firstDoc.getValue().get(UpdateRequest.COMMIT_WITHIN);
          ow = (Boolean) firstDoc.getValue().get(UpdateRequest.OVERWRITE);
        }
        if (ow == null) ow = true;
        int commitWithin = (cw != null && cw != -1) ? cw : request.getCommitWithin();
        boolean overwrite = ow;
        if (commitWithin > -1 || overwrite != true) {
          writer.write(
              "<add commitWithin=\"" + commitWithin + "\" " + "overwrite=\"" + overwrite + "\">");
        } else {
          writer.write("<add>");
        }

        Set<Map.Entry<SolrInputDocument, Map<String, Object>>> entries = docs.entrySet();
        for (Map.Entry<SolrInputDocument, Map<String, Object>> entry : entries) {
          ClientUtils.writeXML(entry.getKey(), writer);
        }

        writer.write("</add>");
      }
    }

    // Add the delete commands
    Map<String, Map<String, Object>> deleteById = request.getDeleteByIdMap();
    List<String> deleteQuery = request.getDeleteQuery();
    boolean hasDeleteById = deleteById != null && !deleteById.isEmpty();
    boolean hasDeleteByQuery = deleteQuery != null && !deleteQuery.isEmpty();
    if (hasDeleteById || hasDeleteByQuery) {
      if (request.getCommitWithin() > 0) {
        writer
            .append("<delete commitWithin=\"")
            .append(String.valueOf(request.getCommitWithin()))
            .append("\">");
      } else {
        writer.append("<delete>");
      }
      if (hasDeleteById) {
        for (Map.Entry<String, Map<String, Object>> entry : deleteById.entrySet()) {
          writer.append("<id");
          Map<String, Object> map = entry.getValue();
          if (map != null) {
            Long version = (Long) map.get(UpdateRequest.VER);
            String route = (String) map.get(ShardParams._ROUTE_);
            if (version != null) {
              writer.append(" version=\"").append(String.valueOf(version)).append('"');
            }

            if (route != null) {
              writer.append(" _route_=\"").append(route).append('"');
            }
          }
          writer.append(">");

          XML.escapeCharData(entry.getKey(), writer);
          writer.append("</id>");
        }
      }
      if (hasDeleteByQuery) {
        for (String q : deleteQuery) {
          writer.append("<query>");
          XML.escapeCharData(q, writer);
          writer.append("</query>");
        }
      }
      writer.append("</delete>");
    }
  }

  private List<Map<SolrInputDocument, Map<String, Object>>> getDocLists(UpdateRequest request) {
    List<Map<SolrInputDocument, Map<String, Object>>> docLists = new ArrayList<>();
    Map<SolrInputDocument, Map<String, Object>> docList = null;
    if (request.getDocumentsMap() != null) {

      Boolean lastOverwrite = true;
      Integer lastCommitWithin = -1;

      Map<SolrInputDocument, Map<String, Object>> documents = request.getDocumentsMap();
      for (Map.Entry<SolrInputDocument, Map<String, Object>> entry : documents.entrySet()) {
        Map<String, Object> map = entry.getValue();
        Boolean overwrite = null;
        Integer commitWithin = null;
        if (map != null) {
          overwrite = (Boolean) entry.getValue().get(UpdateRequest.OVERWRITE);
          commitWithin = (Integer) entry.getValue().get(UpdateRequest.COMMIT_WITHIN);
        }
        if (!Objects.equals(overwrite, lastOverwrite)
            || !Objects.equals(commitWithin, lastCommitWithin)
            || docLists.isEmpty()) {
          docList = new LinkedHashMap<>();
          docLists.add(docList);
        }
        docList.put(entry.getKey(), entry.getValue());
        lastCommitWithin = commitWithin;
        lastOverwrite = overwrite;
      }
    }

    Iterator<SolrInputDocument> docIterator = request.getDocIterator();
    if (docIterator != null) {
      docList = new LinkedHashMap<>();
      docLists.add(docList);
      while (docIterator.hasNext()) {
        SolrInputDocument doc = docIterator.next();
        if (doc != null) {
          docList.put(doc, null);
        }
      }
    }

    return docLists;
  }
}
