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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;

/**
 * A RequestWriter is used to write requests to Solr.
 *
 * <p>A subclass can override the methods in this class to supply a custom format in which a request
 * can be sent.
 *
 * @since solr 1.4
 */
public abstract class RequestWriter {

  public interface ContentWriter {

    void write(OutputStream os) throws IOException;

    String getContentType();
  }

  /**
   * A {@code multipart/form-data} body made of several named parts; build it from {@link
   * #getParts}.
   */
  public interface MultipartContentWriter extends ContentWriter {

    List<NamedPart> getParts();

    @Override
    default void write(OutputStream os) throws IOException {
      throw new UnsupportedOperationException(
          "Multipart content is written per-part via getParts(), not write(OutputStream)");
    }

    @Override
    default String getContentType() {
      return "multipart/form-data";
    }
  }

  /** One named part of a {@link MultipartContentWriter}'s body. */
  public static final class NamedPart {
    public final String name;
    public final ContentWriter writer;

    public NamedPart(String name, ContentWriter writer) {
      this.name = name;
      this.writer = writer;
    }
  }

  /** To be implemented by subclasses to serialize update requests into the appropriate format. */
  public abstract ContentWriter getContentWriter(SolrRequest<?> req);

  protected boolean isEmpty(UpdateRequest updateRequest) {
    return isNull(updateRequest.getDocuments())
        && isNull(updateRequest.getDeleteByIdMap())
        && isNull(updateRequest.getDeleteQuery())
        && updateRequest.getDocIterator() == null;
  }

  public abstract void write(SolrRequest<?> request, OutputStream os) throws IOException;

  public abstract String getUpdateContentType();

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
}
