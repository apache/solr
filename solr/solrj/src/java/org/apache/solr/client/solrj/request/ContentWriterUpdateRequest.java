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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.client.solrj.request.RequestWriter.ContentWriter;
import org.apache.solr.client.solrj.request.RequestWriter.NamedPart;

/**
 * Uploads one or more content parts to Solr Cell or another handler that consumes request content
 * (CSV, XML, JSON). A single part is sent as the request body; several parts are sent as {@code
 * multipart/form-data}.
 *
 * <p>See https://solr.apache.org/guide/solr/latest/indexing-guide/indexing-with-tika.html<br>
 * See https://solr.apache.org/guide/solr/latest/indexing-guide/indexing-with-update-handlers.html
 */
public class ContentWriterUpdateRequest extends AbstractUpdateRequest {
  private final List<NamedPart> parts = new ArrayList<>();

  public ContentWriterUpdateRequest(String path) {
    super(METHOD.POST, path);
  }

  @Override
  public ContentWriter getContentWriter(String expectedType) {
    if (parts.isEmpty()) return null;
    if (parts.size() == 1) {
      return parts.get(0).writer;
    }
    return new RequestWriter.MultipartContentWriter() {
      @Override
      public List<NamedPart> getParts() {
        return List.copyOf(parts);
      }
    };
  }

  /** Adds a part written from {@code writer}, named {@code name} when sent as multipart. */
  public void addPart(String name, ContentWriter writer) {
    parts.add(new NamedPart(name, writer));
  }

  /** Adds the file's bytes as a part, named after the file. */
  public void addFile(Path file, String contentType) {
    addPart(
        file.getFileName().toString(),
        new ContentWriter() {
          @Override
          public void write(OutputStream os) throws IOException {
            Files.copy(file, os);
          }

          @Override
          public String getContentType() {
            return contentType;
          }
        });
  }

  /** Adds {@code content}, encoded as UTF-8, as the request's only part. */
  public void addContentWithType(String content, String contentType) {
    addPart(null, new RequestWriter.StringPayloadContentWriter(content, contentType));
  }
}
