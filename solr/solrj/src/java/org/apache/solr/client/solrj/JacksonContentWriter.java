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

package org.apache.solr.client.solrj;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.solr.client.solrj.request.RequestWriter;

public class JacksonContentWriter implements RequestWriter.ContentWriter {

  public static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  private final Object toWrite;
  private final String contentType;

  public JacksonContentWriter(String contentType, Object toWrite) {
    this.contentType = contentType;
    this.toWrite = toWrite;
  }

  @Override
  public void write(OutputStream os) throws IOException {
    DEFAULT_MAPPER.writeValue(os, toWrite);
  }

  @Override
  public String getContentType() {
    return contentType;
  }
}
