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

import java.io.InputStream;
import java.io.Reader;
import java.util.Collection;
import java.util.Set;
import org.apache.solr.common.util.NamedList;

/**
 * @since solr 1.3
 */
public abstract class ResponseParser {
  public abstract String getWriterType(); // for example: wt=XML, JSON, etc

  public abstract NamedList<Object> processResponse(InputStream body, String encoding);

  public abstract NamedList<Object> processResponse(Reader reader);

  /**
   * A well behaved ResponseParser will return its content-type.
   *
   * @return the content-type this parser expects to parse
   * @deprecated use {@link #getContentTypes()} instead
   */
  @Deprecated
  public String getContentType() {
    return null;
  }

  /**
   * A well-behaved ResponseParser will return the content-types it supports.
   *
   * @return the content-type values that this parser is capable of parsing.
   */
  public Collection<String> getContentTypes() {
    final String contentType = getContentType();
    if (contentType == null) {
      return Set.of();
    }
    return Set.of(getContentType());
  }

  /**
   * @return the version param passed to solr
   */
  public String getVersion() {
    return "2.2";
  }
}
