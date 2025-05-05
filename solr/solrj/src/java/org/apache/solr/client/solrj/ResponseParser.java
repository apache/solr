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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import org.apache.solr.common.util.NamedList;

/**
 * SolrJ Solr response parser.
 *
 * @since solr 1.3
 */
public abstract class ResponseParser {

  /** The writer type placed onto the request as the {@code wt} param. */
  public abstract String getWriterType(); // for example: wt=XML, JSON, etc

  public abstract NamedList<Object> processResponse(InputStream body, String encoding)
      throws IOException;

  /**
   * A well-behaved ResponseParser will return the content-types it supports.
   *
   * @return the content-type values that this parser is capable of parsing. Never null. Empty means
   *     no enforcement.
   */
  public abstract Collection<String> getContentTypes();

  /**
   * @return the version param passed to solr
   */
  public String getVersion() {
    return "2.2";
  }
}
