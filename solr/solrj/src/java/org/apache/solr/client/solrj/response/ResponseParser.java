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
package org.apache.solr.client.solrj.response;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import org.apache.solr.common.util.NamedList;

/**
 * SolrJ Solr response parser.
 *
 * @since solr 1.3
 */
public abstract class ResponseParser {

  protected ResponseParser() {
    assert validateContentTypes();
  }

  // identity check is intentional: getContentTypes() must return the same cached instance
  @SuppressWarnings("ReferenceEquality")
  private boolean validateContentTypes() {
    Collection<String> contentTypes = getContentTypes();
    assert contentTypes == getContentTypes()
        : getClass().getName() + ".getContentTypes() must return the same instance on every call";
    for (String ct : contentTypes) {
      assert !ct.contains(";") && ct.equals(ct.toLowerCase(Locale.ROOT))
          : getClass().getName()
              + ".getContentTypes() must return lowercase MIME types without semicolons, got: "
              + ct;
    }
    return true;
  }

  /** The writer type placed onto the request as the {@code wt} param. */
  public abstract String getWriterType(); // for example: wt=XML, JSON, etc

  public abstract NamedList<Object> processResponse(InputStream body, String encoding)
      throws IOException;

  /**
   * Returns the MIME types this parser supports. Return an empty set to disable. Implementations
   * must:
   *
   * <ul>
   *   <li>Returns the same instance (same reference on every call).
   *   <li>Use only lowercase MIME types without charset or other parameters (no semicolons)
   *   <li>
   * </ul>
   *
   * @return the MIME types that this parser is capable of parsing. Never null.
   */
  public abstract Set<String> getContentTypes();
}
