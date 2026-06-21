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
package org.apache.solr.client.solrj.impl;

import java.nio.charset.Charset;

import com.google.api.client.util.escape.CharEscapers;
import org.eclipse.jetty.client.util.StringRequestContent;
import org.eclipse.jetty.util.Fields;

/**
 * <p> "application/x-www-form-urlencoded" content type.</p>
 */
public class SolrFormEncoder extends StringRequestContent {

  public static final String APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";

  public SolrFormEncoder(Fields fields, Charset charset) {
    super(APPLICATION_X_WWW_FORM_URLENCODED, convert(fields), charset);
  }

  public static String convert(Fields fields) {
    // Assume 32 chars between name and value.
    StringBuilder builder = new StringBuilder(fields.getSize() * 32);
    for (Fields.Field field : fields) {
      for (String value : field.getValues()) {
        if (builder.length() > 0) builder.append('&');
        builder.append(encode(field.getName())).append('=').append(encode(value));
      }
    }
    return builder.toString();
  }

  //    This escaper has identical behavior to (but is potentially much faster than):
  //    java.net.URLEncoder#encode(String, String) with the encoding name "UTF-8"
  private static String encode(String value) {
    return CharEscapers.escapeUri(value);
  }
}
