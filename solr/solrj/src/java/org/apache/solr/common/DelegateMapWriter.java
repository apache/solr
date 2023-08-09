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

package org.apache.solr.common;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import org.apache.solr.common.util.Utils;

public class DelegateMapWriter implements MapWriter {

  private final Object delegate;

  public DelegateMapWriter(Object delegate) {
    this.delegate = delegate;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    Utils.reflectWrite(
        ew,
        delegate,
        // TODO Should we be lenient here and accept both the Jackson and our homegrown annotation?
        field -> field.getAnnotation(JsonProperty.class) != null,
        JsonAnyGetter.class,
        field -> {
          final JsonProperty prop = field.getAnnotation(JsonProperty.class);
          return prop.value().isEmpty() ? field.getName() : prop.value();
        });
  }
}
