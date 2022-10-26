/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.jersey;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;

/**
 * A {@link MapWriter} implementation that relies on Jackson's {@link JsonProperty} annotation.
 *
 * <p>Similar to {@link org.apache.solr.common.util.ReflectMapWriter}, except for its use of the
 * Jackson annotation instead of our own homegrown alternative, {@link
 * org.apache.solr.common.annotation.JsonProperty}. This is useful for when the objects involved
 * must interact with 3rd party libraries that expect Jackson, such as Jersey/
 *
 * @see org.apache.solr.common.util.ReflectMapWriter
 */
public interface JacksonReflectMapWriter extends MapWriter {
  @Override
  default void writeMap(EntryWriter ew) throws IOException {
    Utils.reflectWrite(
        ew,
        this,
        // TODO Should we be lenient here and accept both the Jackson and our homegrown annotation?
        field -> field.getAnnotation(JsonProperty.class) != null,
        JsonAnyGetter.class,
        field -> {
          final JsonProperty prop = field.getAnnotation(JsonProperty.class);
          return prop.value().isEmpty() ? field.getName() : prop.value();
        });
  }
}
