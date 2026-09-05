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

import static org.hamcrest.Matchers.equalTo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.Test;

/** Unit tests for {@link SolrJacksonMapper}'s NamedList/SimpleOrderedMap serialization. */
public class SolrJacksonMapperTest extends SolrTestCaseJ4 {

  @Test
  public void testSimpleOrderedMapSerializesDirectlyWithoutRecursing() throws Exception {
    final SimpleOrderedMap<Object> top = new SimpleOrderedMap<>();
    top.add("status", 0);

    final NamedList<Object> nestedPlainNamedList = new NamedList<>();
    nestedPlainNamedList.add("nestedKey", "nestedVal");
    top.add("nested_plain_namedlist", nestedPlainNamedList);

    final SimpleOrderedMap<Object> nestedSimpleOrderedMap = new SimpleOrderedMap<>();
    nestedSimpleOrderedMap.add("innerKey", 42);
    top.add("nested_simple_ordered_map", nestedSimpleOrderedMap);

    final ObjectMapper mapper = SolrJacksonMapper.getObjectMapper();
    final String json = mapper.writeValueAsString(top);

    assertThat(
        json,
        equalTo(
            "{\"status\":0,"
                + "\"nested_plain_namedlist\":{\"nestedKey\":\"nestedVal\"},"
                + "\"nested_simple_ordered_map\":{\"innerKey\":42}}"));
  }

  @Test
  public void testPlainNamedListStillSerializesViaAsMap() throws Exception {
    final NamedList<Object> namedList = new NamedList<>();
    namedList.add("key", "value");

    final ObjectMapper mapper = SolrJacksonMapper.getObjectMapper();
    final String json = mapper.writeValueAsString(namedList);

    assertThat(json, equalTo("{\"key\":\"value\"}"));
  }

  @Test
  public void testSimpleOrderedMapOmitsNullValuesLikeNamedListDoes() throws Exception {
    final NamedList<Object> namedListWithNull = new NamedList<>();
    namedListWithNull.add("present", "val");
    namedListWithNull.add("absent", null);

    final SimpleOrderedMap<Object> somWithNull = new SimpleOrderedMap<>();
    somWithNull.add("present", "val");
    somWithNull.add("absent", null);

    final ObjectMapper mapper = SolrJacksonMapper.getObjectMapper();
    assertThat(mapper.writeValueAsString(namedListWithNull), equalTo("{\"present\":\"val\"}"));
    assertThat(mapper.writeValueAsString(somWithNull), equalTo("{\"present\":\"val\"}"));
  }
}
