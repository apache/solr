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
package org.apache.solr.update.processor;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContentHashVersionProcessorFactoryTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeWorkingMockito();
  }

  @Test
  public void shouldHaveSensibleDefaultValues() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    assertEquals(List.of("*"), factory.getIncludeFields());
    assertTrue(factory.dropSameDocuments());
  }

  @Test
  public void shouldInitWithHashFieldName() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashField", "_hash_field_");
    factory.init(args);

    assertEquals("_hash_field_", factory.getHashField());
  }

  @Test
  public void shouldInitWithAllField() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashField", "content_hash");
    args.add("includeFields", "*");
    factory.init(args);

    assertEquals(1, factory.getIncludeFields().size());
    assertEquals("*", factory.getIncludeFields().getFirst());
  }

  @Test
  public void shouldInitWithIncludedFields() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashField", "content_hash");
    args.add("includeFields", " field1,field2 , field3 ");
    factory.init(args);

    assertEquals(3, factory.getIncludeFields().size());
    assertEquals(List.of("field1", "field2", "field3"), factory.getIncludeFields());
  }

  @Test
  public void shouldInitWithExcludedFields() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashField", "content_hash");
    args.add("excludeFields", " field1,field2 , field3 ");
    factory.init(args);

    assertEquals(4, factory.getExcludeFields().size());
    assertEquals(List.of("field1", "field2", "field3", "content_hash"), factory.getExcludeFields());
  }

  @Test
  public void shouldSelectDropStrategy() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashField", "content_hash");
    args.add("hashCompareStrategy", "drop");
    factory.init(args);

    assertTrue(factory.dropSameDocuments());
  }

  @Test
  public void shouldSelectLogStrategy() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashField", "content_hash");
    args.add("hashCompareStrategy", "log");
    factory.init(args);

    assertFalse(factory.dropSameDocuments());
  }

  @Test(expected = SolrException.class)
  public void shouldSelectUnsupportedStrategy() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashField", "content_hash");
    args.add("hashCompareStrategy", "unsupported value");
    factory.init(args);
  }

  @Test(expected = SolrException.class)
  public void shouldRejectExcludeAllFields() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashField", "content_hash");
    args.add("excludeFields", "*");
    factory.init(args);
  }

  @Test(expected = SolrException.class)
  public void shouldRequireExplicitHashFieldName() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    // Intentionally not setting hashField
    factory.init(args);
  }

  @Test
  public void shouldAutoExcludeHashFieldFromHashComputation() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashField", "my_hash_field");
    args.add("excludeFields", "field1,field2");
    factory.init(args);

    // Hash field should be automatically added to excludeFields
    assertEquals(3, factory.getExcludeFields().size());
    assertTrue(
        "Should contain explicitly excluded field1", factory.getExcludeFields().contains("field1"));
    assertTrue(
        "Should contain explicitly excluded field2", factory.getExcludeFields().contains("field2"));
    assertTrue(
        "Should auto-exclude hash field name",
        factory.getExcludeFields().contains("my_hash_field"));
  }
}
