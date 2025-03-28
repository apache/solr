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
package org.apache.solr.rest;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.rest.ManagedResourceStorage.FileStorageIO;
import org.apache.solr.rest.ManagedResourceStorage.JsonStorage;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.junit.Test;

public class TestManagedFileStorage extends SolrTestCaseJ4 {

  /** Runs persisted managed resource creation and update tests on JSON storage. */
  @Test
  public void testFileBasedJsonStorage() throws Exception {
    Path instanceDir = createTempDir("json-storage");
    try (SolrResourceLoader loader = new SolrResourceLoader(instanceDir)) {
      NamedList<String> initArgs = new NamedList<>();
      String managedDir = instanceDir.resolve("managed").toString();
      initArgs.add(ManagedResourceStorage.STORAGE_DIR_INIT_ARG, managedDir);
      FileStorageIO fileStorageIO = new FileStorageIO();
      fileStorageIO.configure(loader, initArgs);
      TestManagedFileStorage.doStorageTests(loader, fileStorageIO);
    }
  }

  /**
   * Called from tests for each storage type to run creation and update tests on a persisted managed
   * resource.
   */
  @SuppressWarnings("unchecked")
  public static void doStorageTests(SolrResourceLoader loader, StorageIO storageIO)
      throws Exception {
    String resourceId = "/test/foo";

    JsonStorage jsonStorage = new JsonStorage(storageIO, loader);

    Map<String, String> managedInitArgs = new HashMap<>();
    managedInitArgs.put("ignoreCase", "true");
    managedInitArgs.put("dontIgnoreCase", "false");

    List<String> managedList = new ArrayList<>(); // we need a mutable List for this test
    managedList.addAll(Arrays.asList("a", "b", "c", "d", "e"));

    Map<String, Object> toStore = new HashMap<>();
    toStore.put(ManagedResource.INIT_ARGS_JSON_FIELD, managedInitArgs);
    toStore.put(ManagedResource.MANAGED_JSON_LIST_FIELD, managedList);

    jsonStorage.store(resourceId, toStore);

    String storedResourceId = jsonStorage.getStoredResourceId(resourceId);
    assertTrue(storedResourceId + " file not found!", storageIO.exists(storedResourceId));

    Object fromStorage = jsonStorage.load(resourceId);
    assertNotNull(fromStorage);

    Map<String, Object> storedMap = (Map<String, Object>) fromStorage;
    Map<String, Object> storedArgs =
        (Map<String, Object>) storedMap.get(ManagedResource.INIT_ARGS_JSON_FIELD);
    assertNotNull(storedArgs);
    assertEquals("true", storedArgs.get("ignoreCase"));
    List<String> storedList = (List<String>) storedMap.get(ManagedResource.MANAGED_JSON_LIST_FIELD);
    assertNotNull(storedList);
    assertEquals(managedList.size(), storedList.size());
    assertTrue(storedList.contains("a"));

    // now verify you can update existing data
    managedInitArgs.put("anotherArg", "someValue");
    managedList.add("f");
    jsonStorage.store(resourceId, toStore);
    fromStorage = jsonStorage.load(resourceId);
    assertNotNull(fromStorage);

    storedMap = (Map<String, Object>) fromStorage;
    storedArgs = (Map<String, Object>) storedMap.get(ManagedResource.INIT_ARGS_JSON_FIELD);
    assertNotNull(storedArgs);
    assertEquals("someValue", storedArgs.get("anotherArg"));
    storedList = (List<String>) storedMap.get(ManagedResource.MANAGED_JSON_LIST_FIELD);
    assertNotNull(storedList);
    assertEquals(managedList.size(), storedList.size());
    assertTrue(storedList.contains("f"));
  }
}
