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
package org.apache.solr.core;

import static org.apache.solr.core.ConfigOverlay.isEditableProp;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.CoreAdminParams;

public class TestConfigOverlay extends SolrTestCase {

  public void testPaths() {
    assertTrue(isEditableProp("updateHandler/autoCommit/maxDocs", true, null));
    assertTrue(isEditableProp("updateHandler/autoCommit/maxTime", true, null));
    assertTrue(isEditableProp("updateHandler/autoCommit/openSearcher", true, null));
    assertTrue(isEditableProp("updateHandler/autoSoftCommit/maxDocs", true, null));
    assertTrue(isEditableProp("updateHandler/autoSoftCommit/maxTime", true, null));
    assertTrue(isEditableProp("updateHandler/commitWithin/softCommit", true, null));

    assertTrue(isEditableProp("updateHandler.autoCommit.maxDocs", false, null));
    assertTrue(isEditableProp("updateHandler.autoCommit.maxTime", false, null));
    assertTrue(isEditableProp("updateHandler.autoCommit.openSearcher", false, null));
    assertTrue(isEditableProp("updateHandler.autoSoftCommit.maxDocs", false, null));
    assertTrue(isEditableProp("updateHandler.autoSoftCommit.maxTime", false, null));
    assertTrue(isEditableProp("updateHandler.commitWithin.softCommit", false, null));
    assertTrue(isEditableProp("query.useFilterForSortedQuery", false, null));
    assertTrue(isEditableProp("query.queryResultWindowSize", false, null));
    assertTrue(isEditableProp("query.queryResultMaxDocsCached", false, null));
    assertTrue(isEditableProp("query.enableLazyFieldLoading", false, null));
    assertTrue(isEditableProp("query.boolTofilterOptimizer", false, null));

    assertTrue(
        isEditableProp("requestDispatcher.requestParsers.multipartUploadLimitInKB", false, null));
    assertTrue(
        isEditableProp("requestDispatcher.requestParsers.formdataUploadLimitInKB", false, null));

    assertTrue(isEditableProp("query.filterCache.initialSize", false, null));
    assertFalse(isEditableProp("query.filterCache", false, null));
    assertTrue(isEditableProp("query/filterCache/@initialSize", true, null));
    assertFalse(isEditableProp("query/filterCache/@initialSize1", true, null));
  }

  public void testSetProperty() {
    ConfigOverlay overlay = new ConfigOverlay(Collections.emptyMap(), 0);
    overlay = overlay.setProperty("query.filterCache.initialSize", 100);
    assertEquals(100, overlay.getXPathProperty("query/filterCache/@initialSize"));
    Map<String, Object> map = overlay.getEditableSubProperties("query/filterCache");
    assertNotNull(map);
    assertEquals(1, map.size());
    assertEquals(100, map.get("initialSize"));
  }

  public void testDeletedPluginTombstone() {
    ConfigOverlay overlay = new ConfigOverlay(Collections.emptyMap(), 0);

    // Initially, no plugins should be deleted
    assertFalse(overlay.isPluginDeleted("requestHandler", "/update/json"));
    assertEquals(0, overlay.getDeletedPlugins().size());

    // Delete a plugin
    overlay = overlay.deleteNamedPlugin("/update/json", "requestHandler");

    // Verify the plugin is marked as deleted
    assertTrue(overlay.isPluginDeleted("requestHandler", "/update/json"));
    Set<String> deleted = overlay.getDeletedPlugins();
    assertEquals(1, deleted.size());
    assertTrue(deleted.contains("requestHandler:/update/json"));

    // Delete another plugin
    overlay = overlay.deleteNamedPlugin("/sql", "requestHandler");
    assertTrue(overlay.isPluginDeleted("requestHandler", "/sql"));
    assertEquals(2, overlay.getDeletedPlugins().size());

    // Verify the first one is still deleted
    assertTrue(overlay.isPluginDeleted("requestHandler", "/update/json"));
  }

  public void testDeletedPluginFromOverlay() {
    // Create an overlay with a custom request handler
    Map<String, Object> data = new HashMap<>();
    Map<String, Object> requestHandlers = new HashMap<>();
    Map<String, Object> handlerConfig = new HashMap<>();
    handlerConfig.put(CoreAdminParams.NAME, "/custom");
    handlerConfig.put("class", "solr.DumpRequestHandler");
    requestHandlers.put("/custom", handlerConfig);
    data.put("requestHandler", requestHandlers);

    ConfigOverlay overlay = new ConfigOverlay(data, 0);

    // Verify the handler exists in overlay
    assertEquals(1, overlay.getNamedPlugins("requestHandler").size());

    // Delete it
    overlay = overlay.deleteNamedPlugin("/custom", "requestHandler");

    // Verify it's removed from overlay and added to deleted list
    assertEquals(0, overlay.getNamedPlugins("requestHandler").size());
    assertTrue(overlay.isPluginDeleted("requestHandler", "/custom"));
  }

  public void testDeletedPluginPersistence() {
    // Create an overlay with deleted plugins in the data
    Map<String, Object> data = new HashMap<>();
    List<String> deleted = List.of("requestHandler:/update/json", "requestHandler:/sql");
    data.put("deleted", deleted);

    ConfigOverlay overlay = new ConfigOverlay(data, 0);

    // Verify deleted plugins are loaded correctly
    assertTrue(overlay.isPluginDeleted("requestHandler", "/update/json"));
    assertTrue(overlay.isPluginDeleted("requestHandler", "/sql"));
    assertEquals(2, overlay.getDeletedPlugins().size());
  }

  public void testDeleteSamePluginTwice() {
    ConfigOverlay overlay = new ConfigOverlay(Collections.emptyMap(), 0);

    // Delete a plugin
    overlay = overlay.deleteNamedPlugin("/update/json", "requestHandler");
    assertTrue(overlay.isPluginDeleted("requestHandler", "/update/json"));
    assertEquals(1, overlay.getDeletedPlugins().size());

    // Delete the same plugin again
    overlay = overlay.deleteNamedPlugin("/update/json", "requestHandler");

    // Should still only have one entry
    assertTrue(overlay.isPluginDeleted("requestHandler", "/update/json"));
    assertEquals(1, overlay.getDeletedPlugins().size());
  }

  public void testDeleteResponseWriter() {
    ConfigOverlay overlay = new ConfigOverlay(Collections.emptyMap(), 0);

    // Initially, no writers should be deleted
    assertFalse(overlay.isPluginDeleted("queryResponseWriter", "xml"));
    assertFalse(overlay.isPluginDeleted("queryResponseWriter", "json"));

    // Delete a response writer
    overlay = overlay.deleteNamedPlugin("xml", "queryResponseWriter");

    // Verify the writer is marked as deleted
    assertTrue(overlay.isPluginDeleted("queryResponseWriter", "xml"));
    Set<String> deleted = overlay.getDeletedPlugins();
    assertEquals(1, deleted.size());
    assertTrue(deleted.contains("queryResponseWriter:xml"));

    // Delete another writer
    overlay = overlay.deleteNamedPlugin("csv", "queryResponseWriter");
    assertTrue(overlay.isPluginDeleted("queryResponseWriter", "csv"));
    assertEquals(2, overlay.getDeletedPlugins().size());

    // Verify both are still deleted
    assertTrue(overlay.isPluginDeleted("queryResponseWriter", "xml"));
    assertTrue(overlay.isPluginDeleted("queryResponseWriter", "csv"));
  }

  public void testDeleteMixedPluginTypes() {
    ConfigOverlay overlay = new ConfigOverlay(Collections.emptyMap(), 0);

    // Delete different plugin types
    overlay = overlay.deleteNamedPlugin("/update", "requestHandler");
    overlay = overlay.deleteNamedPlugin("json", "queryResponseWriter");
    overlay = overlay.deleteNamedPlugin("mycomponent", "searchComponent");

    // Verify all are marked as deleted
    assertTrue(overlay.isPluginDeleted("requestHandler", "/update"));
    assertTrue(overlay.isPluginDeleted("queryResponseWriter", "json"));
    assertTrue(overlay.isPluginDeleted("searchComponent", "mycomponent"));

    // Verify the count
    assertEquals(3, overlay.getDeletedPlugins().size());

    // Verify they don't interfere with each other
    assertFalse(overlay.isPluginDeleted("requestHandler", "json"));
    assertFalse(overlay.isPluginDeleted("queryResponseWriter", "/update"));
  }
}
