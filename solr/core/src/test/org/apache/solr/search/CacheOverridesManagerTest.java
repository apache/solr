package org.apache.solr.search;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class CacheOverridesManagerTest extends SolrTestCaseJ4 {
  private SolrZkClient mockZkClient;

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeWorkingMockito();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    mockZkClient = Mockito.mock(SolrZkClient.class);
  }

  @Test
  public void testGetOverrides() throws InterruptedException, KeeperException {
    String jsonString =
        "{\n"
            + " legacyCloud: \"false\",\n"
            + " cacheOverrides : [ \n"
            + "   {\n"
            + "     filterCache :  {\n"
            + "       size: 9999\n"
            + "     }\n"
            + "     documentCache : {\n"
            + "       size: 8888    \n"
            + "     }\n"
            + "     fs-cache : {\n"
            + "       maxRamMB: 7777\n"
            + "     }\n"
            + "   },\n"
            + "   {\n"
            + "     filterCache :  {\n"
            + "       size: 12345\n"
            + "     }\n"
            + "     collections: [ \"104H4B\" ]\n"
            + "   }\n"
            + " ]\n"
            + " \n"
            + "}";

    // Stub a method to return a specific value
    when(mockZkClient.getData(eq(ZkStateReader.CLUSTER_PROPS), any(), any(), anyBoolean()))
        .thenReturn(jsonString.getBytes(Charset.defaultCharset()));

    CacheOverridesManager cacheOverridesManager = new CacheOverridesManager(mockZkClient);

    List<Map<String, String>> overrides;
    overrides = cacheOverridesManager.getOverrides("filterCache", "dummyCollection");
    assertEquals(1, overrides.size());
    assertEquals("9999", overrides.get(0).get("size"));

    overrides = cacheOverridesManager.getOverrides("documentCache", "dummyCollection");
    assertEquals(1, overrides.size());
    assertEquals("8888", overrides.get(0).get("size"));

    overrides = cacheOverridesManager.getOverrides("fs-cache", "dummyCollection");
    assertEquals(1, overrides.size());
    assertEquals("7777", overrides.get(0).get("maxRamMB"));

    overrides = cacheOverridesManager.getOverrides("unknown-cache", "dummyCollection");
    assertNull(overrides);

    overrides = cacheOverridesManager.getOverrides("filterCache", "104H4B");
    assertEquals(2, overrides.size());
    assertEquals("9999", overrides.get(0).get("size"));
    assertEquals("12345", overrides.get(1).get("size"));

    overrides = cacheOverridesManager.getOverrides("documentCache", "104H4B");
    assertEquals(1, overrides.size());
    assertEquals("8888", overrides.get(0).get("size"));

    overrides = cacheOverridesManager.getOverrides("fs-cache", "104H4B");
    assertEquals(1, overrides.size());
    assertEquals("7777", overrides.get(0).get("maxRamMB"));

    overrides = cacheOverridesManager.getOverrides("unknown-cache", "dummyCollection");
    assertNull(overrides);
  }

  @Test
  public void testOverridesZkChanges() throws InterruptedException, KeeperException {
    // start with no clusterprops.json in ZK
    AtomicReference<Watcher> watcherRef = new AtomicReference<>();
    Mockito.doAnswer(
            invocation -> {
              watcherRef.set(invocation.getArgument(1));
              throw new KeeperException.NoNodeException();
            })
        .when(mockZkClient)
        .getData(eq(ZkStateReader.CLUSTER_PROPS), any(), any(), Mockito.anyBoolean());

    CacheOverridesManager cacheOverridesManager = new CacheOverridesManager(mockZkClient);

    List<Map<String, String>> overrides;
    overrides = cacheOverridesManager.getOverrides("filterCache", "dummy");
    assertNull(overrides);

    // emulate new clusterprops.json in ZK
    String jsonString =
        "{\n"
            + " legacyCloud: \"false\",\n"
            + " cacheOverrides : [ \n"
            + "   {\n"
            + "     filterCache :  {\n"
            + "       size: 9999\n"
            + "     }\n"
            + "   },\n"
            + "   {\n"
            + "     filterCache :  {\n"
            + "       size: 12345\n"
            + "     }\n"
            + "     collections: [ \"104H4B\" ]\n"
            + "   }\n"
            + " ]\n"
            + " \n"
            + "}";

    when(mockZkClient.getData(eq(ZkStateReader.CLUSTER_PROPS), any(), any(), anyBoolean()))
        .thenReturn(jsonString.getBytes(Charset.defaultCharset()));
    watcherRef
        .get()
        .process(
            new WatchedEvent(
                Watcher.Event.EventType.NodeCreated,
                Watcher.Event.KeeperState.SyncConnected,
                ZkStateReader.CLUSTER_PROPS));

    overrides = cacheOverridesManager.getOverrides("filterCache", "104H4B");
    assertEquals(2, overrides.size());
    assertEquals("9999", overrides.get(0).get("size"));
    assertEquals("12345", overrides.get(1).get("size"));

    // emulate changes in ZK
    jsonString =
        "{\n"
            + " legacyCloud: \"false\",\n"
            + " cacheOverrides : [ \n"
            + "   {\n"
            + "     filterCache :  {\n"
            + "       size: 1111\n"
            + "     }\n"
            + "     collections: [ \"104H4B\" ]\n"
            + "   },\n"
            + "   {\n"
            + "     filterCache :  {\n"
            + "       size: 2222\n"
            + "     }\n"
            + "     collections: [ \"local\" ]\n"
            + "   }\n"
            + " ]\n"
            + " \n"
            + "}";

    when(mockZkClient.getData(eq(ZkStateReader.CLUSTER_PROPS), any(), any(), anyBoolean()))
        .thenReturn(jsonString.getBytes(Charset.defaultCharset()));
    watcherRef
        .get()
        .process(
            new WatchedEvent(
                Watcher.Event.EventType.NodeDataChanged,
                Watcher.Event.KeeperState.SyncConnected,
                ZkStateReader.CLUSTER_PROPS));

    overrides = cacheOverridesManager.getOverrides("filterCache", "dummy");
    assertNull(overrides);

    overrides = cacheOverridesManager.getOverrides("filterCache", "104H4B");
    assertEquals(1, overrides.size());
    assertEquals("1111", overrides.get(0).get("size"));

    overrides = cacheOverridesManager.getOverrides("filterCache", "local");
    assertEquals(1, overrides.size());
    assertEquals("2222", overrides.get(0).get("size"));

    // emulate overrides removed from clusterprops.json
    jsonString = "{ legacyCloud : \"false\" }";
    when(mockZkClient.getData(eq(ZkStateReader.CLUSTER_PROPS), any(), any(), anyBoolean()))
        .thenReturn(jsonString.getBytes(Charset.defaultCharset()));
    watcherRef
        .get()
        .process(
            new WatchedEvent(
                Watcher.Event.EventType.NodeDataChanged,
                Watcher.Event.KeeperState.SyncConnected,
                ZkStateReader.CLUSTER_PROPS));
    overrides = cacheOverridesManager.getOverrides("filterCache", "104H4B");
    assertNull(overrides);

    // emulate clusterprops.json deleted
    watcherRef
        .get()
        .process(
            new WatchedEvent(
                Watcher.Event.EventType.NodeDeleted,
                Watcher.Event.KeeperState.SyncConnected,
                ZkStateReader.CLUSTER_PROPS));
    overrides = cacheOverridesManager.getOverrides("filterCache", "104H4B");
    assertNull(overrides);
  }

  @Test
  public void testApplyZkOverrides() throws InterruptedException, KeeperException {
    // test when there's no clusterprops.json in ZK
    when(mockZkClient.getData(eq(ZkStateReader.CLUSTER_PROPS), any(), any(), anyBoolean()))
        .thenThrow(new KeeperException.NoNodeException());

    CacheOverridesManager cacheOverridesManager = new CacheOverridesManager(mockZkClient);

    Map<String, String> args = Map.of(NAME, "filterCache", "size", "10000", "initialSize", "10");

    CacheConfig config = new CacheConfig(CaffeineCache.class, args, null);
    CacheConfig afterConfig =
        cacheOverridesManager.applyOverrides(config, "filterCache", "dummyCollection");

    assertEquals(config, afterConfig);

    String jsonString =
        "{\n"
            + " legacyCloud: \"false\",\n"
            + " cacheOverrides : [ \n"
            + "   {\n"
            + "     filterCache :  {\n"
            + "       size: 9999\n"
            + "     }\n"
            + "     documentCache : {\n"
            + "       size: 8888    \n"
            + "     }\n"
            + "     fs-cache : {\n"
            + "       maxRamMB: 7777\n"
            + "     }\n"
            + "   },\n"
            + "   {\n"
            + "     filterCache :  {\n"
            + "       size: 12345\n"
            + "     }\n"
            + "     collections: [ \"104H4B\" ]\n"
            + "   }\n"
            + " ]\n"
            + " \n"
            + "}";
    when(mockZkClient.getData(eq(ZkStateReader.CLUSTER_PROPS), any(), any(), anyBoolean()))
        .thenReturn(jsonString.getBytes(Charset.defaultCharset()));

    cacheOverridesManager = new CacheOverridesManager(mockZkClient);

    afterConfig = cacheOverridesManager.applyOverrides(config, "filterCache", "dummyCollection");
    assertEquals("9999", afterConfig.toMap(new HashMap<>()).get("size"));
    assertEquals("10", afterConfig.toMap(new HashMap<>()).get("initialSize")); // not overridden

    afterConfig = cacheOverridesManager.applyOverrides(config, "filterCache", "104H4B");
    assertEquals("12345", afterConfig.toMap(new HashMap<>()).get("size"));
    assertEquals("10", afterConfig.toMap(new HashMap<>()).get("initialSize")); // not overridden
  }
}
