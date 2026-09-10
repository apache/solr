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
package org.apache.solr.crossdc.common;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.TimeSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class MirroredSolrRequestSerializerTest extends SolrTestCase {

  private static final byte[] EMPTY_ARR = new byte[3];

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  public void testSerializationBufferOptimization() {
    MirroredSolrRequestSerializer serializer = new MirroredSolrRequestSerializer();
    UpdateRequest req = new UpdateRequest();
    SolrInputDocument doc = new SolrInputDocument();
    req.add(doc);
    for (int i = 0; i < 100; i++) {
      // very small docs produce trailing zeroes due to the optimization in
      // ExposedByteArrayOutputStream
      String fieldValue = TestUtil.randomRealisticUnicodeString(random(), i * 100, i * 100);
      doc.setField("test", fieldValue);
      MirroredSolrRequest<?> mirroredRequest = new MirroredSolrRequest<>(req);
      byte[] data = serializer.serialize("test", mirroredRequest);
      if (Arrays.equals(
          Arrays.copyOfRange(data, data.length - EMPTY_ARR.length, data.length), EMPTY_ARR)) {
        System.err.println("TRAILING ZEROES! buf len=" + data.length);
      }
      // fortunately deserialization skips these trailing zeroes
      MirroredSolrRequest<?> deserialized = serializer.deserialize("test", data);
      String deserValue =
          (String)
              ((UpdateRequest) deserialized.getSolrRequest())
                  .getDocuments()
                  .getFirst()
                  .getFieldValue("test");
      assertEquals(fieldValue, deserValue);
    }
  }

  @Test
  public void testMultivaluedParamsRoundTrip() {
    MirroredSolrRequestSerializer serializer = new MirroredSolrRequestSerializer();
    UpdateRequest req = new UpdateRequest();
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    req.add(doc);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "single-value");
    params.add("fq", "a", "b", "c");
    req.setParams(params);

    MirroredSolrRequest<?> mirroredRequest = new MirroredSolrRequest<>(req);
    byte[] data = serializer.serialize("test", mirroredRequest);
    MirroredSolrRequest<?> deserialized = serializer.deserialize("test", data);

    SolrParams deserializedParams = deserialized.getSolrRequest().getParams();
    assertEquals("single-value", deserializedParams.get("q"));
    assertArrayEquals(new String[] {"a", "b", "c"}, deserializedParams.getParams("fq"));
  }

  @Test
  public void testDocsParamsRoundTrip() {
    MirroredSolrRequestSerializer serializer = new MirroredSolrRequestSerializer();
    UpdateRequest req = new UpdateRequest();
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.setField("id", "1");
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.setField("id", "2");
    req.add(doc1, 5000, true);
    req.add(doc2, 1000, false);

    MirroredSolrRequest<?> mirroredRequest = new MirroredSolrRequest<>(req);
    byte[] data = serializer.serialize("test", mirroredRequest);
    MirroredSolrRequest<?> deserialized = serializer.deserialize("test", data);

    UpdateRequest deserializedReq = (UpdateRequest) deserialized.getSolrRequest();
    Map<SolrInputDocument, Map<String, Object>> docsMap = deserializedReq.getDocumentsMap();
    assertEquals(2, docsMap.size());
    for (Map.Entry<SolrInputDocument, Map<String, Object>> entry : docsMap.entrySet()) {
      String id = (String) entry.getKey().getFieldValue("id");
      Map<String, Object> docParams = entry.getValue();
      if ("1".equals(id)) {
        assertEquals(5000, docParams.get(UpdateRequest.COMMIT_WITHIN));
        assertEquals(Boolean.TRUE, docParams.get(UpdateRequest.OVERWRITE));
      } else if ("2".equals(id)) {
        assertEquals(1000, docParams.get(UpdateRequest.COMMIT_WITHIN));
        assertEquals(Boolean.FALSE, docParams.get(UpdateRequest.OVERWRITE));
      } else {
        fail("Unexpected document id: " + id);
      }
    }
  }

  @Test
  public void testDeletesParamsRoundTrip() {
    MirroredSolrRequestSerializer serializer = new MirroredSolrRequestSerializer();
    UpdateRequest req = new UpdateRequest();
    req.deleteById("1", "shard1", 100L);
    req.deleteById("2", "shard2", 200L);

    MirroredSolrRequest<?> mirroredRequest = new MirroredSolrRequest<>(req);
    byte[] data = serializer.serialize("test", mirroredRequest);
    MirroredSolrRequest<?> deserialized = serializer.deserialize("test", data);

    UpdateRequest deserializedReq = (UpdateRequest) deserialized.getSolrRequest();
    Map<String, Map<String, Object>> deleteByIdMap = deserializedReq.getDeleteByIdMap();
    assertEquals(2, deleteByIdMap.size());
    Map<String, Object> params1 = deleteByIdMap.get("1");
    assertEquals("shard1", params1.get(ShardParams._ROUTE_));
    assertEquals(100L, params1.get(UpdateRequest.VER));
    Map<String, Object> params2 = deleteByIdMap.get("2");
    assertEquals("shard2", params2.get(ShardParams._ROUTE_));
    assertEquals(200L, params2.get(UpdateRequest.VER));
  }

  @Test
  public void testBothDocsAndDeletesParamsRoundTrip() {
    MirroredSolrRequestSerializer serializer = new MirroredSolrRequestSerializer();
    UpdateRequest req = new UpdateRequest();
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    req.add(doc, 2000, true);
    req.deleteById("2", "shard1", 50L);
    req.deleteByQuery("field:value");

    MirroredSolrRequest<?> mirroredRequest = new MirroredSolrRequest<>(req);
    byte[] data = serializer.serialize("test", mirroredRequest);
    MirroredSolrRequest<?> deserialized = serializer.deserialize("test", data);

    UpdateRequest deserializedReq = (UpdateRequest) deserialized.getSolrRequest();
    Map<SolrInputDocument, Map<String, Object>> docsMap = deserializedReq.getDocumentsMap();
    assertEquals(1, docsMap.size());
    Map<String, Object> docParams = docsMap.values().iterator().next();
    assertEquals(2000, docParams.get(UpdateRequest.COMMIT_WITHIN));
    assertEquals(Boolean.TRUE, docParams.get(UpdateRequest.OVERWRITE));

    Map<String, Map<String, Object>> deleteByIdMap = deserializedReq.getDeleteByIdMap();
    Map<String, Object> deleteParams = deleteByIdMap.get("2");
    assertEquals("shard1", deleteParams.get(ShardParams._ROUTE_));
    assertEquals(50L, deleteParams.get(UpdateRequest.VER));

    assertEquals(List.of("field:value"), deserializedReq.getDeleteQuery());
  }

  @Test
  public void testDefaultSubmitTime() {
    // The timestamp must be comparable across JVMs (e.g. producer and consumer processes), so it
    // has to be derived from a wall clock (System.currentTimeMillis() via TimeSource.CURRENT_TIME)
    // rather than System.nanoTime(), which is only meaningful within a single JVM's lifetime.
    long before = TimeSource.CURRENT_TIME.getTimeNs();
    MirroredSolrRequest<?> mirroredRequest = new MirroredSolrRequest<>(new UpdateRequest());
    long after = TimeSource.CURRENT_TIME.getTimeNs();

    assertTrue(
        "submitTimeNanos should be a wall-clock nanosecond timestamp, got "
            + mirroredRequest.getSubmitTimeNanos(),
        mirroredRequest.getSubmitTimeNanos() >= before
            && mirroredRequest.getSubmitTimeNanos() <= after);
  }

  @Test
  public void testSubmitTimeNanosRoundTrip() {
    // MirroredSolrRequest(Type, int attempt, SolrRequest) omits submitTimeNanos arg and must
    // fall back to the same wall-clock-based timestamp.
    MirroredSolrRequestSerializer serializer = new MirroredSolrRequestSerializer();
    UpdateRequest req = new UpdateRequest();
    req.deleteById("1");

    long before = TimeSource.CURRENT_TIME.getTimeNs();
    MirroredSolrRequest<?> mirroredRequest =
        new MirroredSolrRequest<>(MirroredSolrRequest.Type.UPDATE, 3, req);
    long after = TimeSource.CURRENT_TIME.getTimeNs();

    byte[] data = serializer.serialize("test", mirroredRequest);
    MirroredSolrRequest<?> deserialized = serializer.deserialize("test", data);

    assertEquals(3, deserialized.getAttempt());
    assertEquals(mirroredRequest.getSubmitTimeNanos(), deserialized.getSubmitTimeNanos());
    assertTrue(
        "submitTimeNanos should be a wall-clock nanosecond timestamp, got "
            + deserialized.getSubmitTimeNanos(),
        deserialized.getSubmitTimeNanos() >= before && deserialized.getSubmitTimeNanos() <= after);
  }
  
  /**
   * Confirms the waste pattern from {@link MirroredSolrRequestSerializer#serialize(String,
   * MirroredSolrRequest)} testing both small and large documents around the 8192 bytes boundary.
   */
  @Test
  public void testSerializationWasteForRealDocumentSizes() {
    MirroredSolrRequest<?> emptyRequest = new MirroredSolrRequest<>(new UpdateRequest());
    Sizes emptySizes = serializeAndMeasure(emptyRequest);
    assertTrue(
        "expected empty-request framing to exceed the 32-byte floor, was "
            + emptySizes.actualSize(),
        emptySizes.actualSize() > 32);
    assertTrue(
        "expected empty-request framing to land in the 32-64 zone, was " + emptySizes.actualSize(),
        emptySizes.actualSize() <= 64);
    logWaste(emptySizes);

    // 64B-8K zone: single doc, single flush -> exact-fit growth, zero padding waste
    for (int fieldLen = 50; fieldLen < 8000; fieldLen += random().nextInt(1000) + 100) {
      Sizes sizes = serializeAndMeasure(docWithField(fieldLen));
      assertTrue("expected to stay under 8k for fieldLen=" + fieldLen, sizes.actualSize() < 8192);
      assertEquals(
          "expected zero padding waste in the 64B-8K zone, actualSize=" + sizes.actualSize(),
          sizes.actualSize(),
          sizes.bufferSize());
      logWaste(sizes);
    }

    // 8k+ zone: a single large doc forces FastOutputStream to flush in 8192-byte chunks,
    // re-triggering the buffer growth and real, measurable waste. This waste is clamped down at 10%
    for (int fieldLen = 8192; fieldLen < 100000; fieldLen += random().nextInt(8000) + 1000) {
      Sizes sizes = serializeAndMeasure(docWithField(fieldLen));
      assertTrue("expected to exceed 8k for fieldLen=" + fieldLen, sizes.actualSize() > 8192);
      double wasteRatio = sizes.waste() / (double) sizes.bufferSize();
      logWaste(sizes);
      if (sizes.waste() == 0) {
        // expect no waste but only due to clamped down padding
        assertTrue(
            "allocated buffer size "
                + sizes.bufferSize()
                + " should be larger than the returned buffer size "
                + sizes.returnedSize(),
            sizes.bufferSize() > sizes.returnedSize());
      }
      assertTrue(
          "waste should never exceed ~10% of the transmitted bufferSize, ratio=" + wasteRatio,
          wasteRatio < 0.11);
    }
  }

  private static void logWaste(Sizes sizes) {
    System.err.println(
        "actualSize="
            + sizes.actualSize()
            + "\treturnedSize="
            + sizes.returnedSize()
            + "\t(allocatedBufferSize="
            + sizes.bufferSize()
            + ")"
            + "\twaste="
            + sizes.waste()
            + "\t("
            + String.format(Locale.ROOT, "%.1f%%", 100.0 * sizes.waste() / sizes.bufferSize())
            + " of transmitted bytes)");
  }

  private static MirroredSolrRequest<?> docWithField(int fieldLen) {
    UpdateRequest req = new UpdateRequest();
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("body", "a".repeat(fieldLen));
    req.add(doc);
    return new MirroredSolrRequest<>(req);
  }

  private record Sizes(int actualSize, int returnedSize, int bufferSize) {
    int waste() {
      return returnedSize - actualSize;
    }
  }

  /**
   * Serializes {@code request} and measures both the true, unpadded size and the padded transmitted
   * bufferSize.
   */
  private static Sizes serializeAndMeasure(MirroredSolrRequest<?> request) {
    MirroredSolrRequestSerializer serializer = Mockito.spy(new MirroredSolrRequestSerializer());
    // capture the actual instance of the ExposedByteArrayOutputStream, so we can measure
    // both the raw buffer size and the size() of just the serialized data in it
    MirroredSolrRequestSerializer.ExposedByteArrayOutputStream[] captured =
        new MirroredSolrRequestSerializer.ExposedByteArrayOutputStream[1];
    Mockito.doAnswer(
            invocation -> {
              captured[0] =
                  (MirroredSolrRequestSerializer.ExposedByteArrayOutputStream)
                      invocation.callRealMethod();
              return captured[0];
            })
        .when(serializer)
        .newExposedByteArrayOutputStream();

    byte[] data = serializer.serialize("test", request);
    return new Sizes(captured[0].size(), data.length, captured[0].getBuffer().length);
  }
}
