package org.apache.solr.crossdc.common;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import java.util.Arrays;

public class MirroredSolrRequestSerializerTest extends LuceneTestCase {

  private static final byte[] EMPTY_ARR = new byte[3];

  @Test
  public void testSerializationBufferOptimization() throws Exception {
    MirroredSolrRequestSerializer serializer = new MirroredSolrRequestSerializer();
    UpdateRequest req = new UpdateRequest();
    SolrInputDocument doc = new SolrInputDocument();
    req.add(doc);
    for (int i = 0; i < 100; i++) {
      // very small docs produce trailing zeroes due to the optimization in ExposedByteArrayOutputStream
      String fieldValue = TestUtil.randomRealisticUnicodeString(random(), i * 100, i * 100);
      doc.setField("test", fieldValue);
      MirroredSolrRequest mirroredRequest = new MirroredSolrRequest(req);
      byte[] data = serializer.serialize("test", mirroredRequest);
      if (Arrays.equals(Arrays.copyOfRange(data, data.length - EMPTY_ARR.length, data.length), EMPTY_ARR)) {
        System.err.println("TRAILING ZEROES! buf len=" + data.length);
      }
      // fortunately deserialization skips these trailing zeroes
      MirroredSolrRequest deserialized = serializer.deserialize("test", data);
      String deserValue = (String) ((UpdateRequest) deserialized.getSolrRequest()).getDocuments().get(0).getFieldValue("test");
      assertEquals(fieldValue, deserValue);
    }
  }
}
