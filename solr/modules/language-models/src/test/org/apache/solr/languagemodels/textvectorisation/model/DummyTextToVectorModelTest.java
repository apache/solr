package org.apache.solr.languagemodels.textvectorisation.model;

import org.apache.solr.SolrTestCase;
import org.junit.Test;
import java.util.Arrays;

public class DummyTextToVectorModelTest extends SolrTestCase {
  @Test
  public void testVectorise() {
    DummyTextToVectorModel model = new DummyTextToVectorModel(new float[] {1, 2, 3});
    float[] vector = model.vectorise("test");
    assertEquals("[1.0, 2.0, 3.0]", Arrays.toString(vector));
  }
}
