package org.apache.solr.llm.textvectorisation.update.processor;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.llm.textvectorisation.store.rest.ManagedTextToVectorModelStore;
import org.junit.Test;

public class LazyMultiFieldTextToVectorUpdateProcessorTest extends TextToVectorUpdateProcessorTest {

  @Test
  public void processAdd_inputField_shouldVectoriseInputField2() throws Exception {
    loadModel("dummy-model.json"); // preparation

    addWithChain(
        sdoc("id", "99", "_text_", "text", "text2_s", "additionalFieldText"),
        "textToVectorLazyMultiField");
    assertU(commit());

    final SolrQuery query = getSolrQuery();

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==1]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/vector==[1.0, 1.0, 1.0, 1.0]");

    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/dummy-1"); // clean up
  }

  @Test
  public void processAdd_inputField_shouldVectoriseInputField3() throws Exception {
    loadModel("dummy-model.json"); // preparation

    addWithChain(
        sdoc("id", "99", "_text_", "text", "text2_s", "additionalFieldText"),
        "textToVectorLazyMultiField");
    assertU(commit());

    final SolrQuery query = getSolrQuery();

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==1]",
        "/response/docs/[0]/id=='99'",
        "/response/docs/[0]/vector==[1.0, 1.0, 1.0, 1.0]");

    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/dummy-1"); // clean up
  }
}
