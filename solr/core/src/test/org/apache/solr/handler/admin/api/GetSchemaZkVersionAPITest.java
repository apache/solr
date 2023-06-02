package org.apache.solr.handler.admin.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link GetSchemaZkVersionAPI} */
public class GetSchemaZkVersionAPITest extends SolrTestCaseJ4 {

  private SolrCore mockCore;
  private IndexSchema mockSchema;
  private GetSchemaZkVersionAPI api;

  @Before
  public void setUpMocks() {
    assumeWorkingMockito();

    mockCore = mock(SolrCore.class);
    mockSchema = mock(IndexSchema.class);
    when(mockCore.getLatestSchema()).thenReturn(mockSchema);
    api = new GetSchemaZkVersionAPI(mockCore);
  }

  @Test
  public void testReturnsInvalidZkVersionWhenNotManagedIndexSchema() throws Exception {

    final var response = api.getSchemaZkVersion(-1);

    assertNotNull(response);
    assertEquals(-1, response.zkversion);
  }
}
