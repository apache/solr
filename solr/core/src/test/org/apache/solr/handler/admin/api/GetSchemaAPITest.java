package org.apache.solr.handler.admin.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.schema.StrField;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link GetSchemaAPI} */
public class GetSchemaAPITest extends SolrTestCaseJ4 {

  private IndexSchema mockSchema;
  private GetSchemaAPI api;

  @Before
  public void setUpMocks() {
    assumeWorkingMockito();

    mockSchema = mock(IndexSchema.class);
    api = new GetSchemaAPI(mockSchema);
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingWholeSchema() {
    when(mockSchema.getNamedPropertyValues()).thenReturn(Map.of("flagKey", "flagValue"));

    final var response = api.getSchemaInfo();

    assertNotNull(response);
    assertNotNull(response.schema);
    assertEquals(1, response.schema.size());
    assertEquals("flagValue", response.schema.get("flagKey"));
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingSimilarity() {
    final var map = new SimpleOrderedMap<Object>();
    map.add("flagKey", "flagValue");
    final SimilarityFactory mockSimFactory = mock(SimilarityFactory.class);
    when(mockSimFactory.getNamedPropertyValues()).thenReturn(map);
    when(mockSchema.getSimilarityFactory()).thenReturn(mockSimFactory);

    final var response = api.getSchemaSimilarity();

    assertNotNull(response);
    assertNotNull(response.similarity);
    assertEquals(1, response.similarity.size());
    assertEquals("flagValue", response.similarity.get("flagKey"));
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingUniqueKey() {
    when(mockSchema.getUniqueKeyField()).thenReturn(new SchemaField("myUniqueKey", new StrField()));

    final var response = api.getSchemaUniqueKey();

    assertNotNull(response);
    assertEquals("myUniqueKey", response.uniqueKey);
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingVersion() {
    when(mockSchema.getVersion()).thenReturn(123.456f);

    final var response = api.getSchemaVersion();

    assertNotNull(response);
    assertEquals(123.456f, response.version, 0.1f);
  }
}
