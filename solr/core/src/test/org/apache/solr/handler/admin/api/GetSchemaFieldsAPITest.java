package org.apache.solr.handler.admin.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.IndexSchema;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link GetSchemaFieldAPI} */
@SuppressWarnings("unchecked")
public class GetSchemaFieldsAPITest extends SolrTestCaseJ4 {

  private IndexSchema mockSchema;
  private SolrParams mockParams;
  private GetSchemaFieldAPI api;

  private SimpleOrderedMap<Object> mockField;
  private ArrayList<SimpleOrderedMap<Object>> mockFieldList;

  @Before
  public void setUpMocks() {
    assumeWorkingMockito();

    mockSchema = mock(IndexSchema.class);
    mockParams = mock(SolrParams.class);
    api = new GetSchemaFieldAPI(mockSchema, mockParams);

    mockField = new SimpleOrderedMap<>();
    mockField.add("name", "id");
    mockField.add("type", "string");

    mockFieldList = new ArrayList<>();
    mockFieldList.add(mockField);
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingAllFields() {
    when(mockSchema.getNamedPropertyValues("fields", mockParams))
        .thenReturn(Map.of("fields", mockFieldList));

    final var response = api.listSchemaFields();

    assertNotNull(response);
    assertCorrectListFields(response.fields);
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingSpecificField() {
    when(mockSchema.getNamedPropertyValues("fields", mockParams))
        .thenReturn(Map.of("fields", mockFieldList));

    final var response = api.getFieldInfo("id");

    assertNotNull(response);
    assertCorrectField(response.fieldInfo);
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingCopyFields() {
    when(mockSchema.getNamedPropertyValues("copyfields", mockParams))
        .thenReturn(Map.of("copyFields", mockFieldList));

    final var response = api.listCopyFields();

    assertNotNull(response);
    assertCorrectListFields(response.copyFields);
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingDynamicFields() {
    when(mockSchema.getNamedPropertyValues("dynamicfields", mockParams))
        .thenReturn(Map.of("dynamicFields", mockFieldList));

    final var response = api.listDynamicFields();

    assertNotNull(response);
    assertCorrectListFields(response.dynamicFields);
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingSpecificDynamicField() {
    when(mockSchema.getNamedPropertyValues("dynamicfields", mockParams))
        .thenReturn(Map.of("dynamicFields", mockFieldList));

    final var response = api.getDynamicFieldInfo("id");

    assertNotNull(response);
    assertCorrectField(response.dynamicFieldInfo);
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingFieldTypes() {
    when(mockSchema.getNamedPropertyValues("fieldtypes", mockParams))
        .thenReturn(Map.of("fieldTypes", mockFieldList));

    final var response = api.listSchemaFieldTypes();

    assertNotNull(response);
    assertCorrectListFields(response.fieldTypes);
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingSpecificFieldType() {
    when(mockSchema.getNamedPropertyValues("fieldtypes", mockParams))
        .thenReturn(Map.of("fieldTypes", mockFieldList));

    final var response = api.getFieldTypeInfo("id");

    assertNotNull(response);
    assertCorrectField(response.fieldTypeInfo);
  }

  private void assertCorrectListFields(Object responseFields) {
    assertNotNull(responseFields);
    assertTrue(responseFields instanceof ArrayList);

    ArrayList<SimpleOrderedMap<Object>> fieldsList =
        (ArrayList<SimpleOrderedMap<Object>>) responseFields;
    assertEquals(1, fieldsList.size());
    assertCorrectField(fieldsList.get(0));
  }

  private void assertCorrectField(SimpleOrderedMap<?> responseField) {
    assertEquals("id", responseField.get("name"));
    assertEquals("string", responseField.get("type"));
  }
}
