package org.apache.solr.update.processor;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.UUIDField;
import org.apache.solr.update.AddUpdateCommand;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class ContentHashVersionProcessorTest extends UpdateProcessorTestBase {

  public static final String ID_FIELD = "_id";
  public static final String HASH_FIELD_NAME = "_hash_";
  public static final String FIRST_FIELD = "field1";
  public static final String SECOND_FIELD = "field2";
  public static final String THIRD_FIELD = "docField3";
  public static final String FOURTH_FIELD = "field4";
  private SolrQueryRequest req;

  private ContentHashVersionProcessor getContentHashVersionProcessor(
      SolrQueryRequest req,
      SolrQueryResponse rsp,
      UpdateRequestProcessor next,
      List<String> includedFields,
      List<String> excludedFields) {
    ContentHashVersionProcessor processor = new ContentHashVersionProcessor(
        ContentHashVersionProcessorFactory.buildFieldMatcher(includedFields),
        ContentHashVersionProcessorFactory.buildFieldMatcher(excludedFields),
        HASH_FIELD_NAME,
        req,
        rsp,
        next);

    // Given (previous doc retrieval configuration)
    processor.setOldDocProvider((core, hashField, indexedDocId) -> {
      final SolrInputDocument inputDocument = doc(
          f(ID_FIELD, indexedDocId.utf8ToString()),
          f(FIRST_FIELD, "Initial values used to compute initial hash"),
          f(SECOND_FIELD, "This a constant value for testing include/exclude fields")
      );
      return doc(
          f(ID_FIELD, inputDocument.getFieldValue(ID_FIELD)),
          f(FIRST_FIELD, inputDocument.getFieldValue(FIRST_FIELD)),
          f(SECOND_FIELD, inputDocument.getFieldValue(SECOND_FIELD)),
          f(hashField, processor.computeDocHash(inputDocument))
      );
    });
    return processor;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    // Given (processor configuration)
    req = mock(SolrQueryRequest.class);
    when(req.getParams()).thenReturn(mock(SolrParams.class));

    // Given (schema configuration)
    IndexSchema indexSchema = mock(IndexSchema.class);
    when(req.getSchema()).thenReturn(indexSchema);
    when(indexSchema.getUniqueKeyField()).thenReturn(new SchemaField(ID_FIELD, new UUIDField()));
    when(indexSchema.indexableUniqueKey(anyString())).then(invocationOnMock -> new BytesRef(invocationOnMock.getArgument(
        0).toString()));
  }

  @Test
  public void shouldComputeHashForDoc() {
    // Given
    ContentHashVersionProcessor processor = getContentHashVersionProcessor(
        req,
        mock(SolrQueryResponse.class),
        mock(UpdateRequestProcessor.class),
        List.of("*"),
        List.of(ID_FIELD));

    // Given (doc for update)
    SolrInputDocument inputDocument1 = doc(
        f(ID_FIELD, UUID.randomUUID().toString()),
        f(FIRST_FIELD, "Values will serve as input to compute a hash"),
        f(SECOND_FIELD, "This a constant value for testing include/exclude fields")
    );

    // Then
    assertEquals("Tak0G5a/DIE=", processor.computeDocHash(inputDocument1));

    // Given (doc for update - with different order)
    SolrInputDocument inputDocument2 = doc(
        f(ID_FIELD, UUID.randomUUID().toString()),
        f(SECOND_FIELD, "This a constant value for testing include/exclude fields"),
        f(FIRST_FIELD, "Values will serve as input to compute a hash")
    );

    // Then (hash remain same, since id is excluded from signature fields)
    assertEquals("Tak0G5a/DIE=", processor.computeDocHash(inputDocument2));
  }

  @Test
  public void shouldUseExcludedFieldsWildcard() {
    // Given
    ContentHashVersionProcessor processor = getContentHashVersionProcessor(
        req,
        mock(SolrQueryResponse.class), mock(UpdateRequestProcessor.class),
        List.of("*"),
        List.of("field*"));

    // Given (doc for update)
    SolrInputDocument inputDocument = doc(
        f(ID_FIELD, "0000000001"),
        f(FIRST_FIELD, UUID.randomUUID().toString()),
        f(SECOND_FIELD, UUID.randomUUID().toString()),
        f(THIRD_FIELD, "constant to have a constant hash"),
        f(FOURTH_FIELD, UUID.randomUUID().toString())
    );

    // Then (only ID and THIRD_FIELD is used in hash, other fields contain random values)
    assertEquals("bwE8Zjq0aOs=", processor.computeDocHash(inputDocument)); // Hash if only ID field was used
  }

  @Test
  public void shouldUseIncludedFieldsWildcard() {
    // Given
    ContentHashVersionProcessor processor = getContentHashVersionProcessor(
        req,
        mock(SolrQueryResponse.class), mock(UpdateRequestProcessor.class),
        List.of("field*"),
        List.of(THIRD_FIELD));

    // Given (doc for update)
    SolrInputDocument inputDocument = doc(
        f(ID_FIELD, "0000000001"),
        f(FIRST_FIELD, "constant to have a constant hash for field1"),
        f(SECOND_FIELD, "constant to have a constant hash for field2"),
        f(THIRD_FIELD, UUID.randomUUID().toString()),
        f(FOURTH_FIELD, "constant to have a constant hash for field4")
    );

    // Then
    assertEquals("PozPs2qZQtw=", processor.computeDocHash(inputDocument));
  }

  @Test
  public void shouldUseIncludedFieldsWildcard2() {
    // Given (variant of previous shouldUseIncludedFieldsWildcard, without the excludedField config)
    ContentHashVersionProcessor processor = getContentHashVersionProcessor(
        req,
        mock(SolrQueryResponse.class), mock(UpdateRequestProcessor.class),
        List.of("field*"),
        Collections.emptyList());

    // Given (doc for update)
    SolrInputDocument inputDocument = doc(
        f(ID_FIELD, "0000000001"),
        f(FIRST_FIELD, "constant to have a constant hash for field1"),
        f(SECOND_FIELD, "constant to have a constant hash for field2"),
        f(THIRD_FIELD, UUID.randomUUID().toString()),
        f(FOURTH_FIELD, "constant to have a constant hash for field4")
    );

    // Then
    assertEquals("PozPs2qZQtw=", processor.computeDocHash(inputDocument));
  }

  @Test
  public void shouldDedupIncludedFields() {
    // Given (processor to include field1 and field2 only)
    ContentHashVersionProcessor processorWithDuplicatedFieldName = getContentHashVersionProcessor(
        req,
        mock(SolrQueryResponse.class), mock(UpdateRequestProcessor.class),
        List.of(FIRST_FIELD, FIRST_FIELD, SECOND_FIELD),
        Collections.emptyList());

    ContentHashVersionProcessor processorWithWildcard = getContentHashVersionProcessor(
        req,
        mock(SolrQueryResponse.class), mock(UpdateRequestProcessor.class),
        List.of(SECOND_FIELD, FIRST_FIELD, "field1*"), // Also change order of config (test reorder of field names)
        Collections.emptyList());

    // Given (doc for update)
    SolrInputDocument inputDocument = doc(
        f(ID_FIELD, "0000000001"),
        f(FIRST_FIELD, "constant to have a constant hash for field1"),
        f(SECOND_FIELD, "constant to have a constant hash for field2"),
        f(THIRD_FIELD, UUID.randomUUID().toString()),
        f(FOURTH_FIELD, "constant to have a constant hash for field4")
    );

    // Then
    assertEquals("XavrOYGlkXM=", processorWithDuplicatedFieldName.computeDocHash(inputDocument));
    assertEquals("XavrOYGlkXM=", processorWithWildcard.computeDocHash(inputDocument));
  }

  @Test
  public void shouldCreateSignatureForNewDoc() throws IOException {
    // Given
    SolrQueryResponse response = mock(SolrQueryResponse.class);
    ContentHashVersionProcessor processor = getContentHashVersionProcessor(
        req,
        response,
        mock(UpdateRequestProcessor.class),
        Arrays.asList(FIRST_FIELD, SECOND_FIELD),
        Collections.emptyList());
    processor.setDiscardSameDocuments(false);
    processor.setOldDocProvider((core, hashField, indexedDocId) -> null);

    // Given (command)
    AddUpdateCommand cmd = new AddUpdateCommand(req);

    // Given (doc for update)
    SolrInputDocument inputDocument = doc(
        f(ID_FIELD, UUID.randomUUID().toString()),
        f(FIRST_FIELD, "Values will serve as input to compute a hash"),
        f(SECOND_FIELD, "This a constant value for testing include/exclude fields")
    );
    cmd.solrDoc = inputDocument;

    // When
    processor.processAdd(cmd);
    processor.finish();

    // Then
    assertNotNull(inputDocument.getField(HASH_FIELD_NAME)); // signature field got added
    assertEquals(
        processor.computeDocHash(inputDocument),
        inputDocument.getField(HASH_FIELD_NAME).getValue()); // ... and contains expected value

    // Then (asserts on hash comparison results)
    verify(response, times(1)).addToLog(eq("numAddsExisting"), eq(0));
    verify(response, times(1)).addToLog(eq("numAddsExistingWithIdentical"), eq(0)); // And no hash clash with old doc
  }

  @Test
  public void shouldAddToResponseLog() throws IOException {
    // Given
    SolrQueryResponse response = mock(SolrQueryResponse.class);
    ContentHashVersionProcessor processor = getContentHashVersionProcessor(
        req,
        response,
        mock(UpdateRequestProcessor.class),
        Arrays.asList(FIRST_FIELD, SECOND_FIELD),
        Collections.emptyList());
    processor.setDiscardSameDocuments(false);

    // Given (command to update existing doc)
    AddUpdateCommand cmdDoesNotChangeValues = new AddUpdateCommand(req);

    // Given (doc for update - matches the existing doc, see getContentHashVersionProcessor())
    SolrInputDocument initialDocument = doc(
        f(ID_FIELD, UUID.randomUUID().toString()),
        f(FIRST_FIELD, "Initial values used to compute initial hash"),
        f(SECOND_FIELD, "This a constant value for testing include/exclude fields")
    );
    cmdDoesNotChangeValues.solrDoc = doc(
        f(ID_FIELD, initialDocument.getFieldValue(ID_FIELD)),
        f(FIRST_FIELD, initialDocument.getFieldValue(FIRST_FIELD)),
        f(SECOND_FIELD, initialDocument.getFieldValue(SECOND_FIELD)),
        f(HASH_FIELD_NAME, processor.computeDocHash(initialDocument))
    );

    // Given (command to update existing doc with different content)
    AddUpdateCommand cmdChangesDocValues = new AddUpdateCommand(req);

    // Given (doc for update - does *not* match the existing doc, see getContentHashVersionProcessor())
    cmdChangesDocValues.solrDoc = doc(
        f(ID_FIELD, UUID.randomUUID().toString()),
        f(FIRST_FIELD, "This is a doc with values"),
        f(SECOND_FIELD, "that differs from stored doc, so it's considered new")
    );

    // When
    processor.processAdd(cmdDoesNotChangeValues);
    processor.processAdd(cmdChangesDocValues);
    processor.finish();

    // Then (read as follows: 2 updates occurred for an existing doc. Among these updates, 1 update tried to replace
    // doc with the same content)
    verify(response, times(1)).addToLog(eq("numAddsExisting"), eq(2));
    verify(response, times(1)).addToLog(eq("numAddsExistingWithIdentical"), eq(1));
  }

  @Test
  public void shouldNotUpdateSignatureForNewDoc() throws IOException {
    // Given
    SolrQueryResponse response = mock(SolrQueryResponse.class);
    ContentHashVersionProcessor processor = getContentHashVersionProcessor(
        req,
        response,
        mock(UpdateRequestProcessor.class),
        List.of(SECOND_FIELD),
        Collections.emptyList());

    // Given (command)
    AddUpdateCommand cmd = new AddUpdateCommand(req);

    // Given (doc for update)
    SolrInputDocument inputDocument = doc(
        f(ID_FIELD, UUID.randomUUID().toString()),
        f(FIRST_FIELD, "Values will serve as input to compute a hash"),
        f(SECOND_FIELD, "This a constant value for testing include/exclude fields")
    );
    cmd.solrDoc = inputDocument;

    // When
    processor.processAdd(cmd);

    // Then
    assertNotNull(inputDocument.getField(HASH_FIELD_NAME)); // signature field got added
    assertEquals(
        processor.computeDocHash(inputDocument),
        inputDocument.getField(HASH_FIELD_NAME).getValue()); // ... and contains expected value
    verify(response, never()).addToLog(eq("numAddsExisting"), eq(0));
    verify(response, never()).addToLog(eq("numAddsExistingWithIdentical"), eq(0));
  }

  @Test
  public void shouldExcludeFieldsUpdateSignatureForNewDoc() throws IOException {
    // Given
    SolrQueryResponse response = mock(SolrQueryResponse.class);
    ContentHashVersionProcessor processor = getContentHashVersionProcessor(
        req,
        response,
        mock(UpdateRequestProcessor.class),
        List.of(FIRST_FIELD, SECOND_FIELD),
        List.of(FIRST_FIELD));
    processor.setDiscardSameDocuments(false);

    // Given (command)
    AddUpdateCommand cmd = new AddUpdateCommand(req);

    // Given (doc for update)
    SolrInputDocument inputDocument = doc(
        f(ID_FIELD, UUID.randomUUID().toString()),
        f(FIRST_FIELD, "Values will serve as input to compute a hash"),
        f(SECOND_FIELD, "This a constant value for testing include/exclude fields")
    );
    cmd.solrDoc = inputDocument;

    // When
    processor.processAdd(cmd);

    // Then
    assertNotNull(inputDocument.getField(HASH_FIELD_NAME)); // signature field got added
    assertEquals(
        processor.computeDocHash(inputDocument),
        inputDocument.getField(HASH_FIELD_NAME).getValue()); // ... and contains expected value
    verify(response, never()).addToLog(eq("numAddsExisting"), eq(1));
    verify(response, never()).addToLog(eq("numAddsExistingWithIdentical"), eq(0));
  }

  @Test
  public void shouldCommitWithDiscardModeEnabled() throws IOException {
    // Given
    UpdateRequestProcessor nextProcessor = mock(UpdateRequestProcessor.class);
    ContentHashVersionProcessor processor = getContentHashVersionProcessor(
        req,
        mock(SolrQueryResponse.class),
        nextProcessor,
        List.of(FIRST_FIELD, SECOND_FIELD),
        List.of(FIRST_FIELD));
    processor.setDiscardSameDocuments(true);

    // Given (command to update existing doc)
    AddUpdateCommand cmdDoesNotChangeValues = new AddUpdateCommand(req);

    // Given (doc for update - matches the existing doc, see getContentHashVersionProcessor())
    SolrInputDocument initialDocument = doc(
        f(ID_FIELD, UUID.randomUUID().toString()),
        f(FIRST_FIELD, "Initial values used to compute initial hash"),
        f(SECOND_FIELD, "This a constant value for testing include/exclude fields")
    );
    cmdDoesNotChangeValues.solrDoc = doc(
        f(ID_FIELD, initialDocument.getFieldValue(ID_FIELD)),
        f(FIRST_FIELD, initialDocument.getFieldValue(FIRST_FIELD)),
        f(SECOND_FIELD, initialDocument.getFieldValue(SECOND_FIELD)),
        f(HASH_FIELD_NAME, processor.computeDocHash(initialDocument))
    );

    // When
    processor.processAdd(cmdDoesNotChangeValues);

    // Then
    verify(nextProcessor, never()).processAdd(eq(cmdDoesNotChangeValues));
  }

}
