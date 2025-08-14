package org.apache.solr.llm.textvectorisation.update.processor;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.llm.textvectorisation.model.SolrTextToVectorModel;
import org.apache.solr.llm.textvectorisation.store.rest.ManagedTextToVectorModelStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;


public class LazyMultiFieldTextToVectorUpdateProcessorFactory extends UpdateRequestProcessorFactory {
  private static final String INPUT_FIELD_PARAM = "inputField";
  private static final String ADDITIONAL_INPUT_FIELDS_PARAM = "additionalInputField";
  private static final String OUTPUT_FIELD_PARAM = "outputField";
  private static final String MODEL_NAME = "model";

  private String inputField;
  private String outputField;
  private String modelName;
  private String[] additionalInputFields;

  @Override
  public void init(final NamedList<?> args) {
    SolrParams params = args.toSolrParams();
    RequiredSolrParams required = params.required();
    inputField = required.get(INPUT_FIELD_PARAM);
    outputField = required.get(OUTPUT_FIELD_PARAM);
    modelName = required.get(MODEL_NAME);
    String inputFields = params.get(ADDITIONAL_INPUT_FIELDS_PARAM);
    additionalInputFields = inputFields != null ? inputFields.split(",") : null;
  }

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    IndexSchema latestSchema = req.getCore().getLatestSchema();

    if (!latestSchema.isDynamicField(inputField) && !latestSchema.hasExplicitField(inputField)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "undefined field: \"" + inputField + "\"");
    }

    if (!latestSchema.isDynamicField(outputField) && !latestSchema.hasExplicitField(outputField)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "undefined field: \"" + outputField + "\"");
    }

    final SchemaField outputFieldSchema = latestSchema.getField(outputField);
    assertIsDenseVectorField(outputFieldSchema);

    ManagedTextToVectorModelStore modelStore =
        ManagedTextToVectorModelStore.getManagedModelStore(req.getCore());
    SolrTextToVectorModel textToVector = modelStore.getModel(modelName);
    if (textToVector == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "The model configured in the Update Request Processor '"
              + modelName
              + "' can't be found in the store: "
              + ManagedTextToVectorModelStore.REST_END_POINT);
    }

    return new LazyMultiFieldTextToVectorUpdateProcessor(
        inputField, additionalInputFields, outputField, textToVector, req, next);
  }

  protected void assertIsDenseVectorField(SchemaField schemaField) {
    FieldType fieldType = schemaField.getType();
    if (!(fieldType instanceof DenseVectorField)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "only DenseVectorField is compatible with Vector Query Parsers: "
              + schemaField.getName());
    }
  }

  public String getInputField() {
    return inputField;
  }

  public String getOutputField() {
    return outputField;
  }

  public String getModelName() {
    return modelName;
  }
}
