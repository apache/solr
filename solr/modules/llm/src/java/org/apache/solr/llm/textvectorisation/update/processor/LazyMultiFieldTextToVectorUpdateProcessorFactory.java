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


public class LazyMultiFieldTextToVectorUpdateProcessorFactory extends TextToVectorUpdateProcessorFactory {
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

  protected TextToVectorUpdateProcessor createTextToVectorUpdateProcessor(SolrQueryRequest req, UpdateRequestProcessor next, SolrTextToVectorModel textToVector) {
    return new LazyMultiFieldTextToVectorUpdateProcessor(
          inputField, additionalInputFields, outputField, textToVector, req, next);
  }

  public String getModelName() {
    return modelName;
  }
}
