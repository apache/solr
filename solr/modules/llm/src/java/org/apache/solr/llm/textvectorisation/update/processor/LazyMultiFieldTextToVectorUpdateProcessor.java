package org.apache.solr.llm.textvectorisation.update.processor;


import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.llm.textvectorisation.model.SolrTextToVectorModel;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.zookeeper.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LazyMultiFieldTextToVectorUpdateProcessor extends UpdateRequestProcessor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final IndexSchema schema;
  private final String inputField;
  private final String outputField;
  private final SolrTextToVectorModel textToVector;
  private final String[] additionalInputFields;
  private final Map<String, SolrClient> solrClientMap = Collections.synchronizedMap(new HashMap<>());

  public LazyMultiFieldTextToVectorUpdateProcessor(
      String inputField,
      String[] additionalInputFields,
      String outputField,
      SolrTextToVectorModel textToVector,
      SolrQueryRequest req,
      UpdateRequestProcessor next) {
    super(next);

    this.schema = req.getSchema();
    this.inputField = inputField;
    this.outputField = outputField;
    this.textToVector = textToVector;

    this.additionalInputFields = additionalInputFields;
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    SolrInputDocument newDoc = cmd.getSolrInputDocument();

    SolrInputField inputFieldContent = newDoc.get(inputField);

    ArrayList<String> allFields = new ArrayList<>(Arrays.asList(additionalInputFields));
    allFields.add(inputField);
    Collections.reverse(allFields);

    String contentFromNewDoc = concatenatedFieldContent(newDoc.toMap(new HashMap<>()), allFields);

    if (!isNullOrEmpty(inputFieldContent) || !StringUtils.isBlank(contentFromNewDoc)) {
      try {
        String newDocId = newDoc.getField("id").getValue().toString();
        SolrDocument oldDoc =
            getCurrentSolrDocFromCore(
                cmd.getReq().getCore(), newDocId, allFields);

        List<Float> targetVector = getFreshVectorIfChanged(oldDoc, allFields, contentFromNewDoc, newDocId);

        newDoc.addField(outputField, targetVector);
      } catch (RuntimeException vectorisationFailure) {
        if (log.isErrorEnabled()) {
          SchemaField uniqueKeyField = schema.getUniqueKeyField();
          String uniqueKeyFieldName = uniqueKeyField.getName();
          log.error(
              "Could not vectorize: {} for the document with {}: {}",
              inputField,
              uniqueKeyFieldName,
              newDoc.getFieldValue(uniqueKeyFieldName),
              vectorisationFailure);
        }
      }
    }
    if (next != null) {
      next.processAdd(cmd);
    }
  }

  @SuppressWarnings("unchecked")
  protected List<Float> getFreshVectorIfChanged(
      SolrDocument oldDoc, ArrayList<String> allFields, String contentFromNewDoc, final String newDocId) {

    if (oldDoc != null && oldDoc.getFieldValue(outputField) != null) {

      String contentOldDoc = concatenatedFieldContent(oldDoc, allFields);
      //no need to create vector if old content is equals to new content
      if (contentFromNewDoc.equals(contentOldDoc)) {
        log.info("Content for doc {} is unchanged, no new vectors are generated", newDocId);
        Object fieldValue = oldDoc.getFieldValue(outputField);
        return (List<Float>) fieldValue;
      }
    }
    return vectorize(contentFromNewDoc);
  }

  private List<Float> vectorize(String textToVectorize) {
    float[] vector = textToVector.vectorise(textToVectorize);
    List<Float> vectorAsList = new ArrayList<>(vector.length);
    for (float f : vector) {
      vectorAsList.add(f);
    }
    return vectorAsList;
  }

  protected SolrDocument getCurrentSolrDocFromCore(SolrCore core, String id, List<String> allFields) {

    SolrClient solrClient = solrClientMap.computeIfAbsent(core.getName(), k -> new EmbeddedSolrServer(core));

    // we need to make a copy since we add an element to it
    allFields = new ArrayList<>(allFields);
    allFields.add(outputField);

    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:" + id);
    queryParams.set("fl", allFields.toArray(new String[0]));

    QueryResponse response;
    try {
      response = solrClient.query(queryParams);
    } catch (SolrServerException | IOException e) {
      log.error("error here");
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.getMessage(), e);
    }

    SolrDocumentList results = response.getResults();
    if (results.getNumFound() > 0) {
      return (results.getFirst());
    }
    return null;
  }

  protected String concatenatedFieldContent(Map<String, Object> docFields, List<String> allFields) {
    if (additionalInputFields == null) {
      return "";
    }

    return allFields.stream()
        .map(docFields::get)
        .filter(Objects::nonNull)
        .map(Object::toString)
        .collect(Collectors.joining(" "));
  }
  
  protected boolean isNullOrEmpty(SolrInputField inputFieldContent) {
    return (inputFieldContent == null
        || inputFieldContent.getValue() == null
        || inputFieldContent.getValue().toString().isEmpty());
  }
}
