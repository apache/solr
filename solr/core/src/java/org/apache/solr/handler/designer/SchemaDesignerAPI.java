/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.designer;

import static org.apache.solr.common.params.CommonParams.JSON_MIME;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.SchemaDesignerApi;
import org.apache.solr.client.api.model.FlexibleSolrJerseyResponse;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.ZkConfigSetService;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.RTimer;
import org.apache.zookeeper.KeeperException;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** All V2 APIs have a prefix of /api/schema-designer/ */
public class SchemaDesignerAPI extends JerseyResource
    implements SchemaDesignerApi, SchemaDesignerConstants {

  private static final Set<String> excludeConfigSetNames = Set.of(DEFAULT_CONFIGSET_NAME);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;
  private final SchemaSuggester schemaSuggester;
  private final SampleDocumentsLoader sampleDocLoader;
  private final SchemaDesignerSettingsDAO settingsDAO;
  private final SchemaDesignerConfigSetHelper configSetHelper;
  private final Map<String, Integer> indexedVersion = new ConcurrentHashMap<>();
  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public SchemaDesignerAPI(CoreContainer coreContainer, SolrQueryRequest solrQueryRequest) {
    this(
        coreContainer,
        SchemaDesignerAPI.newSchemaSuggester(),
        SchemaDesignerAPI.newSampleDocumentsLoader(),
        solrQueryRequest);
  }

  SchemaDesignerAPI(
      CoreContainer coreContainer,
      SchemaSuggester schemaSuggester,
      SampleDocumentsLoader sampleDocLoader,
      SolrQueryRequest solrQueryRequest) {
    this.coreContainer = coreContainer;
    this.schemaSuggester = schemaSuggester;
    this.sampleDocLoader = sampleDocLoader;
    this.solrQueryRequest = solrQueryRequest;
    this.configSetHelper =
        new SchemaDesignerConfigSetHelper(this.coreContainer, this.schemaSuggester);
    this.settingsDAO = new SchemaDesignerSettingsDAO(coreContainer, configSetHelper);
  }

  public static SchemaSuggester newSchemaSuggester() {
    DefaultSchemaSuggester suggester = new DefaultSchemaSuggester();
    suggester.init(new NamedList<>());
    return suggester;
  }

  public static SampleDocumentsLoader newSampleDocumentsLoader() {
    SampleDocumentsLoader loader = new DefaultSampleDocumentsLoader();
    loader.init(new NamedList<>());
    return loader;
  }

  static String getConfigSetZkPath(final String configSet) {
    return getConfigSetZkPath(configSet, null);
  }

  static String getConfigSetZkPath(final String configSet, final String childNode) {
    String path = ZkConfigSetService.CONFIGS_ZKNODE + "/" + configSet;
    if (childNode != null) {
      path += "/" + childNode;
    }
    return path;
  }

  static String getMutableId(final String configSet) {
    return DESIGNER_PREFIX + configSet;
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public FlexibleSolrJerseyResponse getInfo(String configSet) throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);

    Map<String, Object> responseMap = new HashMap<>();
    responseMap.put(CONFIG_SET_PARAM, configSet);
    boolean exists = configExists(configSet);
    responseMap.put("published", exists);

    // mutable config may not exist yet as this is just an info check to gather some basic info the
    // UI needs
    String mutableId = getMutableId(configSet);

    // tricky: if there's a mutable config, prefer the settings from that first but fallback to the
    // original if not found
    SolrConfig srcConfig = exists ? configSetHelper.loadSolrConfig(configSet) : null;
    SolrConfig solrConfig =
        configExists(mutableId) ? configSetHelper.loadSolrConfig(mutableId) : srcConfig;
    addSettingsToResponse(settingsDAO.getSettings(solrConfig), responseMap);

    responseMap.put(SCHEMA_VERSION_PARAM, configSetHelper.getCurrentSchemaVersion(mutableId));
    responseMap.put(
        "collections", exists ? configSetHelper.listCollectionsForConfig(configSet) : List.of());

    // don't fail if loading sample docs fails
    try {
      responseMap.put("numDocs", configSetHelper.retrieveSampleDocs(configSet).size());
    } catch (Exception exc) {
      log.warn("Failed to load sample docs from blob store for {}", configSet, exc);
    }

    return buildFlexibleResponse(responseMap);
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public FlexibleSolrJerseyResponse prepNewSchema(String configSet, String copyFrom)
      throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);
    validateNewConfigSetName(configSet);

    final String effectiveCopyFrom = copyFrom != null ? copyFrom : DEFAULT_CONFIGSET_NAME;

    SchemaDesignerSettings settings =
        getMutableSchemaForConfigSet(configSet, -1, effectiveCopyFrom);
    ManagedIndexSchema schema = settings.getSchema();
    String mutableId = getMutableId(configSet);

    // make sure the temp collection for this analysis exists
    if (!zkStateReader().getClusterState().hasCollection(mutableId)) {
      indexedVersion.remove(mutableId);
      configSetHelper.createCollection(mutableId, mutableId);
    }

    settingsDAO.persistIfChanged(mutableId, settings);

    return buildFlexibleResponse(buildResponse(configSet, schema, settings, null));
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public SolrJerseyResponse cleanupTempSchema(String configSet) throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);
    doCleanupTemp(configSet);
    return instantiateJerseyResponse(SolrJerseyResponse.class);
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public FlexibleSolrJerseyResponse getFileContents(String configSet, String file)
      throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);
    requireNotEmpty("file", file);
    String filePath = getConfigSetZkPath(getMutableId(configSet), file);
    byte[] data;
    try {
      data = zkStateReader().getZkClient().getData(filePath, null, null);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error reading file: " + filePath, SolrZkClient.checkInterrupted(e));
    }
    String stringData =
        data != null && data.length > 0 ? new String(data, StandardCharsets.UTF_8) : "";
    return buildFlexibleResponse(Collections.singletonMap(file, stringData));
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public FlexibleSolrJerseyResponse updateFileContents(String configSet, String file)
      throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);
    requireNotEmpty("file", file);

    String mutableId = getMutableId(configSet);
    String zkPath = getConfigSetZkPath(mutableId, file);
    if (!pathExistsInZk(zkPath)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "File '" + file + "' not found in configSet: " + configSet);
    }

    byte[] data;
    try (InputStream in = extractSingleContentStream(true).getStream()) {
      data = in.readAllBytes();
    }
    Exception updateFileError = null;

    if (SOLR_CONFIG_XML.equals(file)) {
      // verify the updated solrconfig.xml is valid before saving to ZK (to avoid things blowing up
      // later)
      try {
        InMemoryResourceLoader loader =
            new InMemoryResourceLoader(coreContainer, mutableId, SOLR_CONFIG_XML, data);
        SolrConfig.readFromResourceLoader(loader, SOLR_CONFIG_XML, null);
      } catch (Exception exc) {
        updateFileError = exc;
      }
    }

    if (updateFileError != null) {
      // solrconfig.xml update failed, but haven't impacted the configSet yet, so just return the
      // error directly
      Throwable causedBy = SolrException.getRootCause(updateFileError);
      Map<String, Object> response = new HashMap<>();
      response.put("updateFileError", causedBy.getMessage());
      response.put(file, new String(data, StandardCharsets.UTF_8));
      return buildFlexibleResponse(response);
    }

    // apply the update and reload the temp collection / re-index sample docs
    SolrZkClient zkClient = zkStateReader().getZkClient();
    try {
      zkClient.setData(zkPath, data);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(
          "Failed to save data in ZK at path: " + zkPath, SolrZkClient.checkInterrupted(e));
    }

    configSetHelper.reloadTempCollection(mutableId, false);

    ManagedIndexSchema schema = loadLatestSchema(mutableId);
    Map<Object, Throwable> errorsDuringIndexing = null;
    SolrException solrExc = null;
    List<SolrInputDocument> docs = configSetHelper.retrieveSampleDocs(configSet);
    String[] analysisErrorHolder = new String[1];
    if (!docs.isEmpty()) {
      String idField = schema.getUniqueKeyField().getName();
      try {
        errorsDuringIndexing =
            indexSampleDocsWithRebuildOnAnalysisError(
                idField, docs, mutableId, true, analysisErrorHolder);
      } catch (SolrException exc) {
        solrExc = exc;
      }
    }

    Map<String, Object> response = buildResponse(configSet, schema, null, docs);

    if (analysisErrorHolder[0] != null) {
      response.put(ANALYSIS_ERROR, analysisErrorHolder[0]);
    }

    addErrorToResponse(
        mutableId,
        solrExc,
        errorsDuringIndexing,
        response,
        "Failed to re-index sample documents after update to the " + file + " file");

    return buildFlexibleResponse(response);
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public FlexibleSolrJerseyResponse getSampleValue(
      String configSet, String fieldName, String idField, String docId) throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);
    requireNotEmpty(FIELD_PARAM, fieldName);
    requireNotEmpty(UNIQUE_KEY_FIELD_PARAM, idField);

    final List<SolrInputDocument> docs = configSetHelper.retrieveSampleDocs(configSet);
    String textValue = null;
    if (StrUtils.isNullOrEmpty(docId)) {
      // no doc ID from client ... find the first doc with a non-empty string value for fieldName
      Optional<SolrInputDocument> doc =
          docs.stream()
              .filter(
                  d ->
                      d.getField(fieldName) != null
                          && d.getField(fieldName).getFirstValue() != null
                          && !d.getField(fieldName).getFirstValue().toString().isEmpty())
              .findFirst();
      if (doc.isPresent()) {
        docId = doc.get().getFieldValue(idField).toString();
        textValue = doc.get().getField(fieldName).getFirstValue().toString();
      }
    } else {
      final String idFilter = docId;
      Optional<SolrInputDocument> doc =
          docs.stream().filter(d -> idFilter.equals(d.getFieldValue(idField))).findFirst();
      if (doc.isPresent()) {
        SolrInputField field = doc.get().getField(fieldName);
        textValue =
            field != null && field.getFirstValue() != null ? field.getFirstValue().toString() : "";
      }
    }

    if (textValue != null) {
      var analysis = configSetHelper.analyzeField(configSet, fieldName, textValue);
      return buildFlexibleResponse(
          Map.of(idField, docId, fieldName, textValue, "analysis", analysis));
    }
    return instantiateJerseyResponse(FlexibleSolrJerseyResponse.class);
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public FlexibleSolrJerseyResponse listCollectionsForConfig(String configSet) {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);
    return buildFlexibleResponse(
        Collections.singletonMap(
            "collections", configSetHelper.listCollectionsForConfig(configSet)));
  }

  // CONFIG_EDIT_PERM is required here since this endpoint is used by the UI to determine if the
  // user has access to the Schema Designer UI
  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public FlexibleSolrJerseyResponse listConfigs() throws Exception {
    return buildFlexibleResponse(Collections.singletonMap("configSets", listEnabledConfigs()));
  }

  protected Map<String, Integer> listEnabledConfigs() throws IOException {
    List<String> configsInZk = configSetHelper.listConfigsInZk();
    final Map<String, Integer> configs =
        configsInZk.stream()
            .filter(c -> !excludeConfigSetNames.contains(c) && !c.startsWith(DESIGNER_PREFIX))
            .collect(Collectors.toMap(c -> c, c -> settingsDAO.isDesignerDisabled(c) ? 1 : 2));

    // add the in-progress but drop the _designer prefix
    configsInZk.stream()
        .filter(c -> c.startsWith(DESIGNER_PREFIX))
        .map(c -> c.substring(DESIGNER_PREFIX.length()))
        .forEach(c -> configs.putIfAbsent(c, 0));

    return configs;
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public Response downloadConfig(String configSet) throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);
    String mutableId = getMutableId(configSet);

    // find the configSet to download
    SolrZkClient zkClient = zkStateReader().getZkClient();
    String configId = mutableId;
    try {
      if (!zkClient.exists(getConfigSetZkPath(mutableId, null))) {
        if (zkClient.exists(getConfigSetZkPath(configSet, null))) {
          configId = configSet;
        } else {
          throw new SolrException(
              SolrException.ErrorCode.NOT_FOUND, "ConfigSet " + configSet + " not found!");
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error reading config from ZK", SolrZkClient.checkInterrupted(e));
    }

    final byte[] zipBytes = configSetHelper.downloadAndZipConfigSet(configId);
    // Sanitize configSet to safe filename characters to prevent header injection
    final String safeConfigSet = configSet.replaceAll("[^a-zA-Z0-9_\\-.]", "_");
    final String fileName = safeConfigSet + "_configset.zip";
    return Response.ok((StreamingOutput) outputStream -> outputStream.write(zipBytes))
        .type("application/zip")
        .header("Content-Disposition", "attachment; filename=\"" + fileName + "\"")
        .build();
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public FlexibleSolrJerseyResponse addSchemaObject(String configSet, Integer schemaVersion)
      throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);
    requireSchemaVersion(schemaVersion);
    final String mutableId = checkMutable(configSet, schemaVersion);

    Map<String, Object> addJson = readJsonFromRequest();
    log.info("Adding new schema object from JSON: {}", addJson);

    String objectName = configSetHelper.addSchemaObject(configSet, addJson);
    String action = addJson.keySet().iterator().next();

    ManagedIndexSchema schema = loadLatestSchema(mutableId);
    Map<String, Object> response =
        buildResponse(configSet, schema, null, configSetHelper.retrieveSampleDocs(configSet));
    response.put(action, objectName);
    return buildFlexibleResponse(response);
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public FlexibleSolrJerseyResponse updateSchemaObject(String configSet, Integer schemaVersion)
      throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);
    requireSchemaVersion(schemaVersion);
    final String mutableId = checkMutable(configSet, schemaVersion);

    // Updated field definition is in the request body as JSON
    Map<String, Object> updateField = readJsonFromRequest();
    String name = (String) updateField.get("name");
    if (StrUtils.isNullOrEmpty(name)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Invalid update request! JSON payload is missing the required name property: "
              + updateField);
    }
    log.info(
        "Updating schema object: configSet={}, mutableId={}, name={}, JSON={}",
        configSet,
        mutableId,
        name,
        updateField);

    SchemaDesignerSettings settings = getMutableSchemaForConfigSet(configSet, -1, null);
    ManagedIndexSchema schemaBeforeUpdate = settings.getSchema();

    Map<String, Object> updateResult =
        configSetHelper.updateSchemaObject(configSet, updateField, schemaBeforeUpdate);
    SolrException solrExc = (SolrException) updateResult.get("solrExc");
    String updateError = (String) updateResult.get(UPDATE_ERROR);
    String updateType = (String) updateResult.get("updateType");
    boolean needsRebuild = (boolean) updateResult.get("rebuild");

    // re-index the docs if no error to this point
    final ManagedIndexSchema schema = loadLatestSchema(mutableId);
    List<SolrInputDocument> docs = configSetHelper.retrieveSampleDocs(configSet);
    Map<Object, Throwable> errorsDuringIndexing = null;
    String[] analysisErrorHolder = new String[1];
    if (solrExc == null && !docs.isEmpty()) {
      try {
        errorsDuringIndexing =
            indexSampleDocsWithRebuildOnAnalysisError(
                schema.getUniqueKeyField().getName(), docs, mutableId, false, analysisErrorHolder);
      } catch (SolrException exc) {
        solrExc = exc;
        updateError =
            "Failed to re-index sample documents after update to the "
                + name
                + " "
                + updateType
                + " due to: "
                + solrExc.getMessage();
      }
    }

    Map<String, Object> response = buildResponse(configSet, schema, settings, docs);
    response.put("updateType", updateType);
    if (FIELD_PARAM.equals(updateType)) {
      response.put(updateType, fieldToMap(schema.getField(name), schema));
    } else if ("type".equals(updateType)) {
      response.put(updateType, schema.getFieldTypeByName(name).getNamedPropertyValues(true));
    }

    if (analysisErrorHolder[0] != null) {
      response.put(ANALYSIS_ERROR, analysisErrorHolder[0]);
    }

    addErrorToResponse(mutableId, solrExc, errorsDuringIndexing, response, updateError);

    response.put("rebuild", needsRebuild);
    return buildFlexibleResponse(response);
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public FlexibleSolrJerseyResponse publish(
      String configSet,
      Integer schemaVersion,
      String newCollection,
      Boolean reloadCollections,
      Integer numShards,
      Integer replicationFactor,
      Boolean indexToCollection,
      Boolean cleanupTempParam,
      Boolean disableDesigner)
      throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);
    requireSchemaVersion(schemaVersion);
    final String mutableId = checkMutable(configSet, schemaVersion);

    // verify the configSet we're going to apply changes to hasn't been changed since being loaded
    // for
    // editing by the schema designer
    SchemaDesignerSettings settings = settingsDAO.getSettings(mutableId);
    final Optional<Integer> publishedVersion = settings.getPublishedVersion();
    if (publishedVersion.isPresent()) {
      int currentVersionOfSrc = configSetHelper.getCurrentSchemaVersion(configSet);
      if (publishedVersion.get() != currentVersionOfSrc) {
        throw new SolrException(
            SolrException.ErrorCode.CONFLICT,
            "Version mismatch for "
                + configSet
                + "! Expected version "
                + publishedVersion.get()
                + " but current is "
                + currentVersionOfSrc
                + "; another user may have changed the published schema while you were making edits. "
                + "Publishing your changes would result in losing the edits from the other user.");
      }
    }

    if (StrUtils.isNotNullOrEmpty(newCollection)
        && zkStateReader().getClusterState().hasCollection(newCollection)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Collection '" + newCollection + "' already exists!");
    }

    if (configExists(configSet)) {
      SolrZkClient zkClient = coreContainer.getZkController().getZkClient();
      try {
        zkClient.zkTransfer(
            getConfigSetZkPath(mutableId), true, getConfigSetZkPath(configSet), true, true);
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(
            "Failed to copy config set: " + mutableId, SolrZkClient.checkInterrupted(e));
      }
    } else {
      copyConfig(mutableId, configSet);
    }

    if (reloadCollections) {
      log.debug("Reloading collections after update to configSet: {}", configSet);
      List<String> collectionsForConfig = configSetHelper.listCollectionsForConfig(configSet);
      CloudSolrClient csc = cloudClient();
      for (String next : collectionsForConfig) {
        CollectionAdminRequest.reloadCollection(next).processAsync(csc);
      }
    }

    // create new collection
    Map<Object, Throwable> errorsDuringIndexing = null;
    if (StrUtils.isNotNullOrEmpty(newCollection)) {
      configSetHelper.createCollection(newCollection, configSet, numShards, replicationFactor);
      if (indexToCollection) {
        List<SolrInputDocument> docs = configSetHelper.retrieveSampleDocs(configSet);
        if (!docs.isEmpty()) {
          ManagedIndexSchema schema = loadLatestSchema(mutableId);
          errorsDuringIndexing =
              indexSampleDocs(schema.getUniqueKeyField().getName(), docs, newCollection, true);
        }
      }
    }

    if (cleanupTempParam) {
      try {
        doCleanupTemp(configSet);
      } catch (IOException | SolrServerException | SolrException exc) {
        final String excStr = exc.toString();
        log.warn("Failed to clean-up temp collection {} due to: {}", mutableId, excStr);
      }
    }

    settings.setDisabled(disableDesigner);
    settingsDAO.persistIfChanged(configSet, settings);

    Map<String, Object> response = new HashMap<>();
    response.put(CONFIG_SET_PARAM, configSet);
    response.put(SCHEMA_VERSION_PARAM, configSetHelper.getCurrentSchemaVersion(configSet));
    if (StrUtils.isNotNullOrEmpty(newCollection)) {
      response.put(NEW_COLLECTION_PARAM, newCollection);
    }

    addErrorToResponse(newCollection, null, errorsDuringIndexing, response, null);

    return buildFlexibleResponse(response);
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public FlexibleSolrJerseyResponse analyze(
      String configSet,
      Integer schemaVersion,
      String copyFrom,
      String uniqueKeyField,
      List<String> languages,
      Boolean enableDynamicFields,
      Boolean enableFieldGuessing,
      Boolean enableNestedDocs)
      throws Exception {
    final int schemaVersionInt = schemaVersion != null ? schemaVersion : -1;
    requireNotEmpty(CONFIG_SET_PARAM, configSet);

    // don't let the user edit the _default configSet with the designer (for now)
    if (DEFAULT_CONFIGSET_NAME.equals(configSet)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "'"
              + DEFAULT_CONFIGSET_NAME
              + "' is a reserved configSet name! Please choose a different name.");
    }
    validateNewConfigSetName(configSet);

    // Get the sample documents to analyze, preferring those in the request but falling back to
    // previously stored
    SampleDocuments sampleDocuments = loadSampleDocuments(configSet);

    // Get a mutable "temp" schema either from the specified copy source or configSet if it already
    // exists.
    if (copyFrom == null) {
      copyFrom = configExists(configSet) ? configSet : DEFAULT_CONFIGSET_NAME;
    }

    String mutableId = getMutableId(configSet);

    // holds additional settings needed by the designer to maintain state
    SchemaDesignerSettings settings =
        getMutableSchemaForConfigSet(configSet, schemaVersionInt, copyFrom);
    ManagedIndexSchema schema = settings.getSchema();

    if (StrUtils.isNotNullOrEmpty(uniqueKeyField)) {
      String existingKeyField =
          schema.getUniqueKeyField() != null ? schema.getUniqueKeyField().getName() : null;
      if (!uniqueKeyField.equals(existingKeyField)) {
        // The Schema API doesn't support changing the ID field so would have to use XML directly
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Changing the unique key field not supported yet!");
      }
    }

    boolean langsUpdated = false;
    List<String> langs;
    if (languages != null) {
      langs =
          languages.isEmpty() || (languages.size() == 1 && "*".equals(languages.getFirst()))
              ? List.of()
              : languages;
      if (!langs.equals(settings.getLanguages())) {
        settings.setLanguages(langs);
        langsUpdated = true;
      }
    } else {
      // nothing from client, go with what's stored in the settings ...
      langs = settings.getLanguages();
    }

    boolean dynamicUpdated = false;
    if (enableDynamicFields != null && enableDynamicFields != settings.dynamicFieldsEnabled()) {
      settings.setDynamicFieldsEnabled(enableDynamicFields);
      dynamicUpdated = true;
    }

    if (langsUpdated) {
      schema =
          configSetHelper.syncLanguageSpecificObjectsAndFiles(
              mutableId, schema, langs, settings.dynamicFieldsEnabled(), copyFrom);
    }

    if (dynamicUpdated) {
      if (!enableDynamicFields) {
        schema = configSetHelper.removeDynamicFields(schema);
      } else {
        schema = configSetHelper.restoreDynamicFields(schema, langs, copyFrom);
      }
    }

    List<SolrInputDocument> docs = sampleDocuments.parsed;
    if (!docs.isEmpty()) {
      if (sampleDocLoader.ensureUniqueKey(schema.getUniqueKeyField(), docs)) {
        configSetHelper.storeSampleDocs(configSet, docs);
      }
      schema = analyzeInputDocs(schemaSuggester.transposeDocs(docs), schema, langs);
    }

    // persist the updated schema
    schema.persistManagedSchema(false);

    if (enableFieldGuessing != null && enableFieldGuessing != settings.fieldGuessingEnabled()) {
      settings.setFieldGuessingEnabled(enableFieldGuessing);
    }

    // make sure the temp collection for this analysis exists
    if (!zkStateReader().getClusterState().hasCollection(mutableId)) {
      configSetHelper.createCollection(mutableId, mutableId);
      indexedVersion.remove(mutableId);
    } else {
      // already created in the prep step ... reload it to pull in the updated schema
      CollectionAdminRequest.reloadCollection(mutableId).process(cloudClient());
    }

    // nested docs
    if (enableNestedDocs != null && enableNestedDocs != settings.nestedDocsEnabled()) {
      settings.setNestedDocsEnabled(enableNestedDocs);
      configSetHelper.toggleNestedDocsFields(schema, enableNestedDocs);
    }

    // index the sample docs using the suggested schema
    String[] analysisErrorHolder = new String[1];
    Map<Object, Throwable> errorsDuringIndexing = null;
    if (!docs.isEmpty()) {
      String idField = schema.getUniqueKeyField().getName();
      errorsDuringIndexing =
          indexSampleDocsWithRebuildOnAnalysisError(
              idField, docs, mutableId, false, analysisErrorHolder);
    }

    if (settingsDAO.persistIfChanged(mutableId, settings)) {
      CollectionAdminRequest.reloadCollection(mutableId).process(cloudClient());
    }

    Map<String, Object> response =
        buildResponse(configSet, loadLatestSchema(mutableId), settings, docs);
    response.put("sampleSource", sampleDocuments.getSource());
    if (analysisErrorHolder[0] != null) {
      response.put(ANALYSIS_ERROR, analysisErrorHolder[0]);
    }
    addErrorToResponse(mutableId, null, errorsDuringIndexing, response, null);
    return buildFlexibleResponse(response);
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public FlexibleSolrJerseyResponse query(String configSet) throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);
    String mutableId = getMutableId(configSet);
    if (!configExists(mutableId)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          mutableId
              + " configSet not found! Are you sure "
              + configSet
              + " was being edited by the schema designer?");
    }

    if (!zkStateReader().getClusterState().hasCollection(mutableId)) {
      indexedVersion.remove(mutableId);
      configSetHelper.createCollection(mutableId, mutableId);
    }

    // only re-index if current state of test collection is not up-to-date
    int currentVersion = configSetHelper.getCurrentSchemaVersion(mutableId);
    Integer version = indexedVersion.get(mutableId);
    Map<Object, Throwable> errorsDuringIndexing = null;
    if (version == null || version != currentVersion) {
      log.debug(
          "Schema for collection {} is stale ({} != {}), need to re-index sample docs",
          mutableId,
          version,
          currentVersion);
      List<SolrInputDocument> docs = configSetHelper.retrieveSampleDocs(configSet);
      if (!docs.isEmpty()) {
        ManagedIndexSchema schema = loadLatestSchema(mutableId);
        errorsDuringIndexing =
            indexSampleDocsWithRebuildOnAnalysisError(
                schema.getUniqueKeyField().getName(), docs, mutableId, true, null);
        // the version changes when you index (due to field guessing URP)
        currentVersion = configSetHelper.getCurrentSchemaVersion(mutableId);
      }
      indexedVersion.put(mutableId, currentVersion);
    }

    if (errorsDuringIndexing != null) {
      Map<String, Object> errorResponse = new HashMap<>();
      addErrorToResponse(
          mutableId,
          new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Failed to re-index sample documents after schema updated."),
          errorsDuringIndexing,
          errorResponse,
          "Failed to re-index sample documents after schema updated.");
      return buildFlexibleResponse(errorResponse);
    }

    // execute the user's query against the temp collection
    QueryResponse qr = cloudClient().query(mutableId, solrQueryRequest.getParams());
    Map<String, Object> responseMap = new HashMap<>();
    qr.getResponse()
        .forEach(
            (name, val) -> {
              if ("response".equals(name) && val instanceof SolrDocumentList) {
                // SolrDocumentList extends ArrayList, so Jackson would serialize it as a plain
                // array, losing numFound/start metadata that the UI expects at data.response.docs
                SolrDocumentList docList = (SolrDocumentList) val;
                Map<String, Object> responseObj = new HashMap<>();
                responseObj.put("numFound", docList.getNumFound());
                responseObj.put("start", docList.getStart());
                responseObj.put("docs", new ArrayList<>(docList));
                responseMap.put(name, responseObj);
              } else {
                responseMap.put(name, val);
              }
            });
    return buildFlexibleResponse(responseMap);
  }

  /**
   * Return the diff of designer schema with the source schema (either previously published or the
   * copyFrom).
   */
  @Override
  @PermissionName(CONFIG_READ_PERM)
  public FlexibleSolrJerseyResponse getSchemaDiff(String configSet) throws Exception {
    requireNotEmpty(CONFIG_SET_PARAM, configSet);

    SchemaDesignerSettings settings = getMutableSchemaForConfigSet(configSet, -1, null);
    // diff the published if found, else use the original source schema
    String sourceSchema = configExists(configSet) ? configSet : settings.getCopyFrom();
    Map<String, Object> response = new HashMap<>();
    response.put(
        "diff", ManagedSchemaDiff.diff(loadLatestSchema(sourceSchema), settings.getSchema()));
    response.put("diff-source", sourceSchema);
    addSettingsToResponse(settings, response);
    return buildFlexibleResponse(response);
  }

  protected SampleDocuments loadSampleDocuments(String configSet) throws IOException {
    List<SolrInputDocument> docs = null;
    ContentStream stream = extractSingleContentStream(false);
    SampleDocuments sampleDocs = null;
    if (stream != null && stream.getContentType() != null) {
      sampleDocs =
          sampleDocLoader.parseDocsFromStream(
              solrQueryRequest.getParams(), stream, MAX_SAMPLE_DOCS);
      docs = sampleDocs.parsed;
      if (!docs.isEmpty()) {
        // user posted in some docs, if there are already docs stored in the blob store, then add
        // these to the existing set
        List<SolrInputDocument> stored = configSetHelper.retrieveSampleDocs(configSet);
        if (!stored.isEmpty()) {
          // keep the docs in the request as newest
          ManagedIndexSchema latestSchema = loadLatestSchema(getMutableId(configSet));
          docs =
              sampleDocs.appendDocs(
                  latestSchema.getUniqueKeyField().getName(), stored, MAX_SAMPLE_DOCS);
        }

        // store in the Filestore so that we always have access to these docs
        configSetHelper.storeSampleDocs(configSet, docs);
      }
    }

    if (docs == null || docs.isEmpty()) {
      // no sample docs in the request ... find in blob store (or fail if no docs previously stored)
      docs = configSetHelper.retrieveSampleDocs(configSet);

      // no docs? but if this schema has already been published, it's OK, we can skip the docs part
      if (docs.isEmpty() && !configExists(configSet)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "No sample documents provided for analyzing schema! Only CSV/TSV, XML, JSON, and JSON lines supported.");
      }

      sampleDocs = new SampleDocuments(docs, "", "blob");
    }

    return sampleDocs;
  }

  protected ManagedIndexSchema analyzeInputDocs(
      final Map<String, List<Object>> docs, ManagedIndexSchema schema, List<String> langs) {
    // collect the fields to add ... adding all fields at once is faster than one-at-a-time
    List<SchemaField> fieldsToAdd = new ArrayList<>();
    for (String field : docs.keySet()) {
      List<Object> sampleValues = docs.getOrDefault(field, List.of());

      // Collapse all whitespace in fields to a single underscore
      String normalizedField = field.trim().replaceAll("\\s+", "_");
      if (schema.hasExplicitField(normalizedField)) {
        SchemaField existing = schema.getField(normalizedField);
        schema = schemaSuggester.adaptExistingFieldToData(existing, sampleValues, schema);
        continue;
      }

      Optional<SchemaField> maybeSchemaField =
          schemaSuggester.suggestField(normalizedField, sampleValues, schema, langs);
      maybeSchemaField.ifPresent(fieldsToAdd::add);
    }
    if (!fieldsToAdd.isEmpty()) {
      schema = (ManagedIndexSchema) schema.addFields(fieldsToAdd);
    }
    return schema;
  }

  SchemaDesignerSettings getMutableSchemaForConfigSet(
      final String configSet, final int schemaVersion, String copyFrom) throws IOException {
    // The designer works with mutable config sets stored in a "temp" znode in ZK instead of the
    // "live" configSet
    final String mutableId = getMutableId(configSet);

    ManagedIndexSchema schema;

    // create new from the built-in "_default" configSet
    int publishedVersion = -1;
    boolean isNew = false;
    if (!configExists(mutableId)) {

      // are they opening a temp of an existing?
      if (configExists(configSet)) {
        if (settingsDAO.isDesignerDisabled(configSet)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Schema '" + configSet + "' is locked for edits by the schema designer!");
        }
        publishedVersion = configSetHelper.getCurrentSchemaVersion(configSet);
        log.info(
            "Opening temp copy of {} as {} with publishedVersion {}",
            configSet,
            mutableId,
            publishedVersion);
        // ignore the copyFrom as we're making a mutable temp copy of an already published configSet
        copyConfig(configSet, mutableId);
        copyFrom = null;
      } else {
        copyConfig(copyFrom, mutableId);
      }
      isNew = true;
    }

    SolrConfig solrConfig = configSetHelper.loadSolrConfig(mutableId);
    schema = configSetHelper.loadLatestSchema(solrConfig);
    if (!isNew) {
      // schema is not new, so the provided version must match, otherwise, we're trying to edit
      // dirty data
      configSetHelper.checkSchemaVersion(mutableId, schemaVersion, schema.getSchemaZkVersion());
    }

    SchemaDesignerSettings settings = settingsDAO.getSettings(solrConfig);
    if (isNew) {
      if (!configSet.equals(copyFrom)) {
        settings.setDisabled(false);
      }

      // remember where this new one came from
      if (copyFrom != null) {
        settings.setCopyFrom(copyFrom);
      }

      if (publishedVersion != -1) {
        // keep track of the version of the configSet the mutable is derived from in case another
        // user changes the derived from schema before we publish the mutable on top of it
        settings.setPublishedVersion(publishedVersion);
      }

      if (!settings.nestedDocsEnabled()) {
        schema = configSetHelper.deleteNestedDocsFieldsIfNeeded(schema, false);
      }

      if (!settings.dynamicFieldsEnabled()) {
        schema = configSetHelper.removeDynamicFields(schema);
      }

      schema.persistManagedSchema(false);
    }

    settings.setSchema(schema);

    return settings;
  }

  ManagedIndexSchema loadLatestSchema(String configSet) {
    return configSetHelper.loadLatestSchema(configSet);
  }

  protected ContentStream extractSingleContentStream(boolean required) {
    Iterable<ContentStream> streams = solrQueryRequest.getContentStreams();
    Iterator<ContentStream> iter = streams != null ? streams.iterator() : null;
    ContentStream stream = iter != null && iter.hasNext() ? iter.next() : null;
    if (required && stream == null)
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "No JSON content found in the request body!");

    return stream;
  }

  protected CloudSolrClient cloudClient() {
    return coreContainer.getZkController().getSolrClient();
  }

  protected ZkStateReader zkStateReader() {
    return coreContainer.getZkController().getZkStateReader();
  }

  protected Map<Object, Throwable> indexSampleDocsWithRebuildOnAnalysisError(
      String idField,
      List<SolrInputDocument> docs,
      final String collectionName,
      boolean asBatch,
      String[] analysisErrorHolder)
      throws IOException, SolrServerException {
    Map<Object, Throwable> results;
    try {
      results = indexSampleDocs(idField, docs, collectionName, asBatch);
    } catch (IllegalArgumentException analysisExc) {
      String errMsg = SolrException.getRootCause(analysisExc).getMessage();
      if (analysisErrorHolder != null) {
        analysisErrorHolder[0] = errMsg;
      }
      log.warn(
          "Rebuilding temp collection {} after low-level Lucene indexing issue: {}",
          collectionName,
          errMsg);
      configSetHelper.reloadTempCollection(collectionName, true);
      results = indexSampleDocs(idField, docs, collectionName, asBatch);
      log.info(
          "Re-index sample docs into {} after rebuild due to {} succeeded; results: {}",
          collectionName,
          errMsg,
          results);
    }
    return results;
  }

  protected Map<Object, Throwable> indexSampleDocs(
      String idField, List<SolrInputDocument> docs, final String collectionName, boolean asBatch)
      throws IOException, SolrServerException {

    Map<Object, Throwable> errorsDuringIndexing = new LinkedHashMap<>();

    RTimer timer = new RTimer();

    // load sample docs from blob store
    CloudSolrClient cloudSolrClient = cloudClient();
    cloudSolrClient.deleteByQuery(collectionName, "*:*", 1);
    cloudSolrClient.optimize(collectionName, true, true, 1);

    final int commitWithin = 100;
    final int numDocs = docs.size();
    int numAdded = 0;
    if (asBatch) {
      cloudSolrClient.add(collectionName, docs, commitWithin);
      numAdded = docs.size();
    } else {
      int maxErrors = Math.min(100, Math.round(numDocs / 2f));
      for (SolrInputDocument next : docs) {
        try {
          cloudSolrClient.add(collectionName, next, commitWithin);
          ++numAdded;
        } catch (Exception exc) {
          Throwable rootCause = SolrException.getRootCause(exc);
          if (String.valueOf(rootCause.getMessage()).contains("possible analysis error")) {
            throw new IllegalArgumentException(rootCause);
          } else {
            Object docId = next.getFieldValue(idField);
            if (docId == null) {
              throw exc;
            }
            errorsDuringIndexing.put(docId, rootCause);

            // break if there are a lot of errors in indexing as something is very wrong if so ...
            if (errorsDuringIndexing.size() > 20 && errorsDuringIndexing.size() >= maxErrors) {
              break;
            }
          }
        }
      }
    }

    cloudSolrClient.commit(collectionName, true, true, true);

    if (!errorsDuringIndexing.isEmpty()) {
      return errorsDuringIndexing;
    }

    long numFound = waitToSeeSampleDocs(collectionName, numAdded);
    double tookMs = timer.getTime();
    log.debug("Indexed {} docs into collection {}, took {} ms", numFound, collectionName, tookMs);

    return !errorsDuringIndexing.isEmpty() ? errorsDuringIndexing : null;
  }

  protected long waitToSeeSampleDocs(String collectionName, long numAdded)
      throws IOException, SolrServerException {
    CloudSolrClient cloudSolrClient = cloudClient();
    SolrQuery query = new SolrQuery("*:*");
    query.setRows(0);
    QueryResponse queryResponse = cloudSolrClient.query(collectionName, query);
    long numFound = queryResponse.getResults().getNumFound();
    if (numFound < numAdded) {
      // wait up to 5 seconds for this to occur
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      do {
        cloudSolrClient.commit(collectionName, true, true, true);
        queryResponse = cloudSolrClient.query(collectionName, query);
        numFound = queryResponse.getResults().getNumFound();
        if (numFound >= numAdded) {
          break;
        }
        try {
          Thread.sleep(200); // little pause to avoid flooding the server with requests in this loop
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      } while (System.nanoTime() < deadline);

      if (numFound < numAdded) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Failed to index " + numAdded + " sample docs into temp collection: " + collectionName);
      }
    }
    return numFound;
  }

  Map<String, Object> buildResponse(
      String configSet,
      final ManagedIndexSchema schema,
      SchemaDesignerSettings settings,
      List<SolrInputDocument> docs)
      throws IOException {
    String mutableId = getMutableId(configSet);
    int currentVersion = configSetHelper.getCurrentSchemaVersion(mutableId);
    indexedVersion.put(mutableId, currentVersion);

    // response is a map of data structures to support the schema designer
    Map<String, Object> response = new HashMap<>();

    DocCollection coll = zkStateReader().getCollection(mutableId);
    Collection<Slice> activeSlices = coll.getActiveSlices();
    if (!activeSlices.isEmpty()) {
      String coreName = activeSlices.stream().findAny().orElseThrow().getLeader().getCoreName();
      response.put("core", coreName);
    }

    response.put(UNIQUE_KEY_FIELD_PARAM, schema.getUniqueKeyField().getName());

    response.put(CONFIG_SET_PARAM, configSet);
    // important: pass the designer the current schema zk version for MVCC
    response.put(SCHEMA_VERSION_PARAM, currentVersion);
    response.put(TEMP_COLLECTION_PARAM, mutableId);
    response.put("collectionsForConfig", configSetHelper.listCollectionsForConfig(configSet));
    // Guess at a schema for each field found in the sample docs
    // Collect all fields across all docs with mapping to values
    response.put(
        "fields",
        schema.getFields().values().stream()
            .map(f -> fieldToMap(f, schema))
            .sorted(Comparator.comparing(map -> ((String) map.get("name"))))
            .collect(Collectors.toList()));

    if (settings == null) {
      settings = settingsDAO.getSettings(mutableId);
    }
    addSettingsToResponse(settings, response);

    response.put(
        "dynamicFields",
        Arrays.stream(schema.getDynamicFieldPrototypes())
            .map(e -> e.getNamedPropertyValues(true))
            .sorted(Comparator.comparing(map -> ((String) map.get("name"))))
            .collect(Collectors.toList()));

    response.put(
        "fieldTypes",
        schema.getFieldTypes().values().stream()
            .map(fieldType -> fieldType.getNamedPropertyValues(true))
            .sorted(Comparator.comparing(map -> ((String) map.get("name"))))
            .collect(Collectors.toList()));

    // files
    SolrZkClient zkClient = zkStateReader().getZkClient();
    String configPathInZk = getConfigSetZkPath(mutableId);
    final Set<String> files = new HashSet<>();
    try {
      ZkMaintenanceUtils.traverseZkTree(
          zkClient, configPathInZk, ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST, files::add);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(
          "Failed to traverse files under: " + configPathInZk, SolrZkClient.checkInterrupted(e));
    }
    files.remove(configPathInZk);

    final String prefix = configPathInZk + "/";
    final int prefixLen = prefix.length();
    Set<String> stripPrefix =
        files.stream()
            .map(f -> f.startsWith(prefix) ? f.substring(prefixLen) : f)
            .collect(Collectors.toSet());
    stripPrefix.remove(schema.getResourceName());
    stripPrefix.remove("lang");
    stripPrefix.remove(CONFIGOVERLAY_JSON); // treat this file as private

    List<String> sortedFiles = new ArrayList<>(stripPrefix);
    Collections.sort(sortedFiles);
    response.put("files", sortedFiles);

    // info about the sample docs
    if (docs != null) {
      final String uniqueKeyField = schema.getUniqueKeyField().getName();
      response.put(
          "docIds",
          docs.stream()
              .map(d -> (String) d.getFieldValue(uniqueKeyField))
              .filter(Objects::nonNull)
              .limit(100)
              .collect(Collectors.toList()));
    }

    response.put("numDocs", docs != null ? docs.size() : -1);

    return response;
  }

  protected void addErrorToResponse(
      String collection,
      SolrException solrExc,
      Map<Object, Throwable> errorsDuringIndexing,
      Map<String, Object> response,
      String updateError) {

    if (solrExc == null && (errorsDuringIndexing == null || errorsDuringIndexing.isEmpty())) {
      return; // no errors
    }

    if (updateError != null) {
      response.put(UPDATE_ERROR, updateError);
    }

    if (solrExc != null) {
      response.put("updateErrorCode", solrExc.code());
      response.putIfAbsent(UPDATE_ERROR, solrExc.getMessage());
    }

    response.putIfAbsent(UPDATE_ERROR, "Index sample documents into " + collection + " failed!");
    response.putIfAbsent("updateErrorCode", 400);
    if (errorsDuringIndexing != null) {
      response.put(ERROR_DETAILS, errorsDuringIndexing);
    }
  }

  protected SimpleOrderedMap<Object> fieldToMap(SchemaField f, ManagedIndexSchema schema) {
    SimpleOrderedMap<Object> map = f.getNamedPropertyValues(true);

    // add the copy field destination field names
    List<String> copyFieldNames =
        schema.getCopyFieldsList((String) map.get("name")).stream()
            .map(c -> c.getDestination().getName())
            .collect(Collectors.toList());
    map.add("copyDest", String.join(",", copyFieldNames));

    return map;
  }

  @SuppressWarnings("unchecked")
  protected Map<String, Object> readJsonFromRequest() throws IOException {
    ContentStream stream = extractSingleContentStream(true);
    String contentType = stream.getContentType();
    if (StrUtils.isNullOrEmpty(contentType)
        || !contentType.toLowerCase(Locale.ROOT).contains(JSON_MIME)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Expected JSON in request!");
    }
    final Object json;
    try (Reader reader = stream.getReader()) {
      json = ObjectBuilder.getVal(new JSONParser(reader));
    }
    return (Map<String, Object>) json;
  }

  void addSettingsToResponse(SchemaDesignerSettings settings, final Map<String, Object> response) {
    response.put(LANGUAGES_PARAM, settings.getLanguages());
    response.put(ENABLE_FIELD_GUESSING_PARAM, settings.fieldGuessingEnabled());
    response.put(ENABLE_DYNAMIC_FIELDS_PARAM, settings.dynamicFieldsEnabled());
    response.put(ENABLE_NESTED_DOCS_PARAM, settings.nestedDocsEnabled());
    response.put(DISABLED, settings.isDisabled());
    Optional<Integer> publishedVersion = settings.getPublishedVersion();
    publishedVersion.ifPresent(version -> response.put(PUBLISHED_VERSION, version));
    String copyFrom = settings.getCopyFrom();
    if (copyFrom != null) {
      response.put(COPY_FROM_PARAM, copyFrom);
    }
  }

  protected String checkMutable(String configSet, int clientSchemaVersion) throws IOException {
    // an apply just copies over the temp config to the "live" location
    String mutableId = getMutableId(configSet);
    if (!configExists(mutableId)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          mutableId
              + " configSet not found! Are you sure "
              + configSet
              + " was being edited by the schema designer?");
    }

    final int schemaVersionInZk = configSetHelper.getCurrentSchemaVersion(mutableId);
    if (schemaVersionInZk != -1) {
      // check the versions agree
      configSetHelper.checkSchemaVersion(mutableId, clientSchemaVersion, schemaVersionInZk);
    } // else the stored is -1, can't really enforce here

    return mutableId;
  }

  protected void requireSchemaVersion(Integer schemaVersion) {
    if (schemaVersion == null || schemaVersion < 0) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, SCHEMA_VERSION_PARAM + " is a required parameter!");
    }
  }

  protected void requireNotEmpty(final String param, final String value) {
    if (StrUtils.isNullOrEmpty(value)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, param + " is a required parameter!");
    }
  }

  protected void doCleanupTemp(String configSet) throws IOException, SolrServerException {
    String mutableId = getMutableId(configSet);
    indexedVersion.remove(mutableId);
    CollectionAdminRequest.deleteCollection(mutableId).process(cloudClient());
    configSetHelper.deleteStoredSampleDocs(configSet);
    deleteConfig(mutableId);
  }

  protected FlexibleSolrJerseyResponse buildFlexibleResponse(Map<String, Object> responseMap) {
    FlexibleSolrJerseyResponse response =
        instantiateJerseyResponse(FlexibleSolrJerseyResponse.class);
    responseMap.forEach(response::setUnknownProperty);
    return response;
  }

  private boolean configExists(String configSet) throws IOException {
    return coreContainer.getConfigSetService().checkConfigExists(configSet);
  }

  private void deleteConfig(String configSet) throws IOException {
    coreContainer.getConfigSetService().deleteConfig(configSet);
  }

  private void copyConfig(String from, String to) throws IOException {
    coreContainer.getConfigSetService().copyConfig(from, to);
  }

  private void validateNewConfigSetName(String configSet) {
    if (configSet.length() > 50) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Schema name should be 50 characters or less");
    }
    if (configSet.contains(" ") || configSet.contains("/")) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Schema name should not contain spaces or forward slash");
    }
  }

  private boolean pathExistsInZk(final String zkPath) throws IOException {
    SolrZkClient zkClient = zkStateReader().getZkClient();
    try {
      return zkClient.exists(zkPath);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(
          "Failed to check if path exists: " + zkPath, SolrZkClient.checkInterrupted(e));
    }
  }

  private static class InMemoryResourceLoader extends SolrResourceLoader {
    String resource;
    byte[] data;
    ZkSolrResourceLoader delegate;

    public InMemoryResourceLoader(
        CoreContainer cc, String configSet, String resource, byte[] data) {
      super(cc.getResourceLoader().getInstancePath(), cc.getResourceLoader().getClassLoader());

      final SolrResourceLoader resourceLoader = cc.getResourceLoader();
      this.delegate =
          new ZkSolrResourceLoader(
              resourceLoader.getInstancePath(),
              configSet,
              resourceLoader.getClassLoader(),
              cc.getZkController());
      this.resource = resource;
      this.data = data;
    }

    @Override
    public InputStream openResource(String res) throws IOException {
      return (this.resource.equals(res))
          ? new ByteArrayInputStream(data)
          : delegate.openResource(res);
    }
  }
}
