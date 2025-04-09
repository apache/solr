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
package org.apache.solr.schema;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.client.api.model.SchemaChange.OPERATION_TYPE_PROP;
import static org.apache.solr.common.util.CommandOperation.ERR_MSGS;
import static org.apache.solr.schema.IndexSchema.MAX_CHARS;
import static org.apache.solr.schema.IndexSchema.NAME;
import static org.apache.solr.schema.IndexSchema.TYPE;
import static org.apache.solr.schema.SchemaManagerUtils.convertToMap;
import static org.apache.solr.schema.SchemaManagerUtils.convertToMapExcluding;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.api.model.AddCopyFieldOperation;
import org.apache.solr.client.api.model.DeleteCopyFieldOperation;
import org.apache.solr.client.api.model.DeleteDynamicFieldOperation;
import org.apache.solr.client.api.model.DeleteFieldOperation;
import org.apache.solr.client.api.model.DeleteFieldTypeOperation;
import org.apache.solr.client.api.model.SchemaChange;
import org.apache.solr.client.api.model.UpsertDynamicFieldOperation;
import org.apache.solr.client.api.model.UpsertFieldOperation;
import org.apache.solr.client.api.model.UpsertFieldTypeOperation;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to manipulate schema using the bulk mode. This class takes in all the commands
 * and processes them completely. It is an all or nothing operation.
 */
public class SchemaManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final SolrQueryRequest req;
  ManagedIndexSchema managedIndexSchema;
  int updateTimeOut;

  public SchemaManager(SolrQueryRequest req) {
    this.req = req;

    // The default timeout is 10 minutes when no BaseSolrResource.UPDATE_TIMEOUT_SECS is specified
    int defaultUpdateTimeOut = Integer.getInteger("solr.schemaUpdateTimeoutSeconds", 600);
    updateTimeOut =
        req.getParams().getInt(BaseSolrResource.UPDATE_TIMEOUT_SECS, defaultUpdateTimeOut);

    // If BaseSolrResource.UPDATE_TIMEOUT_SECS=0 or -1 then end time then we'll try for 10 mins (
    // default timeout )
    if (updateTimeOut < 1) {
      updateTimeOut = defaultUpdateTimeOut;
    }
  }

  // TODO Replace the error-list used here with something more intuitive?
  /**
   * Take in a set of schema commands and execute them. It tries to capture as many errors as
   * possible instead of failing at the first error it encounters
   *
   * @return List of errors. If the List is empty then the operation was successful.
   */
  public List<Map<String, Object>> performOperations(List<SchemaChange> ops) throws Exception {
    IndexSchema schema = req.getCore().getLatestSchema();
    if (schema instanceof ManagedIndexSchema && schema.isMutable()) {
      return doOperations(ops);
    } else {
      return singletonList(singletonMap(ERR_MSGS, "schema is not editable"));
    }
  }

  private List<Map<String, Object>> doOperations(List<SchemaChange> operations)
      throws InterruptedException, IOException, KeeperException {
    TimeOut timeOut = new TimeOut(updateTimeOut, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    SolrCore core = req.getCore();
    String errorMsg = "Unable to persist managed schema. ";
    List<Map<String, Object>> errors = new ArrayList<>();
    int latestVersion = -1;

    synchronized (req.getSchema().getSchemaUpdateLock()) {
      while (!timeOut.hasTimedOut()) {
        managedIndexSchema = getFreshManagedSchema(req.getCore());
        for (SchemaChange op : operations) {
          OpType opType = OpType.get(op.operationType);
          try {
            opType.perform(op, this);
          } catch (SchemaOperationException soe) {
            errors.add(
                Map.of(
                    op.operationType,
                    SchemaManagerUtils.convertToMap(op),
                    ERR_MSGS,
                    singletonList(soe.getMessage())));
          }
        }
        if (!errors.isEmpty()) break;
        SolrResourceLoader loader = req.getCore().getResourceLoader();
        if (loader instanceof ZkSolrResourceLoader zkLoader) {
          StringWriter sw = new StringWriter();
          try {
            managedIndexSchema.persist(sw);
          } catch (IOException e) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR, "unable to serialize schema");
            // unlikely
          }

          try {
            SolrConfigHandler configHandler =
                ((SolrConfigHandler) req.getCore().getRequestHandler("/config"));
            if (configHandler.getReloadLock().tryLock()) {
              try {
                latestVersion =
                    ZkController.persistConfigResourceToZooKeeper(
                        zkLoader,
                        managedIndexSchema.getSchemaZkVersion(),
                        managedIndexSchema.getResourceName(),
                        sw.toString().getBytes(StandardCharsets.UTF_8),
                        true);
                req.getCore()
                    .getCoreContainer()
                    .reload(req.getCore().getName(), req.getCore().uniqueId);
                break;
              } finally {
                configHandler.getReloadLock().unlock();
              }
            } else {
              log.info("Another reload is in progress. Not doing anything.");
            }
          } catch (ZkController.ResourceModifiedInZkException e) {
            log.info("Schema was modified by another node. Retrying..");
          }
        } else {
          try {
            // only for non cloud stuff
            managedIndexSchema.persistManagedSchema(false);
            core.setLatestSchema(managedIndexSchema);
            core.getCoreContainer().reload(core.getName(), core.uniqueId);
          } catch (SolrException e) {
            log.warn(errorMsg);
            errors =
                singletonList(singletonMap(CommandOperation.ERR_MSGS, errorMsg + e.getMessage()));
          }
          break;
        }
      }
    }
    if (req.getCore().getResourceLoader() instanceof ZkSolrResourceLoader) {
      // Don't block further schema updates while waiting for a pending update to propagate to other
      // replicas. This reduces the likelihood of a (time-limited) distributed deadlock during
      // concurrent schema updates.
      waitForOtherReplicasToUpdate(timeOut, latestVersion);
    }
    if (errors.isEmpty() && timeOut.hasTimedOut()) {
      log.warn("{} Timed out", errorMsg);
      errors = singletonList(singletonMap(CommandOperation.ERR_MSGS, errorMsg + "Timed out."));
    }
    return errors;
  }

  private void waitForOtherReplicasToUpdate(TimeOut timeOut, int latestVersion) {
    SolrCore core = req.getCore();
    CoreDescriptor cd = core.getCoreDescriptor();
    String collection = cd.getCollectionName();
    if (collection != null) {
      if (timeOut.hasTimedOut()) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Not enough time left to update replicas. However, the schema is updated already.");
      }
      ManagedIndexSchema.waitForSchemaZkVersionAgreement(
          collection,
          cd.getCloudDescriptor().getCoreNodeName(),
          latestVersion,
          core.getCoreContainer().getZkController(),
          (int) timeOut.timeLeft(TimeUnit.SECONDS));
    }
  }

  /**
   * Represents an error encountered while processing a schema-change operation
   *
   * <p>Does not necessarily terminate schema modification, in the case of bulk operations.
   */
  private static class SchemaOperationException extends Exception {
    public SchemaOperationException() {
      super();
    }

    public SchemaOperationException(String message) {
      super(message);
    }
  }

  private static <T> T ensureNotNull(String name, T value) throws SchemaOperationException {
    if (value == null) {
      throw new SchemaOperationException("'" + name + "' is a required field");
    }

    return value;
  }

  public enum OpType {
    ADD_FIELD_TYPE("add-field-type") {
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var addFieldTypeOp = (UpsertFieldTypeOperation) op;
        String name = ensureNotNull("name", addFieldTypeOp.name);
        String className = ensureNotNull("class", addFieldTypeOp.className);
        try {
          FieldType fieldType =
              mgr.managedIndexSchema.newFieldType(name, className, convertToMap(addFieldTypeOp));
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.addFieldTypes(singletonList(fieldType), false);
          return true;
        } catch (Exception e) {
          log.error("Could not add field type.", e);
          throw new SchemaOperationException(getErrorStr(e));
        }
      }
    },
    ADD_COPY_FIELD("add-copy-field") {
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var addCopyFieldOp = (AddCopyFieldOperation) op;
        String src = ensureNotNull("source", addCopyFieldOp.source);
        List<String> dests = ensureNotNull("dest", addCopyFieldOp.destinations);

        // If maxChars is not specified, there is no limit on copied chars
        final var maxChars =
            addCopyFieldOp.maxChars == null ? CopyField.UNLIMITED : addCopyFieldOp.maxChars;
        if (maxChars < 0) {
          throw new SchemaOperationException(MAX_CHARS + " '" + maxChars + "' is negative.");
        }

        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.addCopyFields(src, dests, maxChars);
          return true;
        } catch (Exception e) {
          log.error("Could not add copy field", e);
          throw new SchemaOperationException(getErrorStr(e));
        }
      }
    },
    UPSERT_FIELD("upsert-field") { // Internal only
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var addFieldOp = (UpsertFieldOperation) op;
        String name = ensureNotNull("name", addFieldOp.name);
        String type = ensureNotNull("type", addFieldOp.type);

        if (mgr.managedIndexSchema.fields.containsKey(addFieldOp.name)) {
          return OpType.REPLACE_FIELD.perform(op, mgr);
        } else {
          return OpType.ADD_FIELD.perform(op, mgr);
        }
      }
    },
    ADD_FIELD("add-field") {
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var addFieldOp = (UpsertFieldOperation) op;
        String name = ensureNotNull("name", addFieldOp.name);
        String type = ensureNotNull("type", addFieldOp.type);
        try {
          final String[] propsToOmit = new String[] {OPERATION_TYPE_PROP, NAME, TYPE};
          SchemaField field =
              mgr.managedIndexSchema.newField(
                  name, type, convertToMapExcluding(addFieldOp, propsToOmit));
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.addFields(singletonList(field), Collections.emptyMap(), false);
          return true;
        } catch (Exception e) {
          log.error("Could not add field", e);
          throw new SchemaOperationException(getErrorStr(e));
        }
      }
    },
    UPSERT_DYNAMIC_FIELD("upsert-dynamic-field") { // Internal only
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var addDynFieldOp = (UpsertDynamicFieldOperation) op;
        String name = ensureNotNull("name", addDynFieldOp.name);

        final boolean fieldExists =
            Arrays.stream(mgr.managedIndexSchema.dynamicFields)
                .anyMatch(df -> df.getRegex().equals(name));
        if (fieldExists) {
          return OpType.REPLACE_DYNAMIC_FIELD.perform(op, mgr);
        } else {
          return OpType.ADD_DYNAMIC_FIELD.perform(op, mgr);
        }
      }
    },
    ADD_DYNAMIC_FIELD("add-dynamic-field") {
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var addDynFieldOp = (UpsertDynamicFieldOperation) op;
        String name = ensureNotNull("name", addDynFieldOp.name);
        String type = ensureNotNull("type", addDynFieldOp.type);
        try {
          final String[] propsToOmit = new String[] {OPERATION_TYPE_PROP, NAME, TYPE};
          SchemaField field =
              mgr.managedIndexSchema.newDynamicField(
                  name, type, convertToMapExcluding(addDynFieldOp, propsToOmit));
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.addDynamicFields(
                  singletonList(field), Collections.emptyMap(), false);
          return true;
        } catch (Exception e) {
          log.error("Could not add dynamic field", e);
          throw new SchemaOperationException(getErrorStr(e));
        }
      }
    },
    DELETE_FIELD_TYPE("delete-field-type") {
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var deleteFieldTypeOp = (DeleteFieldTypeOperation) op;
        String name = ensureNotNull("name", deleteFieldTypeOp.name);
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.deleteFieldTypes(singleton(name));
          return true;
        } catch (Exception e) {
          log.error("Could not delete field type", e);
          throw new SchemaOperationException(getErrorStr(e));
        }
      }
    },
    DELETE_COPY_FIELD("delete-copy-field") {
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var deleteCopyField = (DeleteCopyFieldOperation) op;
        String source = ensureNotNull("source", deleteCopyField.source);
        List<String> dests = ensureNotNull("dest", deleteCopyField.destinations);
        try {
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.deleteCopyFields(singletonMap(source, dests));
          return true;
        } catch (Exception e) {
          log.error("Could not delete copy field", e);
          throw new SchemaOperationException(getErrorStr(e));
        }
      }
    },
    DELETE_FIELD("delete-field") {
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var deleteFieldOp = (DeleteFieldOperation) op;
        String name = ensureNotNull("name", deleteFieldOp.name);
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.deleteFields(singleton(name));
          return true;
        } catch (Exception e) {
          log.error("Could not delete field", e);
          throw new SchemaOperationException(getErrorStr(e));
        }
      }
    },
    DELETE_DYNAMIC_FIELD("delete-dynamic-field") {
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var deleteDynFieldOp = (DeleteDynamicFieldOperation) op;
        String name = ensureNotNull("name", deleteDynFieldOp.name);
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.deleteDynamicFields(singleton(name));
          return true;
        } catch (Exception e) {
          log.error("Could not delete dynamic field", e);
          throw new SchemaOperationException(getErrorStr(e));
        }
      }
    },
    UPSERT_FIELD_TYPE("upsert-field-type") { // Internal only, to support v2 API
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var upsertFieldTypeOp = (UpsertFieldTypeOperation) op;
        String name = ensureNotNull("name", upsertFieldTypeOp.name);

        if (mgr.managedIndexSchema.fieldTypes.containsKey(name)) {
          return OpType.REPLACE_FIELD_TYPE.perform(op, mgr);
        } else {
          return OpType.ADD_FIELD_TYPE.perform(op, mgr);
        }
      }
    },
    REPLACE_FIELD_TYPE("replace-field-type") {
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var replaceFieldTypeOp = (UpsertFieldTypeOperation) op;
        String name = ensureNotNull("name", replaceFieldTypeOp.name);
        String className = ensureNotNull("class", replaceFieldTypeOp.className);
        try {
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.replaceFieldType(
                  name, className, convertToMap(replaceFieldTypeOp));
          return true;
        } catch (Exception e) {
          log.error("Could not replace field type", e);
          throw new SchemaOperationException(getErrorStr(e));
        }
      }
    },
    REPLACE_FIELD("replace-field") {
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var replaceFieldOp = (UpsertFieldOperation) op;
        String name = ensureNotNull("name", replaceFieldOp.name);
        String type = ensureNotNull("type", replaceFieldOp.type);
        FieldType ft = mgr.managedIndexSchema.getFieldTypeByName(type);
        if (ft == null) {
          throw new SchemaOperationException("No such field type '" + type + "'");
        }
        try {
          final String[] propsToOmit = new String[] {OPERATION_TYPE_PROP, NAME, TYPE};
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.replaceField(
                  name, ft, convertToMapExcluding(replaceFieldOp, propsToOmit));
          return true;
        } catch (Exception e) {
          log.error("Could not replace field", e);
          throw new SchemaOperationException(getErrorStr(e));
        }
      }
    },
    REPLACE_DYNAMIC_FIELD("replace-dynamic-field") {
      @Override
      public boolean perform(SchemaChange op, SchemaManager mgr) throws SchemaOperationException {
        final var replaceDynFieldOp = (UpsertDynamicFieldOperation) op;
        String name = ensureNotNull("name", replaceDynFieldOp.name);
        String type = ensureNotNull("type", replaceDynFieldOp.type);
        FieldType ft = mgr.managedIndexSchema.getFieldTypeByName(type);
        if (ft == null) {
          throw new SchemaOperationException("No such field type '" + type + "'");
        }
        try {
          final String[] propsToOmit = new String[] {OPERATION_TYPE_PROP, NAME, TYPE};
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.replaceDynamicField(
                  name, ft, convertToMapExcluding(replaceDynFieldOp, propsToOmit));
          return true;
        } catch (Exception e) {
          log.error("Could not replace dynamic field", e);
          throw new SchemaOperationException(getErrorStr(e));
        }
      }
    };

    public abstract boolean perform(SchemaChange op, SchemaManager mgr)
        throws SchemaOperationException;

    public static OpType get(String label) {
      return Nested.OP_TYPES.get(label);
    }

    private static class Nested { // Initializes contained static map before any enum ctor
      static final Map<String, OpType> OP_TYPES = new HashMap<>();
    }

    private OpType(String label) {
      Nested.OP_TYPES.put(label, this);
    }
  }

  public static String getErrorStr(Exception e) {
    StringBuilder sb = new StringBuilder();
    Throwable cause = e;
    for (int i = 0; i < 5; i++) {
      sb.append(cause.getMessage()).append("\n");
      if (cause.getCause() == null || cause.getCause() == cause) break;
      cause = cause.getCause();
    }
    return sb.toString();
  }

  private ManagedIndexSchema getFreshManagedSchema(SolrCore core)
      throws IOException, KeeperException, InterruptedException {

    SolrResourceLoader resourceLoader = core.getResourceLoader();
    String schemaResourceName = core.getLatestSchema().getResourceName();
    if (resourceLoader instanceof ZkSolrResourceLoader zkLoader) {
      SolrZkClient zkClient = zkLoader.getZkController().getZkClient();
      String managedSchemaPath = zkLoader.getConfigSetZkPath() + "/" + schemaResourceName;
      try {
        if (!zkClient.exists(managedSchemaPath, true)) {
          String backupName =
              schemaResourceName + ManagedIndexSchemaFactory.UPGRADED_SCHEMA_EXTENSION;
          if (!zkClient.exists(zkLoader.getConfigSetZkPath() + "/" + backupName, true)) {
            log.warn(
                "Unable to retrieve fresh managed schema, neither {} nor {} exist.",
                schemaResourceName,
                backupName);
            // use current schema
            return (ManagedIndexSchema) core.getLatestSchema();
          } else {
            schemaResourceName = backupName;
          }
        }
      } catch (Exception e) {
        log.warn("Unable to retrieve fresh managed schema {}", schemaResourceName, e);
        // use current schema
        return (ManagedIndexSchema) core.getLatestSchema();
      }
      schemaResourceName = managedSchemaPath.substring(managedSchemaPath.lastIndexOf('/') + 1);
      InputStream in = resourceLoader.openResource(schemaResourceName);
      if (in instanceof ZkSolrResourceLoader.ZkByteArrayInputStream) {
        int version = ((ZkSolrResourceLoader.ZkByteArrayInputStream) in).getStat().getVersion();
        log.info("managed schema loaded . version : {} ", version);
        return new ManagedIndexSchema(
            core.getSolrConfig(),
            schemaResourceName,
            () ->
                IndexSchemaFactory.getParsedSchema(
                    in, zkLoader, core.getLatestSchema().getResourceName()),
            true,
            schemaResourceName,
            version,
            core.getLatestSchema().getSchemaUpdateLock());
      } else {
        return (ManagedIndexSchema) core.getLatestSchema();
      }
    } else {
      return (ManagedIndexSchema) core.getLatestSchema();
    }
  }
}
