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
import static org.apache.solr.client.api.model.SchemaChangeOperation.OPERATION_TYPE_PROP;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.api.model.SchemaChangeOperation;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
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

  /**
   * Take in a set of schema commands and execute them. It tries to capture as many errors as
   * possible instead of failing at the first error it encounters
   *
   * @return List of errors. If the List is empty then the operation was successful.
   */
  public List<Exception> performOperations(List<SchemaChangeOperation> ops) throws Exception {
    IndexSchema schema = req.getCore().getLatestSchema();
    if (schema instanceof ManagedIndexSchema && schema.isMutable()) {
      return doOperations(ops);
    } else {
      return singletonList(
          new SolrException(SolrException.ErrorCode.BAD_REQUEST, "schema is not editable"));
    }
  }

  private static void executeAndRecordErrors(List<Exception> errorRecord, Runnable toRun) {
    try {
      toRun.run();
    } catch (Exception e) {
      errorRecord.add(e);
    }
  }

  private List<Exception> doOperations(List<SchemaChangeOperation> operations)
      throws InterruptedException, IOException, KeeperException {
    TimeOut timeOut = new TimeOut(updateTimeOut, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    SolrCore core = req.getCore();
    String errorMsg = "Unable to persist managed schema. ";
    int latestVersion = -1;

    final List<Exception> exceptions = new ArrayList<>();
    synchronized (req.getSchema().getSchemaUpdateLock()) {
      while (!timeOut.hasTimedOut()) {
        managedIndexSchema = getFreshManagedSchema(req.getCore());
        for (SchemaChangeOperation op : operations) {
          OpType opType = OpType.get(op.operationType);
          if (opType != null) {
            executeAndRecordErrors(
                exceptions,
                () -> {
                  opType.perform(op, this);
                });
          } else {
            exceptions.add(
                new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST, "No such operation: " + op.operationType));
          }
        }
        if (!exceptions.isEmpty()) break;
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
            exceptions.add(e);
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
    if (exceptions.isEmpty() && timeOut.hasTimedOut()) {
      log.warn("{} Timed out", errorMsg);
      exceptions.add(
          new SolrException(SolrException.ErrorCode.SERVER_ERROR, errorMsg + "Timed out."));
    }
    return exceptions;
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

  public enum OpType {
    ADD_FIELD_TYPE("add-field-type") {
      @Override
      public boolean perform(SchemaChangeOperation op, SchemaManager mgr) {
        final var addFieldTypeOp = (SchemaChangeOperation.AddFieldType) op;
        String name = addFieldTypeOp.name;
        String className = addFieldTypeOp.className;
        try {
          FieldType fieldType =
              mgr.managedIndexSchema.newFieldType(name, className, convertToMap(addFieldTypeOp));
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.addFieldTypes(singletonList(fieldType), false);
          return true;
        } catch (Exception e) {
          log.error("Could not add field type.", e);
          if (e instanceof SolrException) {
            throw e;
          } else {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR, "Could not add field type.", e);
          }
        }
      }
    },
    ADD_COPY_FIELD("add-copy-field") {
      @Override
      public boolean perform(SchemaChangeOperation op, SchemaManager mgr) {
        final var addCopyFieldOp = (SchemaChangeOperation.AddCopyField) op;
        String src = addCopyFieldOp.source;
        List<String> dests = addCopyFieldOp.destinations;

        // If maxChars is not specified, there is no limit on copied chars
        final var maxChars =
            addCopyFieldOp.maxChars == null ? CopyField.UNLIMITED : addCopyFieldOp.maxChars;
        if (maxChars < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              MAX_CHARS + " " + maxChars + " cannot be negative.");
        }

        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.addCopyFields(src, dests, maxChars);
          return true;
        } catch (Exception e) {
          log.error("Could not add copy field", e);
          throw e;
        }
      }
    },
    ADD_FIELD("add-field") {
      @Override
      public boolean perform(SchemaChangeOperation op, SchemaManager mgr) {
        final var addFieldOp = (SchemaChangeOperation.AddField) op;
        String name = addFieldOp.name;
        String type = addFieldOp.type;
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
          throw e;
        }
      }
    },
    ADD_DYNAMIC_FIELD("add-dynamic-field") {
      @Override
      public boolean perform(SchemaChangeOperation op, SchemaManager mgr) {
        final var addDynFieldOp = (SchemaChangeOperation.AddDynamicField) op;
        String name = addDynFieldOp.name;
        String type = addDynFieldOp.type;
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
          throw e;
        }
      }
    },
    DELETE_FIELD_TYPE("delete-field-type") {
      @Override
      public boolean perform(SchemaChangeOperation op, SchemaManager mgr) {
        final var deleteFieldTypeOp = (SchemaChangeOperation.DeleteFieldType) op;
        String name = deleteFieldTypeOp.name;
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.deleteFieldTypes(singleton(name));
          return true;
        } catch (Exception e) {
          log.error("Could not delete field type", e);
          throw e;
        }
      }
    },
    DELETE_COPY_FIELD("delete-copy-field") {
      @Override
      public boolean perform(SchemaChangeOperation op, SchemaManager mgr) {
        final var deleteCopyField = (SchemaChangeOperation.DeleteCopyField) op;
        String source = deleteCopyField.source;
        List<String> dests = deleteCopyField.destinations;
        try {
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.deleteCopyFields(singletonMap(source, dests));
          return true;
        } catch (Exception e) {
          log.error("Could not delete copy field", e);
          throw e;
        }
      }
    },
    DELETE_FIELD("delete-field") {
      @Override
      public boolean perform(SchemaChangeOperation op, SchemaManager mgr) {
        final var deleteFieldOp = (SchemaChangeOperation.DeleteField) op;
        String name = deleteFieldOp.name;
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.deleteFields(singleton(name));
          return true;
        } catch (Exception e) {
          log.error("Could not delete field", e);
          throw e;
        }
      }
    },
    DELETE_DYNAMIC_FIELD("delete-dynamic-field") {
      @Override
      public boolean perform(SchemaChangeOperation op, SchemaManager mgr) {
        final var deleteDynFieldOp = (SchemaChangeOperation.DeleteDynamicField) op;
        String name = deleteDynFieldOp.name;
        try {
          mgr.managedIndexSchema = mgr.managedIndexSchema.deleteDynamicFields(singleton(name));
          return true;
        } catch (Exception e) {
          log.error("Could not delete dynamic field", e);
          throw e;
        }
      }
    },
    REPLACE_FIELD_TYPE("replace-field-type") {
      @Override
      public boolean perform(SchemaChangeOperation op, SchemaManager mgr) {
        final var replaceFieldTypeOp = (SchemaChangeOperation.ReplaceFieldType) op;
        String name = replaceFieldTypeOp.name;
        String className = replaceFieldTypeOp.className;
        try {
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.replaceFieldType(
                  name, className, convertToMap(replaceFieldTypeOp));
          return true;
        } catch (Exception e) {
          log.error("Could not replace field type", e);
          throw e;
        }
      }
    },
    REPLACE_FIELD("replace-field") {
      @Override
      public boolean perform(SchemaChangeOperation op, SchemaManager mgr) {
        final var replaceFieldOp = (SchemaChangeOperation.ReplaceField) op;
        String name = replaceFieldOp.name;
        String type = replaceFieldOp.type;
        FieldType ft = mgr.managedIndexSchema.getFieldTypeByName(type);
        if (ft == null) {
          throw new SolrException(
              SolrException.ErrorCode.NOT_FOUND, "No such field type '" + type + "'");
        }
        try {
          final String[] propsToOmit = new String[] {OPERATION_TYPE_PROP, NAME, TYPE};
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.replaceField(
                  name, ft, convertToMapExcluding(replaceFieldOp, propsToOmit));
          return true;
        } catch (Exception e) {
          log.error("Could not replace field", e);
          throw e;
        }
      }
    },
    REPLACE_DYNAMIC_FIELD("replace-dynamic-field") {
      @Override
      public boolean perform(SchemaChangeOperation op, SchemaManager mgr) {
        final var replaceDynFieldOp = (SchemaChangeOperation.ReplaceDynamicField) op;
        String name = replaceDynFieldOp.name;
        String type = replaceDynFieldOp.type;
        FieldType ft = mgr.managedIndexSchema.getFieldTypeByName(type);
        if (ft == null) {
          throw new SolrException(
              SolrException.ErrorCode.NOT_FOUND, "No such field type '" + type + "'");
        }
        try {
          final String[] propsToOmit = new String[] {OPERATION_TYPE_PROP, NAME, TYPE};
          mgr.managedIndexSchema =
              mgr.managedIndexSchema.replaceDynamicField(
                  name, ft, convertToMapExcluding(replaceDynFieldOp, propsToOmit));
          return true;
        } catch (Exception e) {
          log.error("Could not replace dynamic field", e);
          throw e;
        }
      }
    };

    public abstract boolean perform(SchemaChangeOperation op, SchemaManager mgr);

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
