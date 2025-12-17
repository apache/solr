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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;

/**
 * Adds fields to nested documents to support some nested search requirements. It can even generate
 * uniqueKey fields for nested docs.
 *
 * @see IndexSchema#NEST_PARENT_FIELD_NAME
 * @see IndexSchema#NEST_PATH_FIELD_NAME
 * @since 7.5.0
 */
public class NestedUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    boolean storeParent = shouldStoreDocParent(req.getSchema());
    boolean storePath = shouldStoreDocPath(req.getSchema());
    if (!(storeParent || storePath)) {
      return next;
    }
    return new NestedUpdateProcessor(req, storeParent, storePath, next);
  }

  private static boolean shouldStoreDocParent(IndexSchema schema) {
    return schema.getFields().containsKey(IndexSchema.NEST_PARENT_FIELD_NAME);
  }

  private static boolean shouldStoreDocPath(IndexSchema schema) {
    return schema.getFields().containsKey(IndexSchema.NEST_PATH_FIELD_NAME);
  }

  private static class NestedUpdateProcessor extends UpdateRequestProcessor {
    private static final String PATH_SEP_CHAR = "/";
    private static final String NUM_SEP_CHAR = "#";
    private static final String SINGULAR_VALUE_CHAR = "";
    private boolean storePath;
    private boolean storeParent;
    private String uniqueKeyFieldName;
    private IndexSchema schema;

    NestedUpdateProcessor(
        SolrQueryRequest req, boolean storeParent, boolean storePath, UpdateRequestProcessor next) {
      super(next);
      this.storeParent = storeParent;
      this.storePath = storePath;
      this.uniqueKeyFieldName = req.getSchema().getUniqueKeyField().getName();
      this.schema = req.getSchema();
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      SolrInputDocument doc = cmd.getSolrInputDocument();
      processDocChildren(doc, null);
      super.processAdd(cmd);
    }

    private boolean processDocChildren(SolrInputDocument doc, String fullPath) {
      boolean isNested = false;
      List<String> originalVectorFieldsToRemove = new ArrayList<>();
      ArrayList<SolrInputDocument> vectors = new ArrayList<>();
      for (SolrInputField field : doc.values()) {
        SchemaField sfield = schema.getFieldOrNull(field.getName());
        int childNum = 0;
        boolean isSingleVal = !(field.getValue() instanceof Collection);
        boolean firstLevelChildren = fullPath == null;
        if (firstLevelChildren && sfield != null && isMultiValuedVectorField(sfield)) {
          for (Object vectorValue : field.getValues()) {
            SolrInputDocument singleVectorNestedDoc = new SolrInputDocument();
            singleVectorNestedDoc.setField(field.getName(), vectorValue);
            final String sChildNum = isSingleVal ? SINGULAR_VALUE_CHAR : String.valueOf(childNum);
            String parentDocId = doc.getField(uniqueKeyFieldName).getFirstValue().toString();
            singleVectorNestedDoc.setField(
                uniqueKeyFieldName, generateChildUniqueId(parentDocId, field.getName(), sChildNum));

            if (!isNested) {
              isNested = true;
            }
            final String lastKeyPath = PATH_SEP_CHAR + field.getName() + NUM_SEP_CHAR + sChildNum;
            final String childDocPath = firstLevelChildren ? lastKeyPath : fullPath + lastKeyPath;
            if (storePath) {
              setPathField(singleVectorNestedDoc, childDocPath);
            }
            if (storeParent) {
              setParentKey(singleVectorNestedDoc, doc);
            }
            ++childNum;
            vectors.add(singleVectorNestedDoc);
          }
          originalVectorFieldsToRemove.add(field.getName());
        } else {
          for (Object val : field) {
            if (!(val instanceof SolrInputDocument cDoc)) {
              // either all collection items are child docs or none are.
              break;
            }
            final String fieldName = field.getName();

            if (fieldName.contains(PATH_SEP_CHAR)) {
              throw new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST,
                  "Field name: '"
                      + fieldName
                      + "' contains: '"
                      + PATH_SEP_CHAR
                      + "' , which is reserved for the nested URP");
            }
            final String sChildNum = isSingleVal ? SINGULAR_VALUE_CHAR : String.valueOf(childNum);
            if (!cDoc.containsKey(uniqueKeyFieldName)) {
              String parentDocId = doc.getField(uniqueKeyFieldName).getFirstValue().toString();
              cDoc.setField(
                  uniqueKeyFieldName, generateChildUniqueId(parentDocId, fieldName, sChildNum));
            }
            if (!isNested) {
              isNested = true;
            }
            final String lastKeyPath = PATH_SEP_CHAR + fieldName + NUM_SEP_CHAR + sChildNum;
            // concat of all paths children.grandChild => /children#1/grandChild#
            final String childDocPath = firstLevelChildren ? lastKeyPath : fullPath + lastKeyPath;
            processChildDoc(cDoc, doc, childDocPath);
            ++childNum;
          }
        }
      }
      this.cleanOriginalVectorFields(doc, originalVectorFieldsToRemove);
      if (vectors.size() > 0) {
        doc.setField("vectors", vectors);
      }
      return isNested;
    }

    private void cleanOriginalVectorFields(
        SolrInputDocument doc, List<String> originalVectorFieldsToRemove) {
      for (String fieldName : originalVectorFieldsToRemove) {
        doc.removeField(fieldName);
      }
    }

    private static boolean isMultiValuedVectorField(SchemaField sfield) {
      return sfield.getType() instanceof DenseVectorField && sfield.multiValued();
    }

    private void processChildDoc(
        SolrInputDocument child, SolrInputDocument parent, String fullPath) {
      if (storePath) {
        setPathField(child, fullPath);
      }
      if (storeParent) {
        setParentKey(child, parent);
      }
      processDocChildren(child, fullPath);
    }

    private String generateChildUniqueId(String parentId, String childKey, String childNum) {
      // combines parentId with the child's key and childNum. e.g. "10/footnote#1"
      return parentId + PATH_SEP_CHAR + childKey + NUM_SEP_CHAR + childNum;
    }

    private void setParentKey(SolrInputDocument child, SolrInputDocument parent) {
      child.setField(IndexSchema.NEST_PARENT_FIELD_NAME, parent.getFieldValue(uniqueKeyFieldName));
    }

    private void setPathField(SolrInputDocument child, String fullPath) {
      child.setField(IndexSchema.NEST_PATH_FIELD_NAME, fullPath);
    }
  }
}
