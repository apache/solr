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
package org.apache.solr.search.grouping.distributed.shardresultserializer;

import static org.apache.solr.common.params.CommonParams.ID;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.grouping.Command;
import org.apache.solr.search.grouping.distributed.command.QueryCommand;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;
import org.apache.solr.search.grouping.distributed.command.TopGroupsFieldCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation for transforming {@link TopGroups} and {@link TopDocs} into a {@link NamedList}
 * structure and vice versa.
 */
public class TopGroupsResultTransformer
    implements ShardResultTransformer<List<Command<?>>, Map<String, ?>> {

  private final ResponseBuilder rb;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public TopGroupsResultTransformer(ResponseBuilder rb) {
    this.rb = rb;
  }

  @Override
  public NamedList<NamedList<Object>> transform(List<Command<?>> data) throws IOException {
    NamedList<NamedList<Object>> result = new NamedList<>();
    final IndexSchema schema = rb.req.getSearcher().getSchema();
    for (Command<?> command : data) {
      NamedList<Object> commandResult;
      if (command instanceof TopGroupsFieldCommand fieldCommand) {
        SchemaField groupField = schema.getField(fieldCommand.getKey());
        commandResult = serializeTopGroups(fieldCommand.result(), groupField);
      } else if (command instanceof QueryCommand queryCommand) {
        commandResult = serializeTopDocs(queryCommand.result());
      } else {
        commandResult = null;
      }

      result.add(command.getKey(), commandResult);
    }
    return result;
  }

  @Override
  public Map<String, ?> transformToNative(
      NamedList<NamedList<?>> shardResponse, Sort groupSort, Sort withinGroupSort, String shard) {
    Map<String, Object> result = new HashMap<>();

    final IndexSchema schema = rb.req.getSearcher().getSchema();

    for (Map.Entry<String, NamedList<?>> entry : shardResponse) {
      String key = entry.getKey();
      NamedList<?> commandResult = entry.getValue();
      Integer totalGroupedHitCount = (Integer) commandResult.get("totalGroupedHitCount");
      Number totalHits = (Number) commandResult.get("totalHits"); // previously Integer now Long
      if (totalHits != null) {
        Integer matches = (Integer) commandResult.get("matches");
        Float maxScore = (Float) commandResult.get("maxScore");
        if (maxScore == null) {
          maxScore = Float.NaN;
        }

        @SuppressWarnings("unchecked")
        List<NamedList<Object>> documents =
            (List<NamedList<Object>>) commandResult.get("documents");
        ScoreDoc[] scoreDocs = transformToNativeShardDoc(documents, groupSort, shard, schema);
        final TopDocs topDocs;
        if (withinGroupSort.equals(Sort.RELEVANCE)) {
          topDocs =
              new TopDocs(
                  new TotalHits(totalHits.longValue(), TotalHits.Relation.EQUAL_TO), scoreDocs);
        } else {
          topDocs =
              new TopFieldDocs(
                  new TotalHits(totalHits.longValue(), TotalHits.Relation.EQUAL_TO),
                  scoreDocs,
                  withinGroupSort.getSort());
        }
        result.put(key, new QueryCommandResult(topDocs, matches, maxScore));
        continue;
      }

      Integer totalHitCount = (Integer) commandResult.get("totalHitCount");

      List<GroupDocs<BytesRef>> groupDocs = new ArrayList<>();

      // Skip first two entries (totalGroupedHitCount and totalHitCount) and process the rest
      int skipCount = 0;
      for (Entry<String, ?> groupEntry : commandResult) {
        if (skipCount++ < 2) {
          continue;
        }

        String groupValue = groupEntry.getKey();
        @SuppressWarnings("unchecked")
        NamedList<Object> groupResult = (NamedList<Object>) groupEntry.getValue();
        // previously Integer now Long
        Number totalGroupHits = (Number) groupResult.get("totalHits");
        Float maxScore = (Float) groupResult.get("maxScore");
        if (maxScore == null) {
          maxScore = Float.NaN;
        }

        @SuppressWarnings("unchecked")
        List<NamedList<Object>> documents = (List<NamedList<Object>>) groupResult.get("documents");
        ScoreDoc[] scoreDocs = transformToNativeShardDoc(documents, withinGroupSort, shard, schema);

        BytesRef groupValueRef = groupValue != null ? new BytesRef(groupValue) : null;
        groupDocs.add(
            new GroupDocs<>(
                Float.NaN,
                maxScore,
                new TotalHits(totalGroupHits.longValue(), TotalHits.Relation.EQUAL_TO),
                scoreDocs,
                groupValueRef,
                null));
      }

      GroupDocs<BytesRef>[] groupDocsArr = groupDocs.toArray(newGroupDocsArray(groupDocs.size()));
      TopGroups<BytesRef> topGroups =
          new TopGroups<>(
              groupSort.getSort(),
              withinGroupSort.getSort(),
              totalHitCount,
              totalGroupedHitCount,
              groupDocsArr,
              Float.NaN);

      result.put(key, topGroups);
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  private static GroupDocs<BytesRef>[] newGroupDocsArray(int size) {
    return (GroupDocs<BytesRef>[]) Array.newInstance(GroupDocs.class, size);
  }

  protected ScoreDoc[] transformToNativeShardDoc(
      List<NamedList<Object>> documents, Sort groupSort, String shard, IndexSchema schema) {
    ScoreDoc[] scoreDocs = new ScoreDoc[documents.size()];
    int j = 0;
    for (NamedList<Object> document : documents) {
      Object docId = document.get(ID);
      if (docId != null) {
        docId = docId.toString();
      } else {
        log.error("doc {} has null 'id'", document);
      }
      Float score = (Float) document.get("score");
      if (score == null) {
        score = Float.NaN;
      }
      Object[] sortValues = null;
      Object sortValuesVal = document.get("sortValues");
      if (sortValuesVal != null) {
        sortValues = ((List<?>) sortValuesVal).toArray();
        for (int k = 0; k < sortValues.length; k++) {
          SchemaField field =
              groupSort.getSort()[k].getField() != null
                  ? schema.getFieldOrNull(groupSort.getSort()[k].getField())
                  : null;
          sortValues[k] = ShardResultTransformerUtils.unmarshalSortValue(sortValues[k], field);
        }
      } else {
        log.debug("doc {} has null 'sortValues'", document);
      }
      scoreDocs[j++] = new ShardDoc(score, sortValues, docId, shard);
    }
    return scoreDocs;
  }

  protected NamedList<Object> serializeTopGroups(TopGroups<BytesRef> data, SchemaField groupField)
      throws IOException {
    NamedList<Object> result = new NamedList<>();
    result.add("totalGroupedHitCount", data.totalGroupedHitCount);
    result.add("totalHitCount", data.totalHitCount);
    if (data.totalGroupCount != null) {
      result.add("totalGroupCount", data.totalGroupCount);
    }

    final IndexSchema schema = rb.req.getSearcher().getSchema();
    SchemaField uniqueField = schema.getUniqueKeyField();
    for (GroupDocs<BytesRef> searchGroup : data.groups) {
      NamedList<Object> groupResult = new NamedList<>();
      assert searchGroup.totalHits.relation == TotalHits.Relation.EQUAL_TO;
      groupResult.add("totalHits", searchGroup.totalHits.value);
      if (!Float.isNaN(searchGroup.maxScore)) {
        groupResult.add("maxScore", searchGroup.maxScore);
      }

      SolrDocumentFetcher docFetcher = rb.req.getSearcher().getDocFetcher();
      List<NamedList<Object>> documents = new ArrayList<>();
      for (int i = 0; i < searchGroup.scoreDocs.length; i++) {
        NamedList<Object> document = new NamedList<>();
        documents.add(document);

        Document doc = retrieveDocument(uniqueField, searchGroup.scoreDocs[i].doc, docFetcher);
        document.add(ID, uniqueField.getType().toExternal(doc.getField(uniqueField.getName())));
        if (!Float.isNaN(searchGroup.scoreDocs[i].score)) {
          document.add("score", searchGroup.scoreDocs[i].score);
        }
        if (!(searchGroup.scoreDocs[i] instanceof FieldDoc fieldDoc)) {
          continue; // thus don't add sortValues below
        }

        Object[] convertedSortValues = new Object[fieldDoc.fields.length];
        for (int j = 0; j < fieldDoc.fields.length; j++) {
          Object sortValue = fieldDoc.fields[j];
          Sort withinGroupSort = rb.getGroupingSpec().getWithinGroupSortSpec().getSort();
          SchemaField field =
              withinGroupSort.getSort()[j].getField() != null
                  ? schema.getFieldOrNull(withinGroupSort.getSort()[j].getField())
                  : null;
          if (field != null) {
            FieldType fieldType = field.getType();
            if (sortValue != null) {
              sortValue = fieldType.marshalSortValue(sortValue);
            }
          }
          convertedSortValues[j] = sortValue;
        }
        document.add("sortValues", convertedSortValues);
      }
      groupResult.add("documents", documents);
      String groupValue =
          searchGroup.groupValue != null
              ? groupField
                  .getType()
                  .indexedToReadable(searchGroup.groupValue, new CharsRefBuilder())
                  .toString()
              : null;
      result.add(groupValue, groupResult);
    }

    return result;
  }

  protected NamedList<Object> serializeTopDocs(QueryCommandResult result) throws IOException {
    NamedList<Object> queryResult = new NamedList<>();
    queryResult.add("matches", result.getMatches());
    TopDocs topDocs = result.getTopDocs();
    assert topDocs.totalHits.relation == TotalHits.Relation.EQUAL_TO;
    queryResult.add("totalHits", topDocs.totalHits.value);
    // debug: assert !Float.isNaN(result.getTopDocs().getMaxScore()) ==
    // rb.getGroupingSpec().isNeedScore();
    if (!Float.isNaN(result.getMaxScore())) {
      queryResult.add("maxScore", result.getMaxScore());
    }
    List<NamedList<?>> documents = new ArrayList<>();
    queryResult.add("documents", documents);

    SolrDocumentFetcher docFetcher = rb.req.getSearcher().getDocFetcher();
    final IndexSchema schema = rb.req.getSearcher().getSchema();
    SchemaField uniqueField = schema.getUniqueKeyField();
    for (ScoreDoc scoreDoc : result.getTopDocs().scoreDocs) {
      NamedList<Object> document = new NamedList<>();
      documents.add(document);

      Document doc = retrieveDocument(uniqueField, scoreDoc.doc, docFetcher);
      document.add(ID, uniqueField.getType().toExternal(doc.getField(uniqueField.getName())));
      if (!Float.isNaN(scoreDoc.score)) {
        document.add("score", scoreDoc.score);
      }
      if (!(scoreDoc instanceof FieldDoc fieldDoc)) {
        continue; // thus don't add sortValues below
      }

      Object[] convertedSortValues = new Object[fieldDoc.fields.length];
      for (int j = 0; j < fieldDoc.fields.length; j++) {
        Object sortValue = fieldDoc.fields[j];
        Sort groupSort = rb.getGroupingSpec().getGroupSortSpec().getSort();
        SchemaField field =
            groupSort.getSort()[j].getField() != null
                ? schema.getFieldOrNull(groupSort.getSort()[j].getField())
                : null;
        convertedSortValues[j] = ShardResultTransformerUtils.marshalSortValue(sortValue, field);
      }
      document.add("sortValues", convertedSortValues);
    }

    return queryResult;
  }

  private Document retrieveDocument(
      final SchemaField uniqueField, int doc, SolrDocumentFetcher docFetcher) throws IOException {
    return docFetcher.doc(doc, Collections.singleton(uniqueField.getName()));
  }
}
