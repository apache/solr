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
package org.apache.solr.handler.sql;

import static org.apache.solr.handler.sql.SolrAggregate.solrAggMetricId;

import java.util.*;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.stream.InnerJoinStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;

/** Relational expression that uses Solr calling convention. */
interface SolrRel extends RelNode {
  Implementor implement(Implementor implementor);

  /** Calling convention for relational operations that occur in Solr. */
  Convention CONVENTION = new Convention.Impl("Solr", SolrRel.class);

  /**
   * Callback for the implementation process that converts a tree of {@link SolrRel} nodes into a
   * Solr query.
   */
  class Implementor {

    public Implementor(
        EnumerableRelImplementor _enumerableRelImplementor,
        EnumerableRel.Prefer _pref,
        RelDataType _rowType,
        PhysType _physType) {
      enumerableRelImplementor = _enumerableRelImplementor;
      pref = _pref;
      rowType = _rowType;
      physType = _physType;
    }

    final RelDataType rowType;
    final EnumerableRel.Prefer pref;
    final EnumerableRelImplementor enumerableRelImplementor;
    final Map<String, String> fieldMappings = new HashMap<>();
    final Map<String, String> reverseAggMappings = new HashMap<>();
    final PhysType physType;
    SolrTableScan solrTableScan;
    String query = null;
    String havingPredicate;
    boolean negativeQuery;
    String limitValue = null;
    String offsetValue = null;
    final List<Pair<String, String>> orders = new ArrayList<>();
    final List<String> buckets = new ArrayList<>();
    final List<Pair<String, String>> metricPairs = new ArrayList<>();

    RelOptTable table;
    SolrTable solrTable;

    void addFieldMapping(String key, String val, boolean overwrite) {
      if (key != null) {
        if (overwrite || !fieldMappings.containsKey(key)) {
          this.fieldMappings.put(key, val);
        }
      }
    }

    void addReverseAggMapping(String key, String val) {
      if (key != null && !reverseAggMappings.containsKey(key)) {
        this.reverseAggMappings.put(key, val);
      }
    }

    void addQuery(String query) {
      this.query = query;
    }

    void setNegativeQuery(boolean negativeQuery) {
      this.negativeQuery = negativeQuery;
    }

    void addOrder(String column, String direction) {
      column = this.fieldMappings.getOrDefault(column, column);
      this.orders.add(new Pair<>(column, direction));
    }

    void addBucket(String bucket) {
      bucket = this.fieldMappings.getOrDefault(bucket, bucket);
      this.buckets.add(bucket);
    }

    void addMetricPair(String outName, String metric, String column) {
      column = this.fieldMappings.getOrDefault(column, column);
      this.metricPairs.add(new Pair<>(metric, column));

      if (outName != null) {
        this.addFieldMapping(outName, solrAggMetricId(metric, column), true);
      }
    }

    void setHavingPredicate(String havingPredicate) {
      this.havingPredicate = havingPredicate;
    }

    void setLimit(String limit) {
      limitValue = limit;
    }

    void setOffset(String offset) {
      this.offsetValue = offset;
    }

    Implementor visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      return ((SolrRel) input).implement(this);
    }

    TupleStream getPhysicalPlan() {
      final List<Map.Entry<String, Class<?>>> fields =
          zip(generateFields(SolrRules.solrFieldNames(rowType), fieldMappings), physType);

      return solrTableScan.getPhysicalPlan(
          fields,
          query,
          orders,
          buckets,
          metricPairs,
          limitValue,
          negativeQuery,
          havingPredicate,
          offsetValue);
    }

    private List<Map.Entry<String, Class<?>>> zip(List<String> fields, PhysType physType) {
      List<Map.Entry<String, Class<?>>> zipped = new ArrayList<>();
      for (int i = 0; i < fields.size(); i++) {
        Map.Entry<String, Class<?>> entry =
            new AbstractMap.SimpleEntry<String, Class<?>>(fields.get(i), physType.fieldClass(i));
        zipped.add(entry);
      }

      return zipped;
    }

    private List<String> generateFields(List<String> queryFields, Map<String, String> fieldMappings) {

      if (fieldMappings.isEmpty()) {
        return queryFields;
      } else {
        List<String> fields = new ArrayList<>();
        for (String field : queryFields) {
          fields.add(getField(fieldMappings, field));
        }
        return fields;
      }
    }


    private String getField(Map<String, String> fieldMappings, String field) {
      String retField = field;
      while (fieldMappings.containsKey(field)) {
        field = fieldMappings.getOrDefault(field, retField);
        if (retField.equals(field)) {
          break;
        } else {
          retField = field;
        }
      }
      return retField;
    }
  }

  /*
  *  The JoinImplementor would be called by the SolrLogicalJoinRule
  */
  class JoinImplementor extends Implementor {

    private Implementor left;
    private Implementor right;
    private StreamEqualitor streamEqualitor;

    JoinImplementor(EnumerableRelImplementor _enumerableRelImplementor, EnumerableRel.Prefer _pref, RelDataType _rowType, PhysType _physType) {
      super( _enumerableRelImplementor, _pref,  _rowType, _physType);
    }

    public void setLeft(Implementor left) {
      this.left = left;
    }

    public void setRight(Implementor right) {
      this.right = right;
    }

    public void setStreamEqualitor(StreamEqualitor streamEqualitor) {
      this.streamEqualitor = streamEqualitor;
    }

    TupleStream getPhysicalPlan() {
      try {
        TupleStream leftPlan = left.getPhysicalPlan();
        TupleStream righPlain = right.getPhysicalPlan();
        return new InnerJoinStream(leftPlan, righPlain, streamEqualitor);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

}
