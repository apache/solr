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


import java.io.IOException;
import java.util.*;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.solr.client.solrj.io.stream.*;

/** Table based on a Solr collection */
class SolrTable extends AbstractQueryableTable implements TranslatableTable {

  private final String collection;
  private final SolrSchema schema;
  private RelProtoDataType protoRowType;

  SolrTable(SolrSchema schema, String collection) {
    super(Object[].class);
    this.schema = schema;
    this.collection = collection;
  }

  public SolrSchema getSchema() {
    return this.schema;
  }

  public String getCollection() {
    return this.collection;
  }

  public String toString() {
    return "SolrTable {" + collection + "}";
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType == null) {
      protoRowType = schema.getRelDataType(collection);
    }
    return protoRowType.apply(typeFactory);
  }

  private Enumerable<Object> query(final Properties properties) {
    return query(
        properties,
        Collections.emptyList(),
        null,
        null);
  }

  /**
   * Executes a Solr query on the underlying table.
   *
   * @param properties Connections properties
   * @param fields List of fields to project
   * @param query A string for the query
   * @return Enumerator of results
   */
  private Enumerable<Object> query(
      final Properties properties,
      final List<Map.Entry<String, Class<?>>> fields,
      final String query,
      final String physicalPlan) {

    try {
      TupleStream tupleStream = SolrTableScan.streamFactory.constructStream(physicalPlan);
      StreamContext streamContext = new StreamContext();
      streamContext.setSolrClientCache(schema.getSolrClientCache());
      tupleStream.setStreamContext(streamContext);
      final TupleStream finalStream = tupleStream;
      return new AbstractEnumerable<Object>() {
        // Use original fields list to make sure only the fields specified are enumerated
        public Enumerator<Object> enumerator() {
          return new SolrEnumerator(finalStream, fields);
        }
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public <T> Queryable<T> asQueryable(
      QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new SolrQueryable<>(queryProvider, schema, this, tableName);
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new SolrTableScan(
        cluster, cluster.traitSetOf(SolrRel.CONVENTION), relOptTable, this, null);
  }

  @SuppressWarnings("WeakerAccess")
  public static class SolrQueryable<T> extends AbstractTableQueryable<T> {
    SolrQueryable(
        QueryProvider queryProvider, SchemaPlus schema, SolrTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      @SuppressWarnings("unchecked")
      final Enumerable<T> enumerable = (Enumerable<T>) getTable().query(getProperties());
      return enumerable.enumerator();
    }

    private SolrTable getTable() {
      return (SolrTable) table;
    }

    private Properties getProperties() {
      return schema.unwrap(SolrSchema.class).properties;
    }

    /**
     * Called via code-generation.
     *
     * @see SolrMethod#SOLR_QUERYABLE_QUERY
     */
    @SuppressWarnings({"UnusedDeclaration"})
    public Enumerable<Object> query(
        List<Map.Entry<String, Class<?>>> fields,
        String query,
        String physicalPlan) {
      return getTable()
          .query(
              getProperties(),
              fields,
              query,
              physicalPlan);
    }
  }
}
