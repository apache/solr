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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.valuesource.FieldCacheSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.SortField;

import java.io.IOException;
import java.util.Map;

/**
 * This class exists to delegate {@link #getSortField(boolean)} to
 * {@link FieldType#getSortField(SchemaField, boolean)}. It is used to wrap Lucene numeric FieldSources
 * (e.g., {@link IntFieldSource}) to apply "missing" values.
 */
class SortDelegatingValueSource extends FieldCacheSource {

    private final FieldCacheSource backing;
    private final FieldType sortFieldProvider;
    private final SchemaField sf;

    public SortDelegatingValueSource(SchemaField sf, FieldType sortFieldProvider, FieldCacheSource backing) {
        super(sf.getName());
        this.backing = backing;
        this.sortFieldProvider = sortFieldProvider;
        this.sf = sf;
    }

    @Override
    public String description() {
        return backing.description();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SortDelegatingValueSource)) return false;
        SortDelegatingValueSource other = (SortDelegatingValueSource) o;
        return this.backing.equals(other.backing);
    }

    @Override
    public int hashCode() {
        return SortDelegatingValueSource.class.hashCode() ^ backing.hashCode();
    }

    @Override
    public String toString() {
        return backing.toString();
    }

    @Override
    public void createWeight(Map<Object, Object> context, IndexSearcher searcher) throws IOException {
        backing.createWeight(context, searcher);
    }

    @Override
    public LongValuesSource asLongValuesSource() {
        return backing.asLongValuesSource();
    }

    @Override
    public DoubleValuesSource asDoubleValuesSource() {
        return backing.asDoubleValuesSource();
    }

    @Override
    public SortField getSortField(boolean reverse) {
        return sortFieldProvider.getSortField(sf, reverse);
    }

    @Override
    public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext) throws IOException {
        return backing.getValues(context, readerContext);
    }
}
