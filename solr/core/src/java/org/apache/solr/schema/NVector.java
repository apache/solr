
//Copyright (c) 2021, Dan Rosher
//    All rights reserved.
//
//    This source code is licensed under the BSD-style license found in the
//    LICENSE file in the root directory of this source tree.
package org.apache.solr.schema;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.MultiValueSource;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader;
import org.apache.solr.util.NVectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NVector extends CoordinateFieldType {

    @Override
    protected void init(IndexSchema schema, Map<String, String> args) {
        super.init(schema, args);
        dimension = 3;
        createSuffixCache(3);
    }

    @Override
    public List<IndexableField> createFields(SchemaField field, Object value) {
        String externalVal = value.toString();
        String[] point = parseCommaSeparatedList(externalVal, dimension);
        String[] nvector = NVectorUtil.latLongToNVector(point);

        List<IndexableField> f = new ArrayList<>((dimension * 2) + 1);

        if (field.indexed()) {
            for (int i = 0; i < dimension; i++) {
                SchemaField sf = subField(field, i, schema);
                f.addAll(sf.createFields(nvector[i]));
            }
        }

        if (field.stored()) {
            f.add(createField(field.getName(), externalVal, StoredField.TYPE));
        }
        return f;
    }

    @Override
    public ValueSource getValueSource(SchemaField field, QParser parser) {
        ArrayList<ValueSource> vs = new ArrayList<>(dimension);
        for (int i = 0; i < dimension; i++) {
            SchemaField sub = subField(field, i, schema);
            vs.add(sub.getType()
                .getValueSource(sub, parser));
        }
        return new NVectorValueSource(vs);
    }


    /**
     * Given a string containing <i>dimension</i> values encoded in it, separated by commas,
     * return a String array of length <i>dimension</i> containing the values.
     *
     * @param externalVal The value to parse
     * @param dimension   The expected number of values for the point
     * @return An array of the values that make up the point (aka vector)
     * @throws SolrException if the dimension specified does not match the number found
     */
    public static String[] parseCommaSeparatedList(String externalVal, int dimension) throws SolrException {
        //TODO: Should we support sparse vectors?
        String[] out = new String[dimension];
        int idx = externalVal.indexOf(',');
        int end = idx;
        int start = 0;
        int i = 0;
        if (idx == -1 && dimension == 1 && externalVal.length() > 0) {//we have a single point, dimension better be 1
            out[0] = externalVal.trim();
            i = 1;
        } else if (idx > 0) {//if it is zero, that is an error
            //Parse out a comma separated list of values, as in: 73.5,89.2,7773.4
            for (; i < dimension; i++) {
                while (start < end && externalVal.charAt(start) == ' ') start++;
                while (end > start && externalVal.charAt(end - 1) == ' ') end--;
                if (start == end) {
                    break;
                }
                out[i] = externalVal.substring(start, end);
                start = idx + 1;
                end = externalVal.indexOf(',', start);
                idx = end;
                if (end == -1) {
                    end = externalVal.length();
                }
            }
        }
        if (i != dimension) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "incompatible dimension (" + dimension +
                    ") and values (" + externalVal + ").  Only " + i + " values specified");
        }
        return out;
    }

    @Override
    protected void checkSupportsDocValues() {
        // DocValues supported only when enabled at the fieldType
        if (!hasProperty(DOC_VALUES)) {
            throw new UnsupportedOperationException("PointType can't have docValues=true in the field definition, use docValues=true in the fieldType definition, or in subFieldType/subFieldSuffix");
        }
    }

    @Override
    public UninvertingReader.Type getUninversionType(SchemaField sf) {
        return null;
    }

    @Override
    public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
        writer.writeStr(name, f.stringValue(), true);
    }

    @Override
    public SortField getSortField(SchemaField field, boolean top) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Sorting not supported on NVector " + field.getName());
    }

    @Override
    public boolean isPolyField() {
        return true;
    }
}

final class NVectorValueSource extends MultiValueSource {
    private final List<ValueSource> sources;

    public NVectorValueSource(List<ValueSource> sources) {
        this.sources = sources;
    }

    @Override
    public FunctionValues getValues(Map<Object,Object> context, LeafReaderContext readerContext) throws IOException {
        final FunctionValues x = sources.get(0)
            .getValues(context, readerContext);
        final FunctionValues y = sources.get(1)
            .getValues(context, readerContext);
        final FunctionValues z = sources.get(2)
            .getValues(context, readerContext);
        return new FunctionValues() {

            @Override
            public void byteVal(int doc, byte[] vals) throws IOException {
                vals[0] = x.byteVal(doc);
                vals[1] = y.byteVal(doc);
                vals[2] = z.byteVal(doc);
            }

            @Override
            public void shortVal(int doc, short[] vals) throws IOException {
                vals[0] = x.shortVal(doc);
                vals[1] = y.shortVal(doc);
                vals[2] = z.shortVal(doc);
            }

            @Override
            public void intVal(int doc, int[] vals) throws IOException {
                vals[0] = x.intVal(doc);
                vals[1] = y.intVal(doc);
                vals[2] = z.intVal(doc);
            }

            @Override
            public void longVal(int doc, long[] vals) throws IOException {
                vals[0] = x.longVal(doc);
                vals[1] = y.longVal(doc);
                vals[2] = z.longVal(doc);
            }

            @Override
            public void floatVal(int doc, float[] vals) throws IOException {
                vals[0] = x.floatVal(doc);
                vals[1] = y.floatVal(doc);
                vals[2] = z.floatVal(doc);
            }

            @Override
            public void doubleVal(int doc, double[] vals) throws IOException {
                vals[0] = x.doubleVal(doc);
                vals[1] = y.doubleVal(doc);
                vals[2] = z.doubleVal(doc);
            }

            @Override
            public void strVal(int doc, String[] vals) throws IOException {
                vals[0] = x.strVal(doc);
                vals[1] = y.strVal(doc);
                vals[2] = z.strVal(doc);
            }

            @Override
            public String toString(int doc) throws IOException {
                return "nvector(" + x.toString(doc) + "," + y.toString(doc) + "," + z.toString(doc) + ")";
            }
        };
    }

    @Override
    public String description() {
        StringBuilder sb = new StringBuilder();
        sb.append("nvector(");
        boolean firstTime = true;
        for (ValueSource source : sources) {
            if (firstTime) {
                firstTime = false;
            } else {
                sb.append(',');
            }
            sb.append(source);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NVectorValueSource)) return false;

        NVectorValueSource that = (NVectorValueSource) o;

        return sources.equals(that.sources);

    }

    @Override
    public int hashCode() {
        return sources.hashCode();
    }

    @Override
    public int dimension() {
        return sources.size();
    }
}
