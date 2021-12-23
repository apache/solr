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

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.AbstractBadConfigTestBase;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class DenseVectorFieldTest extends AbstractBadConfigTestBase {
    
    @Test
    public void fieldTypeDefinition_badVectorDimension_shouldThrowException() throws Exception {
        assertConfigs("solrconfig-basic.xml", "bad-schema-densevector-dimension.xml",
                "the vector dimension must be an integer");
    }

    @Test
    public void fieldTypeDefinition_nullVectorDimension_shouldThrowException() throws Exception {
        assertConfigs("solrconfig-basic.xml", "bad-schema-densevector-dimension-null.xml",
                "the vector dimension is a mandatory parameter");
    }

    @Test
    public void fieldTypeDefinition_badSimilarityDistance_shouldThrowException() throws Exception {
        assertConfigs("solrconfig-basic.xml", "bad-schema-densevector-similarity.xml",
                "No enum constant org.apache.lucene.index.VectorSimilarityFunction.NOT_EXISTENT");
    }

    @Test
    public void fieldDefinition_docValues_shouldThrowException() throws Exception {
        assertConfigs("solrconfig-basic.xml", "bad-schema-densevector-docvalues.xml",
                "does not support doc values");
    }

    @Test
    public void fieldDefinition_multiValued_shouldThrowException() throws Exception {
        assertConfigs("solrconfig-basic.xml", "bad-schema-densevector-multivalued.xml",
                "DenseVectorField fields can not be multiValued: vector");
    }

    @Test
    public void fieldTypeDefinition_nullSimilarityDistance_shouldUseDefaultSimilarityEuclidean() throws Exception {
        try {
            initCore("solrconfig-basic.xml", "schema-densevector-similarity-null.xml");
            IndexSchema schema = h.getCore().getLatestSchema();

            SchemaField vector = schema.getField("vector");
            assertNotNull(vector);

            DenseVectorField type = (DenseVectorField) vector.getType();
            MatcherAssert.assertThat(type.similarityFunction, is(VectorSimilarityFunction.EUCLIDEAN));
            MatcherAssert.assertThat(type.dimension, is(4));

            assertTrue(vector.indexed());
            assertTrue(vector.stored());
        } finally {
            deleteCore();
        }
    }

    @Test
    public void fieldDefinition_correctConfiguration_shouldLoadSchemaField() throws Exception {
        try {
            initCore("solrconfig-basic.xml", "schema-densevector.xml");
            IndexSchema schema = h.getCore().getLatestSchema();

            SchemaField vector = schema.getField("vector");
            assertNotNull(vector);

            DenseVectorField type = (DenseVectorField) vector.getType();
            MatcherAssert.assertThat(type.similarityFunction, is(VectorSimilarityFunction.COSINE));
            MatcherAssert.assertThat(type.dimension, is(4));

            assertTrue(vector.indexed());
            assertTrue(vector.stored());
        } finally {
            deleteCore();
        }
    }

    @Test
    public void indexing_incorrectVectorFormat_shouldThrowException() throws Exception {
        try {
            initCore("solrconfig-basic.xml", "schema-densevector.xml");

            assertFailedU(adoc("id", "0", "vector", "[1.0 ,2,3ss,4]"));
            assertFailedU(adoc("id", "0", "vector", "1,2,3,4]"));
            assertFailedU(adoc("id", "0", "vector", "2.0, 4.4, 3.5, 6.4"));
            assertFailedU(adoc("id", "0", "vector", "[1,2,3,4"));
            assertFailedU(adoc("id", "0", "vector", "[1,,2,3,4]"));
        } finally {
            deleteCore();
        }
    }

    @Test
    public void indexing_inconsistentVectorDimension_shouldThrowException() throws Exception {
        try {
            initCore("solrconfig-basic.xml", "schema-densevector.xml");

            assertFailedU(adoc("id", "0", "vector", "[1, 2, 3]"));
            assertFailedU(adoc("id", "0", "vector", "[1, 2, 3, 4, 5]"));
        } finally {
            deleteCore();
        }
    }

    @Test
    public void indexing_correctDocument_shouldBeIndexed() throws Exception {
        try {
            initCore("solrconfig-basic.xml", "schema-densevector.xml");

            assertU(adoc("id", "0", "vector", "[1.0, 2.0, 3.0, 4.0]"));
        } finally {
            deleteCore();
        }
    }

    @Test
    public void query_storedField_shouldBeReturnedInResults() throws Exception {
        try {
            initCore("solrconfig-basic.xml", "schema-densevector.xml");

            String vectorValue = "[1.0, 2.0, 3.0, 4.0]";
            assertU(adoc("id", "0", "vector", vectorValue));
            assertU(commit());

            assertQ(req("q", "id:0", "fl", "vector"), "*[count(//doc)=1]",
                    "//result/doc[1]/str[@name='vector'][.='" + vectorValue + "']");
        } finally {
            deleteCore();
        }
    }

    /**
     * Not Supported
     */
    @Test
    public void query_rangeSearch_shouldThrowException() throws Exception {
        try {
            initCore("solrconfig-basic.xml", "schema-densevector.xml");

            String vectorValue = "[1.0, 2.0, 3.0, 4.0]";
            assertU(adoc("id", "0", "vector", vectorValue));
            assertU(commit());

            assertQEx("Range queries over vectors are not supported",
                    "Cannot parse 'vector:[[1.0 2.0] TO [1.5 2.5]]'",
                    req("q", "vector:[[1.0 2.0] TO [1.5 2.5]]", "fl", "vector"),
                    SolrException.ErrorCode.BAD_REQUEST);

            assertQEx("Range queries over vectors are not supported",
                    "Range Queries are not supported for Dense Vector fields." +
                            " Please use the {!knn} query parser to run K nearest neighbors search queries.",
                    req("q", "vector:[1 TO 5]", "fl", "vector"),
                    SolrException.ErrorCode.BAD_REQUEST);
        } finally {
            deleteCore();
        }
    }

    /**
     * Not Supported
     */
    @Test
    public void query_existenceSearch_shouldThrowException() throws Exception {
        try {
            initCore("solrconfig-basic.xml", "schema-densevector.xml");

            String vectorValue = "[1.0, 2.0, 3.0, 4.0]";
            assertU(adoc("id", "0", "vector", vectorValue));
            assertU(commit());

            assertQEx("Range queries over vectors are not supported",
                    "Range Queries are not supported for Dense Vector fields." +
                            " Please use the {!knn} query parser to run K nearest neighbors search queries.",
                    req("q", "vector:[* TO *]", "fl", "vector"),
                    SolrException.ErrorCode.BAD_REQUEST);
        } finally {
            deleteCore();
        }
    }

    /**
     * Not Supported
     */
    @Test
    public void query_fieldQuery_shouldThrowException() throws Exception {
        try {
            initCore("solrconfig-basic.xml", "schema-densevector.xml");

            String vectorValue = "[1.0, 2.0, 3.0, 4.0]";
            assertU(adoc("id", "0", "vector", vectorValue));
            assertU(commit());

            assertQEx("Field queries over vectors are not supported",
                    "Cannot parse 'vector:[1.0, 2.0, 3.0, 4.0]",
                    req("q", "vector:[1.0, 2.0, 3.0, 4.0]", "fl", "vector"),
                    SolrException.ErrorCode.BAD_REQUEST);

            assertQEx("Field queries over vectors are not supported",
                    "Field Queries are not supported for Dense Vector fields." +
                            " Please use the {!knn} query parser to run K nearest neighbors search queries.",
                    req("q", "vector:\"[1.0, 2.0, 3.0, 4.0]\"", "fl", "vector"),
                    SolrException.ErrorCode.BAD_REQUEST);

            assertQEx("Field queries over vectors are not supported",
                    "Field Queries are not supported for Dense Vector fields." +
                            " Please use the {!knn} query parser to run K nearest neighbors search queries.",
                    req("q", "vector:2.0", "fl", "vector"),
                    SolrException.ErrorCode.BAD_REQUEST);
        } finally {
            deleteCore();
        }
    }

    /**
     * Not Supported
     */
    @Test
    public void query_sortByVectorField_shouldThrowException() throws Exception {
        try {
            initCore("solrconfig-basic.xml", "schema-densevector.xml");

            String vectorValue = "[1.0, 2.0, 3.0, 4.0]";
            assertU(adoc("id", "0", "vector", vectorValue));
            assertU(commit());

            assertQEx("Sort over vectors is not supported",
                    "Cannot sort on a Dense Vector field",
                    req("q", "*:*", "sort", "vector desc"),
                    SolrException.ErrorCode.BAD_REQUEST);
        } finally {
            deleteCore();
        }
    }

    /**
     * Not Supported
     */
    @Test
    public void query_functionQueryUsage_shouldThrowException() throws Exception {
        try {
            initCore("solrconfig-basic.xml", "schema-densevector.xml");

            String vectorValue = "[1.0, 2.0, 3.0, 4.0]";
            assertU(adoc("id", "0", "vector", vectorValue));
            assertU(commit());

            assertQEx("Function Queries over vectors are not supported",
                    "Function queries are not supported for Dense Vector fields.",
                    req("q", "*:*", "fl", "id,field(vector)"),
                    SolrException.ErrorCode.BAD_REQUEST);
        } finally {
            deleteCore();
        }
    }
}
