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
package org.apache.solr.search.neural;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.solr.search.neural.KnnQParser.DEFAULT_TOP_K;

public class KnnQParserTest extends SolrTestCaseJ4 {
    String IDField = "id";
    String vectorField = "vector";
    String vectorField2 = "vector2";
    
    @Before
    public void prepareIndex() throws Exception {
        /* vectorDimension="4" similarityFunction="cosine" */
        initCore("solrconfig-basic.xml", "schema-densevector.xml");
        
        List<SolrInputDocument> docsToIndex = this.prepareDocs();
        for(SolrInputDocument doc:docsToIndex){
            assertU(adoc(doc));
        }

        assertU(commit());
    }

    private List<SolrInputDocument> prepareDocs() {
        int docsCount = 13;
        List<SolrInputDocument> docs = new ArrayList<>(docsCount);
        for (int i = 1; i < docsCount + 1; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField(IDField, i);
            docs.add(doc);
        }

        docs.get(0).addField(vectorField, Arrays.asList(1f, 2f, 3f, 4f)); // cosine distance vector1= 1.0
        docs.get(1).addField(vectorField, Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f)); // cosine distance vector1= 0.998
        docs.get(2).addField(vectorField, Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f)); // cosine distance vector1= 0.992
        docs.get(3).addField(vectorField, Arrays.asList(1.4f, 2.4f, 3.4f, 4.4f)); // cosine distance vector1= 0.999
        docs.get(4).addField(vectorField, Arrays.asList(30f, 22f, 35f, 20f)); // cosine distance vector1= 0.862
        docs.get(5).addField(vectorField, Arrays.asList(40f, 1f, 1f, 200f)); // cosine distance vector1= 0.756
        docs.get(6).addField(vectorField, Arrays.asList(5f, 10f, 20f, 40f)); // cosine distance vector1= 0.970
        docs.get(7).addField(vectorField, Arrays.asList(120f, 60f, 30f, 15f)); // cosine distance vector1= 0.515
        docs.get(8).addField(vectorField, Arrays.asList(200f, 50f, 100f, 25f)); // cosine distance vector1= 0.554
        docs.get(9).addField(vectorField, Arrays.asList(1.8f, 2.5f, 3.7f, 4.9f)); // cosine distance vector1= 0.997

        docs.get(10).addField(vectorField2, Arrays.asList(1f, 2f, 3f, 4f)); // cosine distance vector2= 1
        docs.get(11).addField(vectorField2, Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f)); // cosine distance vector2= 0.992
        docs.get(12).addField(vectorField2, Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f)); // cosine distance vector2= 0.998

        return docs;
    }

    @After
    public void cleanUp() throws Exception {
        clearIndex();
        deleteCore();
    }

    @Test
    public void incorrectTopK_shouldThrowException() {
        String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

        assertQEx("String topK should throw Exception",
                "For input string: \"string\"",
                req(CommonParams.Q, "{!knn f=vector topK=string}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);

        assertQEx("Double topK should throw Exception",
                "For input string: \"4.5\"",
                req(CommonParams.Q, "{!knn f=vector topK=4.5}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);
    }

    @Test
    public void topKMissing_shouldReturnDefaultTopK() {
        String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

        assertQ(req(CommonParams.Q, "{!knn f=vector}" + vectorToSearch, "fl", "id"),
                "//result[@numFound='" + DEFAULT_TOP_K + "']",
                "//result/doc[1]/str[@name='id'][.='1']",
                "//result/doc[2]/str[@name='id'][.='4']",
                "//result/doc[3]/str[@name='id'][.='2']",
                "//result/doc[4]/str[@name='id'][.='10']",
                "//result/doc[5]/str[@name='id'][.='3']",
                "//result/doc[6]/str[@name='id'][.='7']",
                "//result/doc[7]/str[@name='id'][.='5']",
                "//result/doc[8]/str[@name='id'][.='6']",
                "//result/doc[9]/str[@name='id'][.='9']",
                "//result/doc[10]/str[@name='id'][.='8']"
        );
    }

    @Test
    public void topK_shouldReturnOnlyTopKResults() {
        String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

        assertQ(req(CommonParams.Q, "{!knn f=vector topK=5}" + vectorToSearch, "fl", "id"),
                "//result[@numFound='5']",
                "//result/doc[1]/str[@name='id'][.='1']",
                "//result/doc[2]/str[@name='id'][.='4']",
                "//result/doc[3]/str[@name='id'][.='2']",
                "//result/doc[4]/str[@name='id'][.='10']",
                "//result/doc[5]/str[@name='id'][.='3']"
        );

        assertQ(req(CommonParams.Q, "{!knn f=vector topK=3}" + vectorToSearch, "fl", "id"),
                "//result[@numFound='3']",
                "//result/doc[1]/str[@name='id'][.='1']",
                "//result/doc[2]/str[@name='id'][.='4']",
                "//result/doc[3]/str[@name='id'][.='2']"
        );
    }

    @Test
    public void incorrectVectorFieldType_shouldThrowException() {
        String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

        assertQEx("Incorrect vector field type should throw Exception",
                "only DenseVectorField is compatible with Knn Query Parser",
                req(CommonParams.Q, "{!knn f=id topK=10}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);
    }

    @Test
    public void undefinedVectorField_shouldThrowException() {
        String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

        assertQEx("Undefined vector field should throw Exception",
                "undefined field: \"notExistent\"",
                req(CommonParams.Q, "{!knn f=notExistent topK=10}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);
    }

    @Test
    public void missingVectorField_shouldThrowException() {
        String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

        assertQEx("missing vector field should throw Exception",
                "the Dense Vector field 'f' is missing",
                req(CommonParams.Q, "{!knn topK=10}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);
    }

    @Test
    public void correctVectorField_shouldSearchOnThatField() {
        String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

        assertQ(req(CommonParams.Q, "{!knn f=vector2 topK=5}" + vectorToSearch, "fl", "id"),
                "//result[@numFound='3']",
                "//result/doc[1]/str[@name='id'][.='11']",
                "//result/doc[2]/str[@name='id'][.='13']",
                "//result/doc[3]/str[@name='id'][.='12']"
        );
    }

    @Test
    public void missingVectorToSearch_shouldThrowException() {
        assertQEx("missing vector to search should throw Exception",
                "the Dense Vector value 'v' to search is missing",
                req(CommonParams.Q, "{!knn f=vector topK=10}", "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);
    }

    @Test
    public void incorrectVectorToSearchDimension_shouldThrowException() {
        String vectorToSearch = "[2.0, 4.4, 3.]";
        assertQEx("missing vector to search should throw Exception",
                "incorrect vector dimension. The vector value has size 3 while it is expected a vector with size 4",
                req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);

        vectorToSearch = "[2.0, 4.4,,]";
        assertQEx("incorrect vector to search should throw Exception",
                "incorrect vector dimension. The vector value has size 2 while it is expected a vector with size 4",
                req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);
    }

    @Test
    public void incorrectVectorToSearch_shouldThrowException() {
        String vectorToSearch = "2.0, 4.4, 3.5, 6.4";
        assertQEx("incorrect vector to search should throw Exception",
                "incorrect vector format. The expected format is:'[f1,f2..f3]' where each element f is a float",
                req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);

        vectorToSearch = "[2.0, 4.4, 3.5, 6.4";
        assertQEx("incorrect vector to search should throw Exception",
                "incorrect vector format. The expected format is:'[f1,f2..f3]' where each element f is a float",
                req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);

        vectorToSearch = "2.0, 4.4, 3.5, 6.4]";
        assertQEx("incorrect vector to search should throw Exception",
                "incorrect vector format. The expected format is:'[f1,f2..f3]' where each element f is a float",
                req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);

        vectorToSearch = "[2.0, 4.4, 3.5, stringElement]";
        assertQEx("incorrect vector to search should throw Exception",
                "incorrect vector element: ' stringElement'. The expected format is:'[f1,f2..f3]' where each element f is a float",
                req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);

        vectorToSearch = "[2.0, 4.4, , ]";
        assertQEx("incorrect vector to search should throw Exception",
                "incorrect vector element: ' '. The expected format is:'[f1,f2..f3]' where each element f is a float",
                req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
                SolrException.ErrorCode.BAD_REQUEST);
    }

    @Test
    public void correctQuery_shouldRankBySimilarityFunction() {
        String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

        assertQ(req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fl", "id"),
                "//result[@numFound='10']",
                "//result/doc[1]/str[@name='id'][.='1']",
                "//result/doc[2]/str[@name='id'][.='4']",
                "//result/doc[3]/str[@name='id'][.='2']",
                "//result/doc[4]/str[@name='id'][.='10']",
                "//result/doc[5]/str[@name='id'][.='3']",
                "//result/doc[6]/str[@name='id'][.='7']",
                "//result/doc[7]/str[@name='id'][.='5']",
                "//result/doc[8]/str[@name='id'][.='6']",
                "//result/doc[9]/str[@name='id'][.='9']",
                "//result/doc[10]/str[@name='id'][.='8']"
        );
    }

    @Test
    public void knnQueryWithFilterQuery_shouldIntersectResults() {
        String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

        assertQ(req(CommonParams.Q, "{!knn f=vector topK=10}" + vectorToSearch, "fq", "id:(1 2 7 20)", "fl", "id"),
                "//result[@numFound='3']",
                "//result/doc[1]/str[@name='id'][.='1']",
                "//result/doc[2]/str[@name='id'][.='2']",
                "//result/doc[3]/str[@name='id'][.='7']"
        );
        /*
         * This behavior is counter-intuitive.
         * You would expect the query to apply to only the results filtered by the filter query.
         * So you ideally would like to see the following ranked list as a result: [4,2,3,9].
         * To get this please use the knn query parser as a reranker, example in a following test.
         * This is how filter queries work(it's just intersection of the bitsets coming from each query and filter query):
         * Ranked List from q=[1,4,2,10] <intersects> Set from fq={3,4,9,2} = [4,2]
         * */
        assertQ(req(CommonParams.Q, "{!knn f=vector topK=4}" + vectorToSearch, "fq", "id:(3 4 9 2)", "fl", "id"),
                "//result[@numFound='2']",
                "//result/doc[1]/str[@name='id'][.='4']",
                "//result/doc[2]/str[@name='id'][.='2']"
        );
        /* The ranking is now different as default solr score is used for the main query */
        assertQ(req(CommonParams.Q, "id:(3 4 9 2)", "fq", "{!knn f=vector topK=4}" + vectorToSearch, "fl", "id"),
                "//result[@numFound='2']",
                "//result/doc[1]/str[@name='id'][.='2']",
                "//result/doc[2]/str[@name='id'][.='4']"
        );
    }

    
    /*
     * See {@link org.apache.solr.search.ReRankQParserPlugin.ReRankQueryRescorer.combine} for more details.
     * */
    @Test
    public void knnQueryAsRerank_shouldAddSimilarityFunctionScore() {
        String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";

        assertQ(req(CommonParams.Q, "id:(3 4 9 2)", "rq", "{!rerank reRankQuery=$rqq reRankDocs=4 reRankWeight=1}",
                        "rqq", "{!knn f=vector topK=4}" + vectorToSearch, "fl", "id"),
                "//result[@numFound='4']",
                "//result/doc[1]/str[@name='id'][.='4']",
                "//result/doc[2]/str[@name='id'][.='2']",
                "//result/doc[3]/str[@name='id'][.='3']",
                "//result/doc[4]/str[@name='id'][.='9']"
        );
    }
}
