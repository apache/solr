package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DenseVectorFieldTest extends SolrTestCaseJ4 {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("solrconfig-basic.xml", "schema-dense-vector.xml");
    }

    @Before
    public void cleanUp() {
        clearIndex();
        assertU(commit());
    }

    @Test
    public void testVectorFieldIndexing() {
        assertU(adoc("id", "0", "vector", "[1.0, 2.0, 3.0, 4.0]"));
    }

    @Test
    public void testVectorFieldRetrieval() {
        assertU(adoc("id", "0", "vector", "[1.0, 2.0, 3.0, 4.0]"));
        assertU(commit());
        assertQ(req("q", "id:0", "fl", "vector"), "*[count(//doc)=1]",
                "//result/doc[1]/str[@name='vector'][.='[1.0, 2.0, 3.0, 4.0]']");
    }

    @Test
    public void testWrongVectorFormatShouldRaiseException() {
        assertFailedU(adoc("id", "0", "vector", "[1.0 ,2,3ss,4]"));
        assertFailedU(adoc("id", "0", "vector", "1,2,3,4]"));
        assertFailedU(adoc("id", "0", "vector", "[1,2,3,4"));
        assertFailedU(adoc("id", "0", "vector", "[1,,2,3,4]"));
    }

    @Test
    public void testWrongVectorDimensionShouldRaiseException() {
        assertFailedU(adoc("id", "0", "vector", "[1,2,3]"));
        assertFailedU(adoc("id", "0", "vector", "[1,2,3,4,5]"));
    }
}
