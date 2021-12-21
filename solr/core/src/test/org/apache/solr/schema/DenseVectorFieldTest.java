package org.apache.solr.schema;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.AbstractBadConfigTestBase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import org.hamcrest.MatcherAssert;

public class DenseVectorFieldTest extends AbstractBadConfigTestBase {
    
    @Before
    public void cleanUp() throws Exception {
        initCore("solrconfig-basic.xml", "schema-densevector.xml");
        clearIndex();
        assertU(commit());
    }
    
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
    public void fieldTypeDefinition_nullSimilarityDistance_shouldThrowException() throws Exception {
        assertConfigs("solrconfig-basic.xml", "bad-schema-densevector-similarity-null.xml",
                "similarity function is mandatory parameter");
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
    public void fieldDefinition_correctConfiguration_shouldLoadSchemaField() throws Exception {
        IndexSchema schema = h.getCore().getLatestSchema();

        SchemaField vector = schema.getField("vector");
        assertNotNull(vector);
        
        DenseVectorField type = (DenseVectorField) vector.getType();
        MatcherAssert.assertThat(type.similarityFunction, is(VectorSimilarityFunction.EUCLIDEAN));
        MatcherAssert.assertThat(type.dimension, is(4));

        assertTrue(vector.indexed());
        assertTrue(vector.stored());
    }

    @Test
    public void indexing_wrongVectorFormat_shouldThrowException() {
        assertFailedU(adoc("id", "0", "vector", "[1.0 ,2,3ss,4]"));
        assertFailedU(adoc("id", "0", "vector", "1,2,3,4]"));
        assertFailedU(adoc("id", "0", "vector", "[1,2,3,4"));
        assertFailedU(adoc("id", "0", "vector", "[1,,2,3,4]"));
    }

    @Test
    public void indexing_inconsistentVectorDimension_shouldThrowException() {
        assertFailedU(adoc("id", "0", "vector", "[1,2,3]"));
        assertFailedU(adoc("id", "0", "vector", "[1,2,3,4,5]"));
    }
    
    @Test
    public void indexing_correctDocument_shouldBeIndexed() {
        assertU(adoc("id", "0", "vector", "[1.0, 2.0, 3.0, 4.0]"));
    }

    @Test
    public void query_storedField_shouldBeReturnedInResults() {
        String vectorValue = "[1.0, 2.0, 3.0, 4.0]";
        assertU(adoc("id", "0", "vector", vectorValue));
        assertU(commit());
        
        assertQ(req("q", "id:0", "fl", "vector"), "*[count(//doc)=1]",
                "//result/doc[1]/str[@name='vector'][.='"+vectorValue+"']");
    }
    
    @Test
    public void query_rangeSearch_shouldThrowException() throws Exception {
        String vectorValue = "[1.0, 2.0, 3.0, 4.0]";
        assertU(adoc("id", "0", "vector", vectorValue));
        assertU(commit());

        assertQEx("Range queries over vectors are not supported",
                "Cannot parse 'vector:[[1.0 2.0] TO [1.5 2.5]]'",
                req("q","vector:[[1.0 2.0] TO [1.5 2.5]]", "fl", "vector"),
                SolrException.ErrorCode.BAD_REQUEST);

        assertQEx("Range queries over vectors are not supported",
                "Range Queries are not supported for Dense Vector fields." +
                        " Please use the {!knn} query parser to run K nearest neighbors search queries.",
                req("q","vector:[1 TO 5]", "fl", "vector"),
                SolrException.ErrorCode.BAD_REQUEST);
    }

    @Test
    public void query_fieldQuery_shouldThrowException() throws Exception {
        String vectorValue = "[1.0, 2.0, 3.0, 4.0]";
        assertU(adoc("id", "0", "vector", vectorValue));
        assertU(commit());

        assertQEx("Field queries over vectors are not supported",
                "Cannot parse 'vector:[1.0, 2.0, 3.0, 4.0]",
                req("q","vector:[1.0, 2.0, 3.0, 4.0]", "fl", "vector"),
                SolrException.ErrorCode.BAD_REQUEST);

        assertQEx("Field queries over vectors are not supported",
                "Field Queries are not supported for Dense Vector fields." +
                        " Please use the {!knn} query parser to run K nearest neighbors search queries.",
                req("q","vector:\"[1.0, 2.0, 3.0, 4.0]\"", "fl", "vector"),
                SolrException.ErrorCode.BAD_REQUEST);

        assertQEx("Field queries over vectors are not supported",
                "Field Queries are not supported for Dense Vector fields." +
                        " Please use the {!knn} query parser to run K nearest neighbors search queries.",
                req("q","vector:2.0", "fl", "vector"),
                SolrException.ErrorCode.BAD_REQUEST);
    }

    /*
    @Test
    public void query_sortByVectorField_shouldThrowException() throws Exception {
        assumeTrue("This test is only applicable to the XML file based exchange rate provider",
                expectedProviderClass.equals(FileExchangeRateProvider.class));

        clearIndex();

        assertU(adoc("id", "" + 1, fieldName, "10.00,USD"));
        assertU(adoc("id", "" + 2, fieldName, "15.00,EUR"));
        assertU(adoc("id", "" + 3, fieldName, "7.00,EUR"));
        assertU(adoc("id", "" + 4, fieldName, "6.00,GBP"));
        assertU(adoc("id", "" + 5, fieldName, "2.00,GBP"));
        assertU(commit());

        assertQ(req("fl", "*,score", "q", "*:*", "sort", fieldName+" desc", "limit", "1"), "//str[@name='id']='4'");
        assertQ(req("fl", "*,score", "q", "*:*", "sort", fieldName+" asc", "limit", "1"), "//str[@name='id']='3'");
    }

    public void query_functionQueryUsage_shouldThrowException() throws Exception {
        assumeTrue("This test is only applicable to the XML file based exchange rate provider",
                expectedProviderClass.equals(FileExchangeRateProvider.class));

        clearIndex();
        for (int i = 1; i <= 8; i++) {
            // "GBP" currency code is 1/2 of a USD dollar, for testing.
            assertU(adoc("id", "" + i, fieldName, (((float)i)/2) + ",GBP"));
        }
        for (int i = 9; i <= 11; i++) {
            assertU(adoc("id", "" + i, fieldName, i + ",USD"));
        }

        assertU(commit());

        // direct value source usage, gets "raw" form od default currency
        // default==USD, so raw==penies
        assertQ(req("fl", "id,func:field($f)",
                        "f", fieldName,
                        "q", "id:5"),
                "//*[@numFound='1']",
                "//doc/float[@name='func' and .=500]");
        assertQ(req("fl", "id,func:field($f)",
                        "f", fieldName,
                        "q", "id:10"),
                "//*[@numFound='1']",
                "//doc/float[@name='func' and .=1000]");
        assertQ(req("fl", "id,score,"+fieldName,
                        "q", "{!frange u=500}"+fieldName)
                ,"//*[@numFound='5']"
                ,"//str[@name='id']='1'"
                ,"//str[@name='id']='2'"
                ,"//str[@name='id']='3'"
                ,"//str[@name='id']='4'"
                ,"//str[@name='id']='5'"
        );
        assertQ(req("fl", "id,score,"+fieldName,
                        "q", "{!frange l=500 u=1000}"+fieldName)
                ,"//*[@numFound='6']"
                ,"//str[@name='id']='5'"
                ,"//str[@name='id']='6'"
                ,"//str[@name='id']='7'"
                ,"//str[@name='id']='8'"
                ,"//str[@name='id']='9'"
                ,"//str[@name='id']='10'"
        );

        // use the currency function to convert to default (USD)
        assertQ(req("fl", "id,func:currency($f)",
                        "f", fieldName,
                        "q", "id:10"),
                "//*[@numFound='1']",
                "//doc/float[@name='func' and .=10]");
        assertQ(req("fl", "id,func:currency($f)",
                        "f", fieldName,
                        "q", "id:5"),
                "//*[@numFound='1']",
                "//doc/float[@name='func' and .=5]");
        assertQ(req("fl", "id,score"+fieldName,
                        "f", fieldName,
                        "q", "{!frange u=5}currency($f)")
                ,"//*[@numFound='5']"
                ,"//str[@name='id']='1'"
                ,"//str[@name='id']='2'"
                ,"//str[@name='id']='3'"
                ,"//str[@name='id']='4'"
                ,"//str[@name='id']='5'"
        );
        assertQ(req("fl", "id,score"+fieldName,
                        "f", fieldName,
                        "q", "{!frange l=5 u=10}currency($f)")
                ,"//*[@numFound='6']"
                ,"//str[@name='id']='5'"
                ,"//str[@name='id']='6'"
                ,"//str[@name='id']='7'"
                ,"//str[@name='id']='8'"
                ,"//str[@name='id']='9'"
                ,"//str[@name='id']='10'"
        );

        // use the currency function to convert to MXN
        assertQ(req("fl", "id,func:currency($f,MXN)",
                        "f", fieldName,
                        "q", "id:5"),
                "//*[@numFound='1']",
                "//doc/float[@name='func' and .=10]");
        assertQ(req("fl", "id,func:currency($f,MXN)",
                        "f", fieldName,
                        "q", "id:10"),
                "//*[@numFound='1']",
                "//doc/float[@name='func' and .=20]");
        assertQ(req("fl", "*,score,"+fieldName,
                        "f", fieldName,
                        "q", "{!frange u=10}currency($f,MXN)")
                ,"//*[@numFound='5']"
                ,"//str[@name='id']='1'"
                ,"//str[@name='id']='2'"
                ,"//str[@name='id']='3'"
                ,"//str[@name='id']='4'"
                ,"//str[@name='id']='5'"
        );
        assertQ(req("fl", "*,score,"+fieldName,
                        "f", fieldName,
                        "q", "{!frange l=10 u=20}currency($f,MXN)")
                ,"//*[@numFound='6']"
                ,"//str[@name='id']='5'"
                ,"//str[@name='id']='6'"
                ,"//str[@name='id']='7'"
                ,"//str[@name='id']='8'"
                ,"//str[@name='id']='9'"
                ,"//str[@name='id']='10'"
        );

    }

    @Test
    public void query_facetingOverVectorField_shouldThrowException() throws Exception {
        assumeTrue("This test is only applicable to the XML file based exchange rate provider " +
                        "because it excercies the asymetric exchange rates option it supports",
                expectedProviderClass.equals(FileExchangeRateProvider.class));

        clearIndex();

        // NOTE: in our test conversions EUR uses an asynetric echange rate
        // these are the equivalent values when converting to:     USD        EUR        GBP
        assertU(adoc("id", "" + 1, fieldName, "10.00,USD"));   // 10.00,USD  25.00,EUR   5.00,GBP
        assertU(adoc("id", "" + 2, fieldName, "15.00,EUR"));   //  7.50,USD  15.00,EUR   7.50,GBP
        assertU(adoc("id", "" + 3, fieldName, "6.00,GBP"));    // 12.00,USD  12.00,EUR   6.00,GBP
        assertU(adoc("id", "" + 4, fieldName, "7.00,EUR"));    //  3.50,USD   7.00,EUR   3.50,GBP
        assertU(adoc("id", "" + 5, fieldName, "2,GBP"));       //  4.00,USD   4.00,EUR   2.00,GBP
        assertU(commit());

        for (String suffix : Arrays.asList("", ",USD")) {
            assertQ("Ensure that we get correct facet counts back in USD (explicit or implicit default) (facet.range)",
                    req("fl", "*,score", "q", "*:*", "rows", "0", "facet", "true",
                            "facet.range", fieldName,
                            "f." + fieldName + ".facet.range.start", "4.00" + suffix,
                            "f." + fieldName + ".facet.range.end", "11.00" + suffix,
                            "f." + fieldName + ".facet.range.gap", "1.00" + suffix,
                            "f." + fieldName + ".facet.range.other", "all")
                    ,"count(//lst[@name='counts']/int)=7"
                    ,"//lst[@name='counts']/int[@name='4.00,USD']='1'"
                    ,"//lst[@name='counts']/int[@name='5.00,USD']='0'"
                    ,"//lst[@name='counts']/int[@name='6.00,USD']='0'"
                    ,"//lst[@name='counts']/int[@name='7.00,USD']='1'"
                    ,"//lst[@name='counts']/int[@name='8.00,USD']='0'"
                    ,"//lst[@name='counts']/int[@name='9.00,USD']='0'"
                    ,"//lst[@name='counts']/int[@name='10.00,USD']='1'"
                    ,"//int[@name='after']='1'"
                    ,"//int[@name='before']='1'"
                    ,"//int[@name='between']='3'"
            );
            assertQ("Ensure that we get correct facet counts back in USD (explicit or implicit default) (json.facet)",
                    req("fl", "*,score", "q", "*:*", "rows", "0", "json.facet",
                            "{ xxx : { type:range, field:" + fieldName + ", " +
                                    "          start:'4.00"+suffix+"', gap:'1.00"+suffix+"', end:'11.00"+suffix+"', other:all } }")
                    ,"count(//lst[@name='xxx']/arr[@name='buckets']/lst)=7"
                    ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='4.00,USD']]"
                    ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='5.00,USD']]"
                    ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='6.00,USD']]"
                    ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='7.00,USD']]"
                    ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='8.00,USD']]"
                    ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='9.00,USD']]"
                    ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='10.00,USD']]"
                    ,"//lst[@name='xxx']/lst[@name='before' ]/long[@name='count'][.='1']"
                    ,"//lst[@name='xxx']/lst[@name='after'  ]/long[@name='count'][.='1']"
                    ,"//lst[@name='xxx']/lst[@name='between']/long[@name='count'][.='3']"
            );
        }

        assertQ("Zero value as start range point + mincount (facet.range)",
                req("fl", "*,score", "q", "*:*", "rows", "0", "facet", "true", "facet.mincount", "1",
                        "facet.range", fieldName,
                        "f." + fieldName + ".facet.range.start", "0,USD",
                        "f." + fieldName + ".facet.range.end", "11.00,USD",
                        "f." + fieldName + ".facet.range.gap", "1.00,USD",
                        "f." + fieldName + ".facet.range.other", "all")
                ,"count(//lst[@name='counts']/int)=4"
                ,"//lst[@name='counts']/int[@name='3.00,USD']='1'"
                ,"//lst[@name='counts']/int[@name='4.00,USD']='1'"
                ,"//lst[@name='counts']/int[@name='7.00,USD']='1'"
                ,"//lst[@name='counts']/int[@name='10.00,USD']='1'"
                ,"//int[@name='before']='0'"
                ,"//int[@name='after']='1'"
                ,"//int[@name='between']='4'"
        );
        assertQ("Zero value as start range point + mincount (json.facet)",
                req("fl", "*,score", "q", "*:*", "rows", "0", "json.facet",
                        "{ xxx : { type:range, mincount:1, field:" + fieldName +
                                ", start:'0.00,USD', gap:'1.00,USD', end:'11.00,USD', other:all } }")
                ,"count(//lst[@name='xxx']/arr[@name='buckets']/lst)=4"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='3.00,USD']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='4.00,USD']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='7.00,USD']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='10.00,USD']]"
                ,"//lst[@name='xxx']/lst[@name='before' ]/long[@name='count'][.='0']"
                ,"//lst[@name='xxx']/lst[@name='after'  ]/long[@name='count'][.='1']"
                ,"//lst[@name='xxx']/lst[@name='between']/long[@name='count'][.='4']"
        );

        // NOTE: because of asymetric EUR exchange rate, these buckets are diff then the similar looking USD based request above
        // This request converts the values in each doc into EUR to decide what range buck it's in.
        assertQ("Ensure that we get correct facet counts back in EUR (facet.range)",
                req("fl", "*,score", "q", "*:*", "rows", "0", "facet", "true",
                        "facet.range", fieldName,
                        "f." + fieldName + ".facet.range.start", "8.00,EUR",
                        "f." + fieldName + ".facet.range.end", "22.00,EUR",
                        "f." + fieldName + ".facet.range.gap", "2.00,EUR",
                        "f." + fieldName + ".facet.range.other", "all"
                )
                , "count(//lst[@name='counts']/int)=7"
                , "//lst[@name='counts']/int[@name='8.00,EUR']='0'"
                , "//lst[@name='counts']/int[@name='10.00,EUR']='0'"
                , "//lst[@name='counts']/int[@name='12.00,EUR']='1'"
                , "//lst[@name='counts']/int[@name='14.00,EUR']='1'"
                , "//lst[@name='counts']/int[@name='16.00,EUR']='0'"
                , "//lst[@name='counts']/int[@name='18.00,EUR']='0'"
                , "//lst[@name='counts']/int[@name='20.00,EUR']='0'"
                , "//int[@name='before']='2'"
                , "//int[@name='after']='1'"
                , "//int[@name='between']='2'"
        );
        assertQ("Ensure that we get correct facet counts back in EUR (json.facet)",
                req("fl", "*,score", "q", "*:*", "rows", "0", "json.facet",
                        "{ xxx : { type:range, field:" + fieldName + ", start:'8.00,EUR', gap:'2.00,EUR', end:'22.00,EUR', other:all } }")
                ,"count(//lst[@name='xxx']/arr[@name='buckets']/lst)=7"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='8.00,EUR']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='10.00,EUR']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='12.00,EUR']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='14.00,EUR']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='16.00,EUR']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='18.00,EUR']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='20.00,EUR']]"
                ,"//lst[@name='xxx']/lst[@name='before' ]/long[@name='count'][.='2']"
                ,"//lst[@name='xxx']/lst[@name='after'  ]/long[@name='count'][.='1']"
                ,"//lst[@name='xxx']/lst[@name='between']/long[@name='count'][.='2']"
        );


        // GBP has a symetric echange rate with USD, so these counts are *similar* to the USD based request above...
        // but the asymetric EUR/USD rate means that when computing counts realtive to GBP the EUR based docs wind up in
        // diff buckets
        assertQ("Ensure that we get correct facet counts back in GBP (facet.range)",
                req("fl", "*,score", "q", "*:*", "rows", "0", "facet", "true",
                        "facet.range", fieldName,
                        "f." + fieldName + ".facet.range.start", "2.00,GBP",
                        "f." + fieldName + ".facet.range.end", "5.50,GBP",
                        "f." + fieldName + ".facet.range.gap", "0.50,GBP",
                        "f." + fieldName + ".facet.range.other", "all"
                )
                , "count(//lst[@name='counts']/int)=7"
                , "//lst[@name='counts']/int[@name='2.00,GBP']='1'"
                , "//lst[@name='counts']/int[@name='2.50,GBP']='0'"
                , "//lst[@name='counts']/int[@name='3.00,GBP']='0'"
                , "//lst[@name='counts']/int[@name='3.50,GBP']='1'"
                , "//lst[@name='counts']/int[@name='4.00,GBP']='0'"
                , "//lst[@name='counts']/int[@name='4.50,GBP']='0'"
                , "//lst[@name='counts']/int[@name='5.00,GBP']='1'"
                , "//int[@name='before']='0'"
                , "//int[@name='after']='2'"
                , "//int[@name='between']='3'"
        );
        assertQ("Ensure that we get correct facet counts back in GBP (json.facet)",
                req("fl", "*,score", "q", "*:*", "rows", "0", "json.facet",
                        "{ xxx : { type:range, field:" + fieldName + ", start:'2.00,GBP', gap:'0.50,GBP', end:'5.50,GBP', other:all } }")
                ,"count(//lst[@name='xxx']/arr[@name='buckets']/lst)=7"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='2.00,GBP']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='2.50,GBP']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='3.00,GBP']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='3.50,GBP']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='4.00,GBP']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='4.50,GBP']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='5.00,GBP']]"
                ,"//lst[@name='xxx']/lst[@name='before' ]/long[@name='count'][.='0']"
                ,"//lst[@name='xxx']/lst[@name='after'  ]/long[@name='count'][.='2']"
                ,"//lst[@name='xxx']/lst[@name='between']/long[@name='count'][.='3']"
        );

        assertQ("Ensure that we can set a gap in a currency other than the start and end currencies (facet.range)",
                req("fl", "*,score", "q", "*:*", "rows", "0", "facet", "true",
                        "facet.range", fieldName,
                        "f." + fieldName + ".facet.range.start", "4.00,USD",
                        "f." + fieldName + ".facet.range.end", "11.00,USD",
                        "f." + fieldName + ".facet.range.gap", "0.50,GBP",
                        "f." + fieldName + ".facet.range.other", "all"
                )
                , "count(//lst[@name='counts']/int)=7"
                , "//lst[@name='counts']/int[@name='4.00,USD']='1'"
                , "//lst[@name='counts']/int[@name='5.00,USD']='0'"
                , "//lst[@name='counts']/int[@name='6.00,USD']='0'"
                , "//lst[@name='counts']/int[@name='7.00,USD']='1'"
                , "//lst[@name='counts']/int[@name='8.00,USD']='0'"
                , "//lst[@name='counts']/int[@name='9.00,USD']='0'"
                , "//lst[@name='counts']/int[@name='10.00,USD']='1'"
                , "//int[@name='before']='1'"
                , "//int[@name='after']='1'"
                , "//int[@name='between']='3'"
        );
        assertQ("Ensure that we can set a gap in a currency other than the start and end currencies (json.facet)",
                req("fl", "*,score", "q", "*:*", "rows", "0", "json.facet",
                        "{ xxx : { type:range, field:" + fieldName + ", start:'4.00,USD', gap:'0.50,GBP', end:'11.00,USD', other:all } }")
                ,"count(//lst[@name='xxx']/arr[@name='buckets']/lst)=7"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='4.00,USD']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='5.00,USD']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='6.00,USD']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='7.00,USD']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='8.00,USD']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='0']][str[@name='val'][.='9.00,USD']]"
                ,"//lst[@name='xxx']/arr[@name='buckets']/lst[long[@name='count'][.='1']][str[@name='val'][.='10.00,USD']]"

                ,"//lst[@name='xxx']/lst[@name='before' ]/long[@name='count'][.='1']"
                ,"//lst[@name='xxx']/lst[@name='after'  ]/long[@name='count'][.='1']"
                ,"//lst[@name='xxx']/lst[@name='between']/long[@name='count'][.='3']"
        );

        for (SolrParams facet : Arrays.asList(params("facet", "true",
                        "facet.range", fieldName,
                        "f." + fieldName + ".facet.range.start", "4.00,USD",
                        "f." + fieldName + ".facet.range.end", "11.00,EUR",
                        "f." + fieldName + ".facet.range.gap", "1.00,USD",
                        "f." + fieldName + ".facet.range.other", "all"),
                params("json.facet",
                        "{ xxx : { type:range, field:" + fieldName + ", start:'4.00,USD', " +
                                "          gap:'1.00,USD', end:'11.00,EUR', other:all } }"))) {
            assertQEx("Ensure that we throw an error if we try to use different start and end currencies",
                    "Cannot compare CurrencyValues when their currencies are not equal",
                    req(facet, "q", "*:*"),
                    SolrException.ErrorCode.BAD_REQUEST);
        }
    }

    */
}
