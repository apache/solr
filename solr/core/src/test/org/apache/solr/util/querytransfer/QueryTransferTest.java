package org.apache.solr.util.querytransfer;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SyntaxError;
import org.junit.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class QueryTransferTest extends SolrTestCase {
    @Test
    public void testTermQuery() throws IOException, SyntaxError {
        final TermQuery termQuery = randomTermQuery();
        assertTransfer(termQuery);
    }

    private static TermQuery randomTermQuery() {
        return new TermQuery(new Term(TestUtil.randomSimpleString(random()), TestUtil.randomUnicodeString(random())));
    }

    @Test
    public void testBoostQuery() throws IOException, SyntaxError {
        final TermQuery termQuery = randomTermQuery();
        final BoostQuery boostQuery = randomBoostQuery(termQuery);
        assertTransfer(boostQuery);
    }

    final BooleanClause.Occur[] occurs = BooleanClause.Occur.values();
    @Test
    public void testBoolQuery() throws IOException, SyntaxError {
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        Set<Query> noDupes = new HashSet<>();
        int clauses = 1+random().nextInt(5);
        for (int i =0; i<clauses; i++) {
            Query termQuery = randomTermQuery();
            if (random().nextBoolean()) {
                termQuery = randomBoostQuery(termQuery);
            }
            if (noDupes.add(termQuery)) {
                builder.add(termQuery, occurs[random().nextInt(occurs.length)]);
            }
        }
        if (random().nextBoolean()) {
            builder.setMinimumNumberShouldMatch(random().nextInt(5));
        }
        assertTransfer(builder.build());
    }
    private static BoostQuery randomBoostQuery(Query termQuery) {
        return new BoostQuery(termQuery, random().nextFloat() * random().nextInt(10));
    }

    private static void assertTransfer(Query termQuery) throws IOException, SyntaxError {
        final Query mltQ = mltRemoteReduce(termQuery);
        assertEquals(termQuery, mltQ);
        assertNotSame(termQuery, mltQ);
    }

    private static Query mltRemoteReduce(Query termQuery) throws IOException, SyntaxError {
        final byte[] blob = QueryTransfer.transfer(termQuery);
        final LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, new NamedList<>());
        final String qstr = Base64.getEncoder().encodeToString(blob);
        final Query mltQ;
        try (QParserPlugin plugin = new QueryTransferQParserPlugin()) {
            mltQ = plugin.createParser(qstr, new MapSolrParams(Map.of()),
                    new MapSolrParams(Map.of()), req).getQuery();
            req.close();
        }
        return mltQ;
    }
}
