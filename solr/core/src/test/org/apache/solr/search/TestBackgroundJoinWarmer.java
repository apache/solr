package org.apache.solr.search;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.servlet.DirectSolrConnection;
import org.apache.solr.util.RefCounted;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestBackgroundJoinWarmer extends SolrTestCaseJ4 {

    private static SolrCore fromCore;

    @BeforeClass
    public static void beforeTests() throws Exception {
        System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
        System.setProperty("solr.filterCache.async", "true");

        initCore("solrconfig-crosscore-join-cache.xml", "schema12.xml", TEST_HOME(), "collection1");
        final CoreContainer coreContainer = h.getCoreContainer();

        fromCore = coreContainer.create("fromCore", ImmutableMap.of("configSet", "minimal-bckg-join"));

        assertU(add(doc("id", "1", "id_s_dv", "1", "name", "john", "title", "Director", "dept_s", "Engineering")));
        assertU(add(doc("id", "2", "id_s_dv", "2", "name", "mark", "title", "VP", "dept_s", "Marketing")));
        assertU(add(doc("id", "3", "id_s_dv", "3", "name", "nancy", "title", "MTS", "dept_s", "Sales")));
        assertU(add(doc("id", "4", "id_s_dv", "4", "name", "dave", "title", "MTS", "dept_s", "Support", "dept_s", "Engineering")));
        assertU(add(doc("id", "5", "id_s_dv", "5", "name", "tina", "title", "VP", "dept_s", "Engineering")));
        assertU(commit());

        update(fromCore, add(doc("id", "10", "id_s_dv", "10", "dept_id_s", "Engineering", "text", "These guys develop stuff", "cat", "dev")));
        update(fromCore, add(doc("id", "11", "id_s_dv", "11", "dept_id_s", "Marketing", "text", "These guys make you look good")));
        update(fromCore, add(doc("id", "12", "id_s_dv", "12", "dept_id_s", "Sales", "text", "These guys sell stuff")));
        update(fromCore, add(doc("id", "13", "id_s_dv", "13", "dept_id_s", "Support", "text", "These guys help customers")));
        update(fromCore, commit());

    }


    public static String update(SolrCore core, String xml) throws Exception {
        return update(core, xml, null);
    }

    public static String update(SolrCore core, String xml, SolrParams params) throws Exception {
        DirectSolrConnection connection = new DirectSolrConnection(core);
        SolrRequestHandler handler = core.getRequestHandler("/update");
        return connection.request(handler, params, xml);
    }

    @Test
    public void testScoreJoin() throws Exception {
        doTestJoin("{!join "+(random().nextBoolean() ? "score=none" : ""));
    }

    void doTestJoin(String joinPrefix) throws Exception {
        long toSearcherOpenTime = getSearcherOpenTime();
        assertUserCacheStats("fromCore", "size", 0);
        assertTrue(toSearcherOpenTime>0L);
        for(long hits=0L; hits<2L; hits++) {
            assertJQ(req(qOrFq(joinPrefix + " from=dept_id_s to=dept_s cacheEventually=true fromIndex=fromCore}cat:dev", "fl", "id",
                    "debugQuery", random().nextBoolean() ? "true" : "false"))
                    , "/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'},{'id':'5'}]}"
            );
            // assert user cache hit
            assertUserCacheStats("fromCore", "size", 1,
                    "hits", hits, "cumulative_hits", hits, //0, 1
                    "inserts", 1L , "cumulative_inserts", 1L);
        }
        update(fromCore, delQ("id:10"));
        update(fromCore, add(doc("id", "10", "id_s_dv", "10", "dept_id_s", "Marketing",
                "text", "now they sell", "cat", "dev")));
        update(fromCore, commit());
        // change in fromCore isn't visible due to user cache cache
        assertJQ(req(qOrFq( joinPrefix + " from=dept_id_s to=dept_s cacheEventually=true fromIndex=fromCore}cat:dev", "fl", "id",
                "debugQuery", random().nextBoolean() ? "true" : "false"))
                , "/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'},{'id':'5'}]}"
        );
        // nothing have changed ...
        assertUserCacheStats("fromCore", "size", 1,
                "hits", 2L, "cumulative_hits", 2L, // inc from the last check
                "inserts", 1L , "cumulative_inserts", 1L);

        if (true) {
            update(fromCore, commit(), new MapSolrParams(Map.of("post-processor", "refresh-join-caches")));
            assertUserCacheStats("fromCore", "size", 1,
                    "hits", 0L,
                    "cumulative_hits", 0L, // that's because it warms itself
                    "inserts", 0L ,
                    "cumulative_inserts", 0L); // that's because it warms itself
        } else { // commit into main index refreshes cache as well
            assertU(add(doc("id", "99999999")));
            assertU(commit()); // TODO this causes an error.
            // cached join
        }

        // fromCore change become visible
        for(long hits=1L; hits<=3L; hits++) {
            assertJQ(req(qOrFq(joinPrefix + " from=dept_id_s to=dept_s cacheEventually=true fromIndex=fromCore}cat:dev", "fl", "id",
                    "debugQuery", random().nextBoolean() ? "true" : "false"))
                    , "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'2'}]}"
            );
            // assert user cache has single new entry
            assertUserCacheStats("fromCore", "size", 1,
                    "hits", hits, "cumulative_hits", hits,
                    "inserts", 0L, "cumulative_inserts", 0L);
        }
        // we haven't changed "to" core ever
        assertEquals(toSearcherOpenTime, getSearcherOpenTime());
        assertNoFilterCacheHit();
    }

    private String[] qOrFq(String ... params)   {
        final ArrayList<String> modification = random().nextBoolean()?
                new ArrayList<>(Arrays.asList("q")) : new ArrayList<>(Arrays.asList("q", "*:*", "fq"));
        modification.addAll(Arrays.asList(params));
        return modification.toArray(new String[0]);
    }

    private void assertNoFilterCacheHit() throws IOException {
        h.getCore().withSearcher(s -> {
            final MetricsMap metricsMap = ((CaffeineCache) s.getFilterCache()).getMetricsMap();
            try {
                assertEquals(0, metricsMap.getAttribute("size"));
                assertEquals(0L, metricsMap.getAttribute("hits"));
                assertEquals(0L, metricsMap.getAttribute("cumulative_hits"));
                assertEquals(0L, metricsMap.getAttribute("inserts"));
                assertEquals(0L, metricsMap.getAttribute("cumulative_inserts"));
            return (Void)null;
            } catch (Exception e) {
                throw new IOException(e);
            }
        });
    }

    @AfterClass
    public static void nukeAll() {
        fromCore = null;
    }

    void assertUserCacheStats(String userCacheName, Object ... nameVals) throws IOException {
        h.getCore().withSearcher(s -> {
            final MetricsMap metricsMap = ((CaffeineCache) s.getCache(userCacheName)).getMetricsMap();
            try {
                for(int i=0; i<nameVals.length-1; i+=2) {
                    String name = (String) nameVals[i];
                    Object expected = nameVals[i + 1];
                    assertEquals(metricsMap.toString(), expected, metricsMap.getAttribute(name));
                }
                return (Void)null;
            } catch (Exception e) {
                throw new IOException(e);
            }
        });
    }

    @Test(expected = AssertionError.class)
    public void testForTest() throws IOException {
        assertUserCacheStats("fromCore", "size", Integer.MAX_VALUE);
    }

    long getSearcherOpenTime() throws IOException {
        long[] searcherStamp = new long[1];
        h.getCore().withSearcher(s -> {
            searcherStamp[0] = s.getOpenNanoTime();
            return (Void)null;
        });
        return searcherStamp[0];
    }
}
