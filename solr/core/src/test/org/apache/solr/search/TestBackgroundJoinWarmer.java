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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

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
    public void testLuceneJoin() throws Exception {
        doTestJoin("{!joinTest "+(random().nextBoolean() ? "score=none" : ""));
    }

    void doTestJoin(String joinPrefix) throws Exception {
        BackgroundJoinWarmerQP.inserts.set(0);
        CrossCoreJoinCacheTestReg.bypass.set(0);
        CrossCoreJoinCacheTestReg.regenerate.set(0);

        long toSearcherOpenTime = getSearcherOpenTime();
        assertUserCacheStats("fromCore", "size", 0);
        assertTrue(toSearcherOpenTime>0L);
        assertEquals(0, BackgroundJoinWarmerQP.inserts.get());
        for(long hits=0L; hits<2L; hits++) {
            assertJQ(req(qOrFq(joinPrefix + " from=dept_id_s to=dept_s cacheEventually=true fromIndex=fromCore}cat:dev", "fl", "id"//,
                    /*"debugQuery", random().nextBoolean() ? "true" : "false"*/))
                    , "/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'},{'id':'5'}]}"
            );
            assertEquals(1, BackgroundJoinWarmerQP.inserts.get());
            // assert user cache hit
            assertUserCacheStats("fromCore", "size", 1,
                    "hits", hits, "cumulative_hits", hits, //0, 1
                    "inserts", 1L , "cumulative_inserts", 1L);
        }
        update(fromCore, delQ("id:10"));
        update(fromCore, add(doc("id", "10", "id_s_dv", "10", "dept_id_s", "Marketing",
                "text", "now they go on market", "cat", "dev")));
        update(fromCore, commit());
        // change in fromCore isn't visible due to user cache cache
        assertJQ(req(qOrFq( joinPrefix + " from=dept_id_s to=dept_s cacheEventually=true fromIndex=fromCore}cat:dev", "fl", "id"/*,
                "debugQuery", random().nextBoolean() ? "true" : "false"*/))
                , "/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'},{'id':'5'}]}"
        );
        // nothing have changed ...
        long cumulative_hits = 2L;
        long cumulative_inserts = 1L;
        assertUserCacheStats("fromCore", "size", 1,
                "hits", 2L, "cumulative_hits", cumulative_hits,
                "inserts", 1L, "cumulative_inserts", cumulative_inserts);
        assertEquals("entry inserted once",     1,BackgroundJoinWarmerQP.inserts.get());
        assertEquals("no regeneration attempts",0,CrossCoreJoinCacheTestReg.bypass.get());
        assertEquals("no regenerations yet",    0,CrossCoreJoinCacheTestReg.regenerate.get());
        if (random().nextBoolean()) {
            // refresh explicitly
            update(fromCore, commit(), new MapSolrParams(Map.of("post-processor", "refresh-join-caches")));
            assertUserCacheStats("fromCore", "size", 1, // we have entry
                    "hits", 0L,
                    "cumulative_hits", cumulative_hits=0L, // forgets stats
                    "inserts", 0L ,
                    "cumulative_inserts", cumulative_inserts=0L); // forgets stats
        } else { // commit into main index regenerates cache as well
            assertU(add(doc("id", "99999999")));
            assertU(commit());
            toSearcherOpenTime = getSearcherOpenTime();
            assertUserCacheStats("fromCore", "size", 1,
                    "hits", 0L,
                    "cumulative_hits", cumulative_hits, // that's because it warms itself
                    "inserts", 0L ,
                    "cumulative_inserts", cumulative_inserts);
        }
        assertEquals(0,CrossCoreJoinCacheTestReg.bypass.get());
        assertEquals(1,CrossCoreJoinCacheTestReg.regenerate.get());
        // fromCore change become visible
        for(long hits=1L; hits<=3L; hits++) {
            assertJQ(req(qOrFq(joinPrefix + " from=dept_id_s to=dept_s cacheEventually=true fromIndex=fromCore}cat:dev", "fl", "id",
                    "debugQuery", random().nextBoolean() ? "true" : "false"))
                    , "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'2'}]}"
            );
            // assert user cache has single new entry
            assertUserCacheStats("fromCore", "size", 1,
                    "hits", hits, "cumulative_hits", hits+cumulative_hits,
                    "inserts", 0L, "cumulative_inserts", cumulative_inserts);
        }
        // ignore refresh
        update(fromCore, commit(), new MapSolrParams(Map.of("post-processor", "refresh-join-caches")));
        assertEquals(1,CrossCoreJoinCacheTestReg.bypass.get());
        assertEquals(1,CrossCoreJoinCacheTestReg.regenerate.get());

        // we haven't changed "to" core ever, unless we change it
        assertEquals(toSearcherOpenTime, getSearcherOpenTime());
        assertNoFilterCacheHit();
    }

    private String[] qOrFq(String ... params)   {
        final ArrayList<String> modification = //random().nextBoolean()?
                 //new ArrayList<>(Arrays.asList("q")) ;//:
                new ArrayList<>(Arrays.asList("q", "*:*", "fq"));
        modification.addAll(Arrays.asList(params));
        return modification.toArray(new String[0]);
    }

    private void assertNoFilterCacheHit() throws IOException {
        assertCacheStats((s)-> (CaffeineCache) s.getFilterCache(),"size", 0 ,
                "hits",0L, "cumulative_hits", 0L,"inserts", 0L ,"cumulative_inserts", 0L);
    }

    @AfterClass
    public static void nukeAll() {
        fromCore = null;
    }

    void assertUserCacheStats(String userCacheName, Object ... nameVals) throws IOException {
        assertCacheStats((s)-> (CaffeineCache) s.getCache(userCacheName), nameVals);
    }

    private void assertCacheStats(Function<SolrIndexSearcher, CaffeineCache<?,?>> cacheAccessor, Object ... nameVals)
            throws IOException {
        h.getCore().withSearcher(s -> {
            final CaffeineCache<?,?> cache = cacheAccessor.apply(s);
            final MetricsMap metricsMap = cache.getMetricsMap();
            System.out.println(metricsMap);
            try {
                for(int i=0; i<nameVals.length-1; i+=2) {
                    String name = (String) nameVals[i];
                    Object expected = nameVals[i + 1];
                    assertEquals(name, expected, metricsMap.getAttribute(name));
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
