package org.apache.solr;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.AbstractSolrEventListener;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.CaffeineCache;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.servlet.DirectSolrConnection;
import org.apache.solr.util.IOFunction;
import org.apache.solr.util.RefCounted;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;

public class TestBackgroundJoinWarmer extends SolrTestCaseJ4 {

        private static SolrCore fromCore;

        @BeforeClass
        public static void beforeTests() throws Exception {
            System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
            System.setProperty("solr.filterCache.async", "true");
//    initCore("solrconfig.xml","schema12.xml");

            // File testHome = createTempDir().toFile();
            // FileUtils.copyDirectory(getFile("solrj/solr"), testHome);
            initCore("solrconfig-crosscore-join-cache.xml", "schema12.xml", TEST_HOME(), "collection1");
            final CoreContainer coreContainer = h.getCoreContainer();

            fromCore = coreContainer.create("fromCore", ImmutableMap.of("configSet", "minimal"));

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
            DirectSolrConnection connection = new DirectSolrConnection(core);
            SolrRequestHandler handler = core.getRequestHandler("/update");
            return connection.request(handler, null, xml);
        }

        @Test
        public void testJoin() throws Exception {
            fromCore.registerNewSearcherListener(new AbstractSolrEventListener(fromCore) {

                @Override
                public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
                    final SolrCore rightSideCore = this.getCore();
                    final List<String> loadedCoreNames = rightSideCore.getCoreContainer().getLoadedCoreNames();
                    for (String leftCoreName: loadedCoreNames){
                        if (!leftCoreName.equals(rightSideCore.getName())) {
                            final RefCounted<SolrIndexSearcher> refSearcher = rightSideCore.getCoreContainer().getCore(leftCoreName).getSearcher();
                            try {
                                final SolrCache joinCache = refSearcher.get().getCache(rightSideCore.getName());
                                joinCache.warm(refSearcher.get(), joinCache);
                                joinCache.reload();
                            } finally {
                                refSearcher.decref();
                            }
                        }
                    }
                }
            });
            doTestJoin("{!join");
        }

        public String query(SolrCore core, SolrQueryRequest req) throws Exception {
            String handler = "standard";
            if (req.getParams().get("qt") != null) {
                handler = req.getParams().get("qt");
            }
            if (req.getParams().get("wt") == null){
                ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
                params.set("wt", "xml");
                req.setParams(params);
            }
            SolrQueryResponse rsp = new SolrQueryResponse();
            SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
            core.execute(core.getRequestHandler(handler), req, rsp);
            if (rsp.getException() != null) {
                throw rsp.getException();
            }
            StringWriter sw = new StringWriter(32000);
            QueryResponseWriter responseWriter = core.getQueryResponseWriter(req);
            responseWriter.write(sw, req, rsp);
            req.close();
            SolrRequestInfo.clearRequestInfo();
            return sw.toString();
        }

        @AfterClass
        public static void nukeAll() {
            fromCore = null;
        }
}
