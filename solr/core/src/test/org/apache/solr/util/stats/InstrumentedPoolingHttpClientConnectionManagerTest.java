package org.apache.solr.util.stats;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.prometheus.PrometheusMetricReader;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricTestUtils;
import org.apache.solr.metrics.SolrMetricsContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class InstrumentedPoolingHttpClientConnectionManagerTest extends SolrTestCaseJ4 {

    private InstrumentedPoolingHttpClientConnectionManager connectionManager;
    private PrometheusMetricReader metricsReader;

    private static final int DEFAULT_HTTP_MAX_CONNECTIONS = 20;
    private static final String METRIC_MAX_CONNECTIONS = "solr_http_connection_pool_max_connections";
    private static final String METRIC_AVAILABLE_CONNECTIONS = "solr_http_connection_pool_available_connections";
    private static final String METRIC_LEASED_CONNECTIONS = "solr_http_connection_pool_leased_connections";
    private static final String METRIC_PENDING_REQUESTS = "solr_http_connection_pool_pending_requests";

    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("solrconfig-minimal.xml", "schema.xml");
        h.getCoreContainer().waitForLoadingCoresToFinish(30000);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        Registry<ConnectionSocketFactory > socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.INSTANCE)
                .build();

        connectionManager = new InstrumentedPoolingHttpClientConnectionManager(socketFactoryRegistry);

        SolrMetricsContext solrMetricsContext = h.getCoreContainer()
                .getMetricsHandler()
                .getSolrMetricsContext();

        Attributes attributes = Attributes.builder()
                .put(SolrMetricProducer.CATEGORY_ATTR, SolrInfoBean.Category.HTTP.toString())
                .build();

        connectionManager.initializeMetrics(solrMetricsContext, attributes, "test");
        metricsReader = SolrMetricTestUtils.getPrometheusMetricReader(h.getCoreContainer(), "solr.node");
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (connectionManager != null) {
            connectionManager.close();
        }
        super.tearDown();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        deleteCore();
    }

    @Test
    public void testInitializedMetrics() {
        assertGaugeMetricValue(METRIC_AVAILABLE_CONNECTIONS, 0);
        assertGaugeMetricValue(METRIC_LEASED_CONNECTIONS, 0);
        assertGaugeMetricValue(METRIC_PENDING_REQUESTS, 0);
        assertGaugeMetricValue(METRIC_MAX_CONNECTIONS, DEFAULT_HTTP_MAX_CONNECTIONS);
    }

    @Test
    public void testConnectionPoolLeasedMetrics() throws Exception {
        HttpRoute route = new HttpRoute(new HttpHost("localhost", 8080));

        assertGaugeMetricValue(METRIC_LEASED_CONNECTIONS, 0);

        HttpClientConnection conn = connectionManager
                .requestConnection(route, null)
                .get(1000, TimeUnit.MILLISECONDS);

        assertGaugeMetricValue(METRIC_LEASED_CONNECTIONS, 1);

        connectionManager.releaseConnection(conn, null, 0, null);

        assertGaugeMetricValue(METRIC_LEASED_CONNECTIONS, 0);
    }

    private void assertGaugeMetricValue(String metricName, double expectedValue) {
        GaugeSnapshot.GaugeDataPointSnapshot dataPointSnapshot = (GaugeSnapshot.GaugeDataPointSnapshot)
                SolrMetricTestUtils.getDataPointSnapshot(metricsReader, metricName);
        assertNotNull(dataPointSnapshot);
        assertEquals(expectedValue, dataPointSnapshot.getValue(), 0.0);
    }
}