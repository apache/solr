package org.apache.solr.util.stats;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.prometheus.PrometheusMetricReader;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
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
    private static final String METRIC_NAME = "solr_http_connection_pool";
    private static final String METRIC_TYPE_AVAILABLE_CONNECTIONS = "available_connections";
    private static final String METRIC_TYPE_MAX_CONNECTIONS = "max_connections";
    private static final String METRIC_TYPE_LEASED_CONNECTIONS = "leased_connections";
    private static final String METRIC_TYPE_PENDING_CONNECTIONS = "pending_connections";

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
        assertGaugeMetricValueForType(METRIC_TYPE_AVAILABLE_CONNECTIONS, 0);
        assertGaugeMetricValueForType(METRIC_TYPE_LEASED_CONNECTIONS, 0);
        assertGaugeMetricValueForType(METRIC_TYPE_PENDING_CONNECTIONS, 0);
        assertGaugeMetricValueForType(METRIC_TYPE_MAX_CONNECTIONS, DEFAULT_HTTP_MAX_CONNECTIONS);
    }

    @Test
    public void testConnectionPoolLeasedMetrics() throws Exception {
        HttpRoute route = new HttpRoute(new HttpHost("localhost", 8080));

        assertGaugeMetricValueForType(METRIC_TYPE_LEASED_CONNECTIONS, 0);

        HttpClientConnection conn = connectionManager
                .requestConnection(route, null)
                .get(1000, TimeUnit.MILLISECONDS);

        assertGaugeMetricValueForType(METRIC_TYPE_LEASED_CONNECTIONS, 1);

        connectionManager.releaseConnection(conn, null, 0, null);

        assertGaugeMetricValueForType(METRIC_TYPE_LEASED_CONNECTIONS, 0);
    }

    private void assertGaugeMetricValueForType(String metricType, double expectedValue) {
        Labels labels = Labels.builder()
                .label("category", "HTTP")
                .label("otel_scope_name", "org.apache.solr")
                .label("type", metricType)
                .build();
        GaugeSnapshot.GaugeDataPointSnapshot dataPointSnapshot = (GaugeSnapshot.GaugeDataPointSnapshot)
                SolrMetricTestUtils.getDataPointSnapshot(metricsReader, METRIC_NAME, labels);
        assertNotNull(dataPointSnapshot);
        assertEquals(expectedValue, dataPointSnapshot.getValue(), 0.0);
    }
}