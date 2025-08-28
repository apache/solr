package org.apache.solr.util.stats;

import io.opentelemetry.exporter.prometheus.PrometheusMetricReader;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.metrics.SolrMetricTestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class InstrumentedPoolingHttpClientConnectionManagerTest extends SolrTestCaseJ4 {

    private static InstrumentedPoolingHttpClientConnectionManager connectionManager;
    private PrometheusMetricReader metricsReader;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("solrconfig-minimal.xml", "schema.xml");
        h.getCoreContainer().waitForLoadingCoresToFinish(30000);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        connectionManager = (InstrumentedPoolingHttpClientConnectionManager) h.getCoreContainer()
                .getUpdateShardHandler()
                .getDefaultConnectionManager();
        metricsReader = SolrMetricTestUtils.getPrometheusMetricReader(h.getCoreContainer(), "solr.node");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (connectionManager != null) {
            connectionManager.close();
        }
        deleteCore();
    }

    @Test
    public void testInitializedMetrics() {
        assertGaugeMetricValueForType("available_connections", 0);
        assertGaugeMetricValueForType("leased_connections", 0);
        assertGaugeMetricValueForType("pending_connections", 0);
        // The overridden value of max connections for the connection pool via configuration
        assertGaugeMetricValueForType("max_connections", 100000);
    }

    @Test
    public void testLeasedConnectionsMetrics() throws Exception {
        HttpRoute route = new HttpRoute(new HttpHost("localhost", 8080));

        assertGaugeMetricValueForType("leased_connections", 0);

        HttpClientConnection conn = connectionManager
                .requestConnection(route, null)
                .get(1000, TimeUnit.MILLISECONDS);

        assertGaugeMetricValueForType("leased_connections", 1);

        connectionManager.releaseConnection(conn, null, 0, null);

        assertGaugeMetricValueForType("leased_connections", 0);
    }

    private void assertGaugeMetricValueForType(String metricType, double expectedValue) {
        Labels labels = Labels.builder()
                .label("category", "HTTP")
                .label("otel_scope_name", "org.apache.solr")
                .label("type", metricType)
                .build();
        GaugeSnapshot.GaugeDataPointSnapshot dataPointSnapshot = (GaugeSnapshot.GaugeDataPointSnapshot)
                SolrMetricTestUtils.getDataPointSnapshot(metricsReader, "solr_http_connection_pool", labels);
        assertNotNull(dataPointSnapshot);
        assertEquals(expectedValue, dataPointSnapshot.getValue(), 0.0);
    }
}
