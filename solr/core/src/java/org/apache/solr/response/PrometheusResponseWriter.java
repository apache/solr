package org.apache.solr.response;

import io.prometheus.client.CollectorRegistry;

import io.prometheus.metrics.expositionformats.TextFormatUtil;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.w3c.dom.Text;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;

@SuppressWarnings (value="unchecked")
public class PrometheusResponseWriter extends RawResponseWriter {
    @Override
    public void write(OutputStream out, SolrQueryRequest request, SolrQueryResponse response)
            throws IOException {

        NamedList<Object> prometheusRegistries = (NamedList<Object>) response.getValues().get("metrics");
        Map<String, Object> registryMap = prometheusRegistries.asShallowMap();
        PrometheusTextFormatWriter prometheusTextFormatWriter = new PrometheusTextFormatWriter(true);
        registryMap.forEach((name, registry) -> {
            try {
                PrometheusRegistry prometheusRegistry = (PrometheusRegistry) registry;
                prometheusTextFormatWriter.write(out, prometheusRegistry.scrape());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }

}