package org.apache.solr.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.opentracingshim.OpenTracingShim;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.ResourceProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.opentracing.Tracer;
import org.apache.solr.core.TracerConfigurator;

import java.util.Map;
import java.util.function.Supplier;

public abstract class BaseOtelTracerConfigurator extends TracerConfigurator {
  Resource serviceNameResource =
      Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "solr"));

  @Override
  public Tracer getTracer() {
    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(getExporter()))
            .setResource(serviceNameResource.merge(Resource.getDefault()))
            .build();

    OpenTelemetrySdk openTelemetryOrig =
        OpenTelemetrySdk.builder()
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .setTracerProvider(tracerProvider)
            .build();

    OpenTelemetrySdk openTelemetry =
        AutoConfiguredOpenTelemetrySdk.builder()
            .addPropertiesSupplier(getSolrDefaultPropertiesSupplier())
            .addPropagatorCustomizer(
                (textMapPropagator, configProperties) -> textMapPropagator )
            .addTracerProviderCustomizer(
                (sdkTracerProviderBuilder, configProperties) -> sdkTracerProviderBuilder )
            .build()
            .getOpenTelemetrySdk();

    Runtime.getRuntime().addShutdownHook(new Thread(tracerProvider::close));

    return OpenTracingShim.createTracerShim(openTelemetry);

    // https://opentelemetry.io/docs/reference/specification/sdk-environment-variables/

    // OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
    // OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf  // grpc
    // OTEL_EXPORTER_OTLP_CERTIFICATE=
    // OTEL_EXPORTER_OTLP_CLIENT_KEY=
    // OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE

    // OTEL_SERVICE_NAME=solr
    // OTEL_PROPAGATORS=tracecontext
    // OTEL_TRACES_SAMPLER=parentbased_always_on
    // OTEL_TRACES_SAMPLER_ARG=
    // OTEL_EXPORTER_JAEGER_PROTOCOL=http/thrift.binary
    // OTEL_EXPORTER_JAEGER_ENDPOINT=
    // OTEL_EXPORTER_JAEGER_USER=
    // OTEL_EXPORTER_JAEGER_PASSWORD=
    // OTEL_EXPORTER_JAEGER_AGENT_HOST=localhost
    // OTEL_EXPORTER_JAEGER_AGENT_PORT=6831/6832
  }

  private Supplier<Map<String, String>> getSolrDefaultPropertiesSupplier() {
    return () -> Map.ofEntries(
        Map.entry(ResourceAttributes.SERVICE_NAME.getKey(), "solr"),
        Map.entry("propagators", "tracecontext"),
        Map.entry("traces.sampler", "parentbased_always_on"));
  }

  protected abstract SpanExporter getExporter();
}
