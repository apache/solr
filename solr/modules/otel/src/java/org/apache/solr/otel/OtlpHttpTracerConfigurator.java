//package org.apache.solr.otel;
//
//import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
//import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
//import io.opentelemetry.sdk.trace.export.SpanExporter;
//
//public class OtlpHttpTracerConfigurator extends BaseOtelTracerConfigurator {
//
//  private static SpanExporter exporter = OtlpHttpSpanExporter.getDefault();
//  @Override
//  protected SpanExporter getExporter() {
//    return exporter;
//  }
//}
