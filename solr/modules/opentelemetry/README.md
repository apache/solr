Apache Solr Open Telemetry Tracer
=====================================

Introduction
------------
This module brings support for the new [OTEL](https://opentelemetry.io) standard,
and exposes a tracer configurator that can be enabled in the
`<tracerConfig>` tag of `solr.xml` as follows:

```xml

<tracerConfig name="tracerConfig" class="org.apache.solr.opentelemetry.OtelTracerConfigurator"/>
```

Configuration
-------------
The tracer can be configured through environment variables, see [OTEL SDK Environment Variables](https://opentelemetry.io/docs/reference/specification/sdk-environment-variables/).

The default configuration will ship trace using [OTLP](https://opentelemetry.io/docs/reference/specification/protocol/) over [gRPC](https://grpc.io), and will propagate trace IDs using [W3C TraceContext](https://www.w3.org/TR/trace-context/). Here are the default environment settings:  

```
OTEL_SDK_DISABLED=false
OTEL_SERVICE_NAME=solr
OTEL_TRACES_EXPORTER=otlp
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317
OTEL_TRACES_SAMPLER=parentbased_always_on
OTEL_PROPAGATORS=tracecontext,baggage
```

Consult the OTEL documentation for details on how to reconfigure. Here are some examples:

Jaeger with thrift/HTTP transport to collector:

```
OTEL_TRACES_EXPORTER=jaeger
OTEL_EXPORTER_JAEGER_PROTOCOL=http/thrift.binary
OTEL_EXPORTER_JAEGER_ENDPOINT=http://my-host:14268/api/traces
OTEL_PROPAGATORS=tracecontext,jaeger
```

Jaeger with binary thrift/UDP to agent on localhost port 6832:

```
OTEL_TRACES_EXPORTER=jaeger
OTEL_EXPORTER_JAEGER_PROTOCOL=udp/thrift.binary
OTEL_PROPAGATORS=tracecontext,jaeger
```

Zipkin with protobuf:

```
OTEL_TRACES_EXPORTER=zipkin
OTEL_EXPORTER_ZIPKIN_ENDPOINT=http://localhost:9411/api/v2/spans
```
