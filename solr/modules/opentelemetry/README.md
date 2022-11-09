Apache Solr Open Telemetry Tracer
=====================================

Introduction
------------
This module brings support for the new [OTEL](https://opentelemetry.io) standard,
and exposes a tracer configurator that can be enabled in the
`<tracerConfig>` tag of `solr.xml`:

```xml

<tracerConfig name="tracerConfig" class="org.apache.solr.opentelemetry.OtelTracerConfigurator"/>
```

The tracer can be configured with environment variables, see https://opentelemetry.io/docs/concepts/sdk-configuration/otlp-exporter-configuration/ and users can change both the exprter, trace propagator and many other settings.

The defaults are: 

```
OTEL_SDK_DISABLED=false
OTEL_SERVICE_NAME=solr
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_TRACES_SAMPLER=parentbased_always_on
OTEL_TRACES_SAMPLER_ARG=
OTEL_PROPAGATORS=tracecontext,baggage
OTEL_TRACES_EXPORTER=otlp

OTEL_EXPORTER_OTLP_CERTIFICATE=
OTEL_EXPORTER_OTLP_CLIENT_KEY=
OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE
```

TODO: Document in RefGuide. Provide examples for env.variables for e.g. jaeger exporter and other common cases.