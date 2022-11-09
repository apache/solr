Apache Solr Open Telemetry Tracer
=====================================

Introduction
------------
This module brings support for the new [OTEL](https://opentelemetry.io) standard,
and exposes an `OtlpGrpcTracerConfigurator` class that can be configured in the
`<tracerConfig>` tag of `solr.xml`, and will send traces over 
[OTLP protocol](https://opentelemetry.io/docs/reference/specification/protocol/).

See Reference guide for details.