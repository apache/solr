Welcome to Apache Solr Jaeger Tracer Configurator
========

Apache Solr Jaeger Tracer Configurator (solr-jaegertracer) provides a way for you to expose Solr's tracing to Jaeger.

# Setup Jaeger Tracer Configurator

Add the solr-jaegertracer JAR file and the other JARs provided with this module to your Solr installation, ideally to each node.  
GSON is a dependency that is only used by Jaeger's "remote" sampler,
which is the default.  Solr doesn't distribute it, so you'll need to add GSON yourself or configure a different sampler.

The Jaeger Tracer Configurator is added to `solr.xml` like this:

```
<tracerConfig name="tracerConfig" class="org.apache.solr.jaeger.JaegerTracerConfigurator" />
```

There are no configuration elements in the XML; instead, this 3rd party system is configured using System Properties or Environment Variables.  The full list are listed at [Jaeger-README](https://github.com/jaegertracing/jaeger-client-java/blob/master/jaeger-core/README.md).
For example, to use the probabilistic sampler, you could set this environment variable:

```
export JAEGER_SAMPLER_TYPE=probabilistic
```

or System property:

```
bin/solr start -DJAEGER_SAMPLER_TYPE=probabilistic
```