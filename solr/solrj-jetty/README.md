# SolrJ Jetty Module

This module provides Jetty-based HTTP client implementations for Apache Solr.

## Overview

The `solrj-jetty` artifact contains HTTP clients built on Eclipse Jetty's HTTP client library,
offering robust HTTP/1.1 and HTTP/2 support with advanced features.

## Included Clients

- **HttpJettySolrClient** - Direct HTTP client using Jetty (HTTP/1.1 and HTTP/2)
- **CloudJettySolrClient** - SolrCloud-aware client using Jetty
- **LBJettySolrClient** - Load-balancing client using Jetty
- **ConcurrentUpdateJettySolrClient** - High-throughput update client using Jetty

## When to Use

Use this module when:
- You need HTTP/2 support
- You need advanced connection pooling and management
- You need SSL/TLS with custom configurations
- You need the most mature and tested Solr client implementation

## Alternative: JDK-based Clients

If you want to avoid the Jetty dependency, use JDK-based clients from `solr-solrj`:
- **HttpJdkSolrClient** - Uses Java's built-in HttpClient (Java 11+)
- **CloudHttp2SolrClient** - Automatically falls back to JDK client when Jetty not available

## Maven Dependency

```xml
<dependency>
    <groupId>org.apache.solr</groupId>
    <artifactId>solrj-jetty</artifactId>
    <version>${solr.version}</version>
</dependency>
```

## Gradle Dependency

```gradle
implementation 'org.apache.solr:solrj-jetty:${solrVersion}'
```

## Migration from Solr 10.0

Starting with Solr 10.1, Jetty-based clients have been separated into this module.

### Before (Solr 10.0 and earlier)
```gradle
dependencies {
    implementation 'org.apache.solr:solr-solrj:10.0.0'
}
```

### After (Solr 10.1+)
```gradle
dependencies {
    implementation 'org.apache.solr:solr-solrj:10.1.0'
    implementation 'org.apache.solr:solrj-jetty:10.1.0'  // Add this
}
```

### Code Changes

No code changes are required - all class names and packages remain the same.

### Automatic Fallback

`CloudSolrClient` automatically falls back to JDK-based clients when `solrj-jetty` is not on the classpath,
so applications can work with `solr-solrj` alone if desired.
