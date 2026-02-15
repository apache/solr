<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Solr Bootstrap Module

## Overview

The Bootstrap module is the foundation of **SIP-6 Phase 1**, which modernizes Solr's server startup process by replacing Jetty's traditional `start.jar` launcher with native Solr code that programmatically controls the entire bootstrap process. This gives Solr full ownership of server initialization, configuration, and lifecycle management.

## SIP-6 Context and Roadmap

### What is SIP-6?

SIP-6 (Solr Improvement Proposal 6) is a multi-phase initiative to modernize Solr's architecture and startup process. The Bootstrap module represents **Phase 1: Jetty Bootstrap Ownership**.

### Current Status: Phase 1

This module replaces Jetty's `start.jar` with native Solr bootstrap code, providing:
- Programmatic control over Jetty server initialization
- Java-based configuration instead of XML files
- Modern protocol support (HTTP/2, ALPN)
- Enhanced security features
- Automatic SSL certificate generation for development/testing

### Future Phases

While Phase 1 focuses on bootstrap ownership, future SIP-6 phases will continue modernizing Solr's architecture. This bootstrap module provides the foundation for those enhancements.

### Migration Path

The bootstrap module maintains backward compatibility with existing SSL configurations and system properties. Existing deployments can gradually adopt the new startup mechanism without breaking changes.

## What This Module Does

The Bootstrap module provides a complete Jetty server bootstrap implementation:

- **Replaces start.jar**: No longer dependent on Jetty's launcher; Solr controls the entire startup
- **Programmatic configuration**: All Jetty configuration via Java objects instead of XML files
- **Auto-SSL generation**: Automatically generates self-signed certificates for development and testing
- **Modern protocols**: HTTP/2, HTTP/2 Cleartext (h2c), and ALPN support
- **Security hardening**: Built-in security headers (CSP, X-Frame-Options, etc.)
- **Flexible configuration**: 80+ properties configurable via environment variables

## Key Differences from start.jar

| Aspect | Traditional (start.jar) | SIP-6 Bootstrap Module |
|--------|-------------------------|------------------------|
| **Bootstrap Control** | Jetty owns the process | Solr owns the process |
| **Configuration Method** | XML files (jetty.xml, etc.) | Java objects + system properties |
| **SSL Setup** | Manual certificate creation | Auto-generated for dev/test |
| **Protocol Support** | HTTP/1.1 primarily | HTTP/1.1 + HTTP/2 + ALPN |
| **Connector Setup** | XML configuration | `ConnectorFactory` (Java) |
| **Handler Chain** | jetty.xml + web.xml | `HandlerChainBuilder` (Java) |
| **Thread Pool** | XML configured | `ServerConfiguration` (Java) |
| **Security Headers** | Manual configuration | Built-in via `HandlerChainBuilder` |
| **Entry Point** | `java -jar start.jar` | `org.apache.solr.bootstrap.SolrStart` |

## Testing with bin/solr

The `bin/solr` script now starts Solr through the bootstrap module. The old script is found at `bin/solr-classic`.

### Basic Testing Examples

```bash
# Start Solr in HTTP mode (default)
bin/solr start

# Check that Solr is running
bin/solr status

# Stop Solr
bin/solr stop
```

### Testing SSL Auto-Generation

```bash
# Enable SSL with auto-generated certificate
SOLR_SSL_ENABLED=true bin/solr start

# Verify HTTPS endpoint (will show certificate details)
curl -vk https://localhost:8983/solr/admin/info/system

# Check generated certificate location (in $SOLR_PID_DIR)
ls -l $SOLR_PID_DIR/solr-ssl.keystore.p12
# Example: ls -l solr/packaging/build/dev-slim/server/logs/solr-ssl.keystore.p12

# Stop
bin/solr stop
```

## Module Architecture

### Core Classes

#### SolrStart
**Location**: `src/java/org/apache/solr/bootstrap/SolrStart.java`

Main entry point for the Solr bootstrap process.

**Key Responsibilities**:
- Install SLF4J bridge for java.util.logging
- Configure SSL (auto-generate or use user-provided keystores)
- Load server configuration from system properties
- Create and configure Jetty thread pool
- Create Jetty server instance with HTTP/HTTPS connectors
- Build handler chain (servlet context, filters, rewrite rules)
- Register shutdown hook for graceful termination
- Start server and wait for shutdown signal

**Main Method**: `main(String[] args)` orchestrates the entire bootstrap sequence.

#### SslCertificateGenerator
**Location**: `src/java/org/apache/solr/bootstrap/SslCertificateGenerator.java`

Handles automatic SSL certificate generation for development and testing environments.

**Key Responsibilities**:
- Determine if SSL should be enabled based on configuration
- Auto-generate self-signed certificates when appropriate
- Create PKCS12 keystores with Subject Alternative Names (SANs)
- Save certificates and passwords to disk
- Provide backward compatibility with existing SSL configurations

**Configuration Logic**:
1. Check if user provided keystores (backward compatibility)
2. Auto-enable SSL if keystores are present
3. If SSL enabled but no keystores provided, check `SOLR_SSL_GENERATE` (defaults to `true`)
4. Generate certificate or error if generation disabled

**Generated Certificate Details**:
- **Algorithm**: RSA 2048-bit
- **Validity**: 365 days
- **Subject**: `CN=localhost, OU=Solr Bootstrap, O=Apache Solr`
- **SANs**: Configurable via `SOLR_SSL_CERT_SAN` (default: `IP:127.0.0.1`)
- **Format**: PKCS12 keystore
- **Password**: Secure random 32-character password

#### ServerConfiguration
**Location**: `src/java/org/apache/solr/bootstrap/ServerConfiguration.java`

Encapsulates all Jetty server configuration as a Java object, replacing XML-based configuration.

**Key Responsibilities**:
- Parse system properties using `EnvUtils`
- Provide sensible defaults for all configuration values
- Support 80+ configurable properties including:
  - Core settings (port, host, directories)
  - Thread pool settings (min/max threads, idle timeout)
  - HTTP/HTTPS configuration
  - SSL/TLS settings
  - GZIP compression
  - IP access control (includes/excludes)
  - Request logging

**Factory Method**: `fromSystemProperties()` creates configuration from environment variables.

**Example Properties**:
- `solr.port.listen` (default: 8983)
- `solr.host.bind` (default: 127.0.0.1)
- `solr.jetty.threads.min` (default: 10)
- `solr.jetty.threads.max` (default: 10000)
- `solr.ssl.enabled` (auto-enabled if keystores present)

#### ConnectorFactory
**Location**: `src/java/org/apache/solr/bootstrap/ConnectorFactory.java`

Creates HTTP and HTTPS ServerConnectors with modern protocol support.

**Key Responsibilities**:
- Create HTTP connector with HTTP/2 cleartext (h2c) support
- Create HTTPS connector with HTTP/2 and ALPN (Application-Layer Protocol Negotiation)
- Configure SSL context factory with keystores and security settings
- Apply HTTP configuration (buffer sizes, timeouts, header sizes)
- Setup SSL keystore reload monitoring

**Protocol Support**:
- HTTP/1.1
- HTTP/2 (h2) over TLS
- HTTP/2 Cleartext (h2c) over plain HTTP
- ALPN for automatic protocol negotiation

**Cipher Configuration**: Uses `HTTP/2Cipher.COMPARATOR` for proper cipher ordering compatible with HTTP/2 requirements.

#### HandlerChainBuilder
**Location**: `src/java/org/apache/solr/bootstrap/HandlerChainBuilder.java`

Builds the complete Jetty handler chain for request processing.

**Key Responsibilities**:
- Create `ServletContextHandler` for Solr webapp
- Configure `CoreContainerProvider` and `SolrDispatchFilter`
- Add static file serving (`DefaultServlet`)
- Add security headers automatically
- Add URL rewrite rules (e.g., `/v2` and `/api` endpoints)
- Add IP-based access control
- Add GZIP compression handler

**Security Headers** (automatically applied):
- `Content-Security-Policy`: Strict policy to prevent XSS
- `X-Content-Type-Options`: `nosniff`
- `X-Frame-Options`: `SAMEORIGIN`
- `X-XSS-Protection`: `1; mode=block`

**Handler Chain Order** (innermost to outermost):
1. `ServletContextHandler` - Core Solr application
2. `GzipHandler` - Response compression
3. `InetAccessHandler` - IP filtering
4. `RewriteHandler` - Security headers + URL rewrites

#### SslReloadHandler
**Location**: `src/java/org/apache/solr/bootstrap/SslReloadHandler.java`

Handles SSL certificate reload without server restart.

**Key Responsibilities**:
- Monitor keystore file for changes using Jetty's `KeyStoreScanner`
- Automatically reload SSL context when certificate is updated
- Enable/disable via `SOLR_KEYSTORE_RELOAD_ENABLED` (default: `true`)
- Configurable scan interval (default: 60 seconds)

This replaces the `--module=ssl-reload` functionality from the old startup approach.

## SSL Auto-Generation Capabilities

### Overview

The Bootstrap module includes intelligent SSL certificate auto-generation, making it trivial to run Solr with HTTPS in development and testing environments.

### When Auto-Generation Activates

SSL auto-generation follows this decision tree:

```
┌─ User provided keystores? ────────────────────────────┐
│  (SOLR_SSL_KEY_STORE or SOLR_SSL_TRUST_STORE set)     │
│                                                        │
│  YES ──> Auto-enable SSL (backward compatibility)     │
│          Use provided keystores                       │
│                                                        │
│  NO ──> Check SOLR_SSL_ENABLED                        │
│         │                                              │
│         ├─ false or unset ──> HTTP mode (no SSL)      │
│         │                                              │
│         └─ true ──> Check SOLR_SSL_GENERATE           │
│                     │                                  │
│                     ├─ true (DEFAULT) ──> Generate    │
│                     │                     certificate │
│                     │                                  │
│                     └─ false ──> ERROR: SSL enabled   │
│                                   but no keystore     │
└────────────────────────────────────────────────────────┘
```

### Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `SOLR_SSL_ENABLED` | Enable/disable SSL | Auto-enabled if keystores present |
| `SOLR_SSL_GENERATE` | Auto-generate certificate if SSL enabled | `true` |
| `SOLR_SSL_KEY_STORE` | Path to keystore | `$SOLR_PID_DIR/solr-ssl.keystore.p12` (typically `bin/`) |
| `SOLR_SSL_TRUST_STORE` | Path to truststore | Same as keystore |
| `SOLR_SSL_CERT_SAN` | Subject Alternative Names | `IP:127.0.0.1` |
| `SOLR_KEYSTORE_RELOAD_ENABLED` | Auto-reload on cert change | `true` |
| `SOLR_KEYSTORE_RELOAD_INTERVAL` | Scan interval (seconds) | `60` |

### Certificate Storage

Generated certificates are saved to:
- **Keystore**: `$SOLR_PID_DIR/solr-ssl.keystore.p12` (typically in the `bin/` directory, or custom path via `SOLR_SSL_KEY_STORE`)
- **Password**: Stored in system properties and logged on first generation

The password is a cryptographically secure random 32-character string. Using `$SOLR_PID_DIR` (which defaults to `bin/`) keeps auto-generated certificates with other runtime files (like PID files) rather than polluting `$SOLR_HOME` with temporary artifacts.

### Custom Subject Alternative Names (SANs)

For multi-interface or custom domain testing, configure SANs:

```bash
export SOLR_SSL_CERT_SAN="DNS:localhost,DNS:solr.local,IP:127.0.0.1,IP:192.168.1.100"
```

### Certificate Reuse

Once generated, certificates are reused on subsequent starts unless:
- The keystore file is deleted
- A different keystore path is specified
- The certificate expires (365 days)

### Auto-Reload

If enabled (default), the module monitors the keystore file for changes and automatically reloads the SSL context when the certificate is updated - no server restart required.

## Configuration

The bootstrap module supports extensive configuration through environment variables. All traditional `SOLR_*` properties continue to work.

### Common Environment Variables

```bash
# Core settings
SOLR_PORT_LISTEN=9000
SOLR_HOST_BIND=127.0.0.1
SOLR_HOME=/path/to/solr/home

# SSL settings (see SSL section above)
SOLR_SSL_ENABLED=true
SOLR_SSL_GENERATE=true

# Thread pool
SOLR_JETTY_THREADS_MIN=20
SOLR_JETTY_THREADS_MAX=5000
SOLR_JETTY_THREADS_IDLE_TIMEOUT=120000

# JVM
SOLR_HEAP=2g
SOLR_JAVA_HOME=/path/to/java21

# GZIP compression
SOLR_JETTY_GZIP_ENABLED=true
SOLR_JETTY_GZIP_MIN_SIZE=2048

# IP access control
SOLR_JETTY_INETACCESS_INCLUDES=127.0.0.1,192.168.1.0/24
SOLR_JETTY_INETACCESS_EXCLUDES=
```

For a complete list of configurable properties, see `ServerConfiguration.java`.

## Current Status and Next Steps

### Implementation Status

- **Phase 1: Complete** - Bootstrap module fully implemented
- **Testing**: Using temporary `bin/solr` script
- **Integration**: Pending integration into main `bin/solr` script

### Testing and Validation

Committers are encouraged to test the bootstrap module using `bin/solr` and report any issues. The module includes comprehensive unit tests in:
- `src/test/org/apache/solr/bootstrap/SslCertificateGeneratorTest.java`
- `src/test/org/apache/solr/bootstrap/ServerConfigurationTest.java`

### Future Integration

Once validated, the bootstrap module will be integrated into the main `bin/solr` startup script, and `bin/solr` will be removed. The transition will be seamless for users, maintaining backward compatibility with existing configurations.

### SIP-6 Next Phases

While this module represents Phase 1 of SIP-6, future phases will build upon this foundation to further modernize Solr's architecture. The programmatic bootstrap control provided here enables those future enhancements.

## See Also

- SIP-6 Documentation (link to JIRA or wiki once available)
- Jetty 12 Documentation: https://eclipse.dev/jetty/documentation/jetty-12/
- Solr Reference Guide: SSL/TLS Configuration
