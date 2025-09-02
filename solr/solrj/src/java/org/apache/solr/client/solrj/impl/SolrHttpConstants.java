package org.apache.solr.client.solrj.impl;

/**
 * Constants for HTTP Solr interaction.
 */
public interface SolrHttpConstants {
  int DEFAULT_CONNECT_TIMEOUT = 60000;
  int DEFAULT_SO_TIMEOUT = 600000;
  int DEFAULT_MAXCONNECTIONSPERHOST = 100000;
  int DEFAULT_MAXCONNECTIONS = 100000;

  /**
   * Socket timeout measured in ms, closes a socket if read takes longer than x ms to complete.
   * throws {@link java.net.SocketTimeoutException}: Read timed out exception
   */
  String PROP_SO_TIMEOUT = "socketTimeout";

  /**
   * connection timeout measures in ms, closes a socket if connection cannot be established within x
   * ms. with a {@link java.net.SocketTimeoutException}: Connection timed out
   */
  String PROP_CONNECTION_TIMEOUT = "connTimeout";

  /** Maximum connections allowed per host */
  String PROP_MAX_CONNECTIONS_PER_HOST = "maxConnectionsPerHost";

  /** Maximum total connections allowed */
  String PROP_MAX_CONNECTIONS = "maxConnections";

  /**
   * A Java system property to select the {@linkplain HttpClientBuilderFactory} used for configuring
   * HTTP based SolrClients.
   */
  String SYS_PROP_HTTP_CLIENT_BUILDER_FACTORY = "solr.httpclient.builder.factory";

  /**
   * System property consulted to determine if HTTP based SolrClients will require hostname
   * validation of SSL Certificates. The default behavior is to enforce peer name validation.
   */
  String SYS_PROP_CHECK_PEER_NAME = "solr.ssl.checkPeerName";

  /** Basic auth username */
  String PROP_BASIC_AUTH_USER = "httpBasicAuthUser";

  /** Basic auth password */
  String PROP_BASIC_AUTH_PASS = "httpBasicAuthPassword";
}
