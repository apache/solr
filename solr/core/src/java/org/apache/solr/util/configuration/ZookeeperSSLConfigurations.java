/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.util.configuration;

import java.security.KeyStore;
import java.util.Properties;
import org.apache.solr.client.solrj.impl.SolrHttpConstants;
import org.apache.solr.common.util.EnvUtils;
import org.apache.zookeeper.ClientCnxnSocketNetty;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.NettyServerCnxnFactory;

/** Derives ZooKeeper TLS defaults from Solr SSL settings, preserving ZooKeeper overrides. */
public final class ZookeeperSSLConfigurations {
  private ZookeeperSSLConfigurations() {}

  /**
   * Configures an embedded server's TLS listener without changing its configuration file. Unless
   * {@code secureClientPort} is already set, the plaintext {@code clientPort} becomes the secure
   * one.
   */
  public static void configureServer(Properties props) {
    String clientPort = props.getProperty("clientPort");
    if (props.getProperty("secureClientPort") == null && clientPort != null) {
      props.setProperty("secureClientPort", clientPort);
      props.remove("clientPort");
      props.remove("clientPortAddress");
    }
    setDefault(props, "serverCnxnFactory", NettyServerCnxnFactory.class.getName());
    SSLConfigurations ssl = SSLConfigurationsFactory.current();
    configureStore(
        props,
        "keyStore",
        EnvUtils.getProperty("solr.jetty.keystore", "./etc/solr-ssl.keystore.jks"),
        EnvUtils.getProperty("solr.jetty.keystore.type", "PKCS12"),
        ssl.getKeyStorePassword());
    boolean useSolrTrustStore =
        configureStore(
            props,
            "trustStore",
            EnvUtils.getProperty("solr.jetty.truststore", "./etc/solr-ssl.keystore.jks"),
            EnvUtils.getProperty("solr.jetty.truststore.type", "PKCS12"),
            ssl.getTrustStorePassword());
    configureContextSupplier(props, useSolrTrustStore);
    setDefault(props, "ssl.clientAuth", "none");
    setDefault(props, "client.portUnification", "false");
    setDefault(props, "ssl.hostnameVerification", peerNameVerification());
  }

  static void configureClient() {
    if (!EnvUtils.getPropertyAsBool(ZKClientConfig.SECURE_CLIENT, false)) {
      return;
    }
    Properties props = new Properties();
    setDefault(props, "client.secure", "true");
    setDefault(props, "clientCnxnSocket", ClientCnxnSocketNetty.class.getName());
    configureStore(
        props,
        "keyStore",
        EnvUtils.getProperty("javax.net.ssl.keyStore"),
        EnvUtils.getProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType()),
        EnvUtils.getProperty("javax.net.ssl.keyStorePassword"));
    boolean useSolrTrustStore =
        configureStore(
            props,
            "trustStore",
            EnvUtils.getProperty("javax.net.ssl.trustStore"),
            EnvUtils.getProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType()),
            EnvUtils.getProperty("javax.net.ssl.trustStorePassword"));
    configureContextSupplier(props, useSolrTrustStore);
    setDefault(props, "ssl.hostnameVerification", peerNameVerification());
    props.forEach((key, value) -> System.setProperty("zookeeper." + key, (String) value));
  }

  private static boolean configureStore(
      Properties props, String store, String location, String type, String password) {
    String prefix = "ssl." + store + ".";
    String configuredLocation = effective(props, prefix + "location", null);
    boolean useSolrStore = configuredLocation == null || configuredLocation.equals(location);
    setDefault(props, prefix + "location", location);
    setDefault(props, prefix + "type", useSolrStore ? type : null);
    setDefault(props, prefix + "passwordPath", null);
    if (props.getProperty(prefix + "passwordPath") == null) {
      setDefault(props, prefix + "password", useSolrStore ? password : null);
    }
    return useSolrStore && location != null;
  }

  private static void configureContextSupplier(Properties props, boolean useSolrTrustStore) {
    String provider = effective(props, "ssl.sslProvider", "JDK");
    boolean reverseDns =
        Boolean.parseBoolean(effective(props, "ssl.allowReverseDnsLookup", "false").trim());
    if (useSolrTrustStore && "JDK".equals(provider) && !reverseDns) {
      setDefault(props, "ssl.context.supplier.class", ZookeeperSSLContextSupplier.class.getName());
    }
  }

  /**
   * The value ZooKeeper will actually use for {@code key}: the explicit setting in {@code props},
   * else the {@code zookeeper.}-prefixed system property, else {@code defaultValue}.
   */
  private static String effective(Properties props, String key, String defaultValue) {
    return props.getProperty(key, EnvUtils.getProperty("zookeeper." + key, defaultValue));
  }

  private static void setDefault(Properties props, String key, String value) {
    String effectiveValue = effective(props, key, value);
    if (effectiveValue != null) {
      props.setProperty(key, effectiveValue);
    }
  }

  private static String peerNameVerification() {
    return Boolean.toString(
        EnvUtils.getPropertyAsBool(SolrHttpConstants.SYS_PROP_CHECK_PEER_NAME, true));
  }
}
