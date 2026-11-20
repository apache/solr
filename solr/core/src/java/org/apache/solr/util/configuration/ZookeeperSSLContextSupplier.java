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

import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.CertPathValidator;
import java.security.cert.CertificateException;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.PKIXRevocationChecker;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.EnumSet;
import java.util.function.Supplier;
import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.common.ZKConfig;

/** Supplies ZooKeeper TLS contexts using Solr-compatible trust material. */
public class ZookeeperSSLContextSupplier implements Supplier<SSLContext> {
  @Override
  public SSLContext get() {
    try (ClientX509Util x509 = new ClientX509Util()) {
      ZKConfig config = new ZKConfig();
      KeyManager[] keys = null;
      String location = config.getProperty(x509.getSslKeystoreLocationProperty(), "");
      if (!location.isEmpty()) {
        keys =
            new KeyManager[] {
              X509Util.createKeyManager(
                  location,
                  x509.getPasswordFromConfigPropertyOrFile(
                      config,
                      x509.getSslKeystorePasswdProperty(),
                      x509.getSslKeystorePasswdPathProperty()),
                  config.getProperty(x509.getSslKeystoreTypeProperty()))
            };
      }
      SSLContext context =
          SSLContext.getInstance(
              config.getProperty(
                  x509.getSslProtocolProperty(), X509Util.defaultTlsProtocol(config)));
      context.init(keys, new TrustManager[] {trustManager(x509, config)}, null);
      return context;
    } catch (IOException | GeneralSecurityException | X509Exception e) {
      throw new IllegalStateException("Could not create ZooKeeper SSL context", e);
    }
  }

  private static X509ExtendedTrustManager trustManager(ClientX509Util x509, ZKConfig config)
      throws IOException, GeneralSecurityException {
    KeyStore store = null;
    String location = config.getProperty(x509.getSslTruststoreLocationProperty(), "");
    if (!location.isEmpty()) {
      store =
          X509Util.loadTrustStore(
              location,
              x509.getPasswordFromConfigPropertyOrFile(
                  config,
                  x509.getSslTruststorePasswdProperty(),
                  x509.getSslTruststorePasswdPathProperty()),
              config.getProperty(x509.getSslTruststoreTypeProperty()));
    }
    TrustManagerFactory factory = TrustManagerFactory.getInstance("PKIX");
    boolean crl = config.getBoolean(x509.getSslCrlEnabledProperty());
    boolean ocsp = config.getBoolean(x509.getSslOcspEnabledProperty());
    if (store != null && (crl || ocsp)) {
      KeyStore certificates = KeyStore.getInstance(KeyStore.getDefaultType());
      certificates.load(null, null);
      for (String alias : Collections.list(store.aliases())) {
        var certificate = store.getCertificate(alias);
        if (certificate != null) certificates.setCertificateEntry(alias, certificate);
      }
      PKIXBuilderParameters parameters =
          new PKIXBuilderParameters(certificates, new X509CertSelector());
      PKIXRevocationChecker checker =
          (PKIXRevocationChecker) CertPathValidator.getInstance("PKIX").getRevocationChecker();
      var options = EnumSet.noneOf(PKIXRevocationChecker.Option.class);
      if (!ocsp) {
        options.add(PKIXRevocationChecker.Option.NO_FALLBACK);
        options.add(PKIXRevocationChecker.Option.PREFER_CRLS);
      }
      if (Boolean.parseBoolean(
          Security.getProperty("com.sun.security.onlyCheckRevocationOfEECert"))) {
        options.add(PKIXRevocationChecker.Option.ONLY_END_ENTITY);
      }
      checker.setOptions(options);
      parameters.addCertPathChecker(checker);
      factory.init(new CertPathTrustManagerParameters(parameters));
    } else {
      factory.init(store);
    }
    for (TrustManager manager : factory.getTrustManagers()) {
      if (manager instanceof X509ExtendedTrustManager extended) {
        return X509Util.getFipsMode(config)
            ? extended
            : new HostnameVerifyingTrustManager(
                extended,
                x509.isServerHostnameVerificationEnabled(config),
                x509.isClientHostnameVerificationEnabled(config));
      }
    }
    throw new GeneralSecurityException("No X509ExtendedTrustManager available");
  }

  private static final class HostnameVerifyingTrustManager extends X509ExtendedTrustManager {
    private final X509ExtendedTrustManager delegate;
    private final boolean verifyServer;
    private final boolean verifyClient;

    private HostnameVerifyingTrustManager(
        X509ExtendedTrustManager delegate, boolean verifyServer, boolean verifyClient) {
      this.delegate = delegate;
      this.verifyServer = verifyServer;
      this.verifyClient = verifyClient;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
        throws CertificateException {
      identify(socket, verifyClient);
      delegate.checkClientTrusted(chain, authType, socket);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
        throws CertificateException {
      identify(socket, verifyServer);
      delegate.checkServerTrusted(chain, authType, socket);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
        throws CertificateException {
      identify(engine, verifyClient);
      delegate.checkClientTrusted(chain, authType, engine);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
        throws CertificateException {
      identify(engine, verifyServer);
      delegate.checkServerTrusted(chain, authType, engine);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      delegate.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      delegate.checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return delegate.getAcceptedIssuers();
    }

    private static void identify(Socket socket, boolean enabled) {
      if (enabled && socket instanceof SSLSocket ssl) {
        var parameters = ssl.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        ssl.setSSLParameters(parameters);
      }
    }

    private static void identify(SSLEngine engine, boolean enabled) {
      if (enabled && engine != null) {
        var parameters = engine.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(parameters);
      }
    }
  }
}
