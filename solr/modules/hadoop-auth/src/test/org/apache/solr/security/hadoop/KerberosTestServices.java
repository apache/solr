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
package org.apache.solr.security.hadoop;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.net.BindException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import org.apache.commons.io.file.PathUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosTestServices {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile MiniKdc kdc;
  private final JaasConfiguration jaasConfiguration;
  private final Configuration savedConfig;
  private volatile Locale savedLocale;
  private final boolean debug;

  private final File workDir;

  private KerberosTestServices(
      File workDir, JaasConfiguration jaasConfiguration, Configuration savedConfig, boolean debug) {
    this.jaasConfiguration = jaasConfiguration;
    this.savedConfig = savedConfig;
    this.workDir = workDir;
    this.debug = debug;
  }

  public MiniKdc getKdc() {
    return kdc;
  }

  public void start() throws Exception {
    if (incompatibleLanguagesWithMiniKdc.contains(Locale.getDefault().getLanguage())) {
      savedLocale = Locale.getDefault();
      Locale.setDefault(Locale.US);
    }

    // There is time lag between selecting a port and trying to bind with it. It's possible that
    // another service captures the port in between which'll result in BindException.
    boolean bindException;
    int numTries = 0;
    do {
      try {
        bindException = false;

        kdc = getKdc(workDir, debug);
        kdc.start();
      } catch (BindException e) {
        PathUtils.deleteDirectory(workDir.toPath()); // clean directory
        numTries++;
        if (numTries == 3) {
          log.error("Failed setting up MiniKDC. Tried {} times.", numTries);
          throw e;
        }
        log.error("BindException encountered when setting up MiniKdc. Trying again.");
        bindException = true;
      }
    } while (bindException);

    Configuration.setConfiguration(jaasConfiguration);
    Krb5HttpClientBuilder.regenerateJaasConfiguration();
  }

  public void stop() {
    if (kdc != null) kdc.stop();
    Configuration.setConfiguration(savedConfig);
    Krb5HttpClientBuilder.regenerateJaasConfiguration();
    if (savedLocale != null) Locale.setDefault(savedLocale);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a MiniKdc that can be used for creating kerberos principals and keytabs. Caller is
   * responsible for starting/stopping the kdc.
   */
  private static MiniKdc getKdc(File workDir, boolean debug) throws Exception {
    Properties conf = MiniKdc.createConf();
    conf.setProperty("kdc.port", "0");
    conf.setProperty("debug", String.valueOf(debug));
    return new MiniKdc(conf, workDir);
  }

  /**
   * Programmatic version of a jaas.conf file suitable for connecting to a SASL-configured
   * zookeeper.
   */
  private static class JaasConfiguration extends Configuration {

    private static AppConfigurationEntry[] clientEntry;
    private static AppConfigurationEntry[] serverEntry;
    private String clientAppName = "Client", serverAppName = "Server";

    /**
     * Add an entry to the jaas configuration with the passed in name, principal, and keytab. The
     * other necessary options will be set for you.
     *
     * @param clientPrincipal The principal of the client
     * @param clientKeytab The location of the keytab with the clientPrincipal
     * @param serverPrincipal The principal of the server
     * @param serverKeytab The location of the keytab with the serverPrincipal
     */
    public JaasConfiguration(
        String clientPrincipal, File clientKeytab, String serverPrincipal, File serverKeytab) {
      Map<String, String> clientOptions = new HashMap<>();
      clientOptions.put("principal", clientPrincipal);
      clientOptions.put("keyTab", clientKeytab.getAbsolutePath());
      clientOptions.put("useKeyTab", "true");
      clientOptions.put("storeKey", "true");
      clientOptions.put("useTicketCache", "false");
      clientOptions.put("refreshKrb5Config", "true");
      String jaasProp = System.getProperty("solr.jaas.debug");
      if (jaasProp != null && "true".equalsIgnoreCase(jaasProp)) {
        clientOptions.put("debug", "true");
      }
      clientEntry =
          new AppConfigurationEntry[] {
            new AppConfigurationEntry(
                krb5LoginModuleName,
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                clientOptions)
          };
      if (serverPrincipal != null && serverKeytab != null) {
        Map<String, String> serverOptions = new HashMap<>(clientOptions);
        serverOptions.put("principal", serverPrincipal);
        serverOptions.put("keytab", serverKeytab.getAbsolutePath());
        serverEntry =
            new AppConfigurationEntry[] {
              new AppConfigurationEntry(
                  krb5LoginModuleName,
                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                  serverOptions)
            };
      }
    }

    /**
     * Add an entry to the jaas configuration with the passed in principal and keytab, along with
     * the app name.
     *
     * @param principal The principal
     * @param keytab The keytab containing credentials for the principal
     * @param appName The app name of the configuration
     */
    public JaasConfiguration(String principal, File keytab, String appName) {
      this(principal, keytab, null, null);
      clientAppName = appName;
      serverAppName = null;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      if (name.equals(clientAppName)) {
        return clientEntry;
      } else if (name.equals(serverAppName)) {
        return serverEntry;
      }
      return null;
    }
  }

  public static final String krb5LoginModuleName =
      Constants.IS_IBM_JAVA
          ? "com.ibm.security.auth.module.Krb5LoginModule"
          : "com.sun.security.auth.module.Krb5LoginModule";

  /**
   * These Locales don't generate dates that are compatible with Hadoop MiniKdc. See LocaleTest.java
   * and https://issues.apache.org/jira/browse/DIRKRB-753
   */
  static final List<String> incompatibleLanguagesWithMiniKdc =
      List.of(
          "mzn", "ps", "mr", "uz", "ks", "bn", "my", "sd", "pa", "ar", "th", "dz", "ja", "ne",
          "ckb", "fa", "lrc", "ur", "ig", "sat", "mni", "sa", "as", "raj", "bho", "bgc");

  public static class Builder {
    private File kdcWorkDir;
    private String clientPrincipal;
    private File clientKeytab;
    private String serverPrincipal;
    private File serverKeytab;
    private String appName;
    private boolean debug = false;

    public Builder() {}

    public Builder withKdc(File kdcWorkDir) {
      this.kdcWorkDir = kdcWorkDir;
      return this;
    }

    public Builder withDebug() {
      this.debug = true;
      return this;
    }

    public Builder withJaasConfiguration(
        String clientPrincipal, File clientKeytab, String serverPrincipal, File serverKeytab) {
      this.clientPrincipal = Objects.requireNonNull(clientPrincipal);
      this.clientKeytab = Objects.requireNonNull(clientKeytab);
      this.serverPrincipal = serverPrincipal;
      this.serverKeytab = serverKeytab;
      this.appName = null;
      return this;
    }

    public Builder withJaasConfiguration(String principal, File keytab, String appName) {
      this.clientPrincipal = Objects.requireNonNull(principal);
      this.clientKeytab = Objects.requireNonNull(keytab);
      this.serverPrincipal = null;
      this.serverKeytab = null;
      this.appName = appName;
      return this;
    }

    public KerberosTestServices build() throws Exception {
      final Configuration oldConfig =
          clientPrincipal != null ? Configuration.getConfiguration() : null;
      JaasConfiguration jaasConfiguration = null;
      if (clientPrincipal != null) {
        jaasConfiguration =
            (appName == null)
                ? new JaasConfiguration(
                    clientPrincipal, clientKeytab, serverPrincipal, serverKeytab)
                : new JaasConfiguration(clientPrincipal, clientKeytab, appName);
      }
      return new KerberosTestServices(kdcWorkDir, jaasConfiguration, oldConfig, debug);
    }
  }
}
