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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.util.LogLevel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "https://issues.apache.org/jira/browse/DIRKRB-753")
@LogLevel("org.apache.kerby=WARN")
public class LocaleTest extends SolrTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testWithKdc() throws Exception {
    final String principal = "server";

    Path kdcDir = createTempDir().resolve("miniKdc");
    File keytabFile = kdcDir.resolve("keytabs").toFile();

    Properties conf = MiniKdc.createConf();
    conf.setProperty("kdc.port", "0");
    MiniKdc kdc = new MiniKdc(conf, kdcDir.toFile());
    kdc.start();
    kdc.createPrincipal(keytabFile, principal);

    AppConfigurationEntry appConfigEntry =
        new AppConfigurationEntry(
            KerberosTestServices.krb5LoginModuleName,
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            Map.of(
                "principal",
                principal,
                "storeKey",
                "true",
                "useKeyTab",
                "true",
                "useTicketCache",
                "false",
                "refreshKrb5Config",
                "true",
                "keyTab",
                keytabFile.getAbsolutePath(),
                "keytab",
                keytabFile.getAbsolutePath()));
    Configuration configuration =
        new Configuration() {
          @Override
          public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            return new AppConfigurationEntry[] {appConfigEntry};
          }
        };

    Set<String> locales = new HashSet<>();
    for (Locale locale : Locale.getAvailableLocales()) {
      try {
        Locale.setDefault(locale);
        new LoginContext("Server", null, null, configuration).login();
      } catch (LoginException e) {
        locales.add(locale.getLanguage());
      }
    }

    kdc.stop();

    log.info("Could not login with locales {}", locales);

    List<String> missingLanguages = new ArrayList<>();
    for (String locale : locales) {
      if (!KerberosTestServices.incompatibleLanguagesWithMiniKdc.contains(locale)) {
        missingLanguages.add(locale);
      }
    }

    assertTrue(
        "KerberosTestServices#incompatibleLanguagesWithMiniKdc is missing languages: "
            + missingLanguages,
        missingLanguages.isEmpty());
  }
}
