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
package org.apache.solr.common.cloud;

import static org.apache.solr.common.cloud.ZkCredentialsInjector.ZkCredential.Perms;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads credentials from System Properties and injects them into {@link
 * DigestZkCredentialsProvider} &amp; {@link DigestZkACLProvider} Usage:
 *
 * <pre>
 *   -DzkCredentialsInjector=org.apache.solr.common.cloud.VMParamsZkCredentialsInjector \
 *   -DzkDigestUsername=admin-user -DzkDigestPassword=CHANGEME-ADMIN-PASSWORD \
 *   -DzkDigestReadonlyUsername=readonly-user -DzkDigestReadonlyPassword=CHANGEME-READONLY-PASSWORD
 * </pre>
 *
 * Or from a Java property file:
 *
 * <pre>
 *   -DzkCredentialsInjector=org.apache.solr.common.cloud.VMParamsZkCredentialsInjector \
 *   -DzkDigestCredentialsFile=SOLR_HOME_DIR/server/etc/zookeepercredentials.properties
 * </pre>
 *
 * Example of a Java property file:
 *
 * <pre>
 * zkDigestUsername=admin-user
 * zkDigestPassword=CHANGEME-ADMIN-PASSWORD
 * zkDigestReadonlyUsername=readonly-user
 * zkDigestReadonlyPassword=CHANGEME-READONLY-PASSWORD
 * </pre>
 */
public class VMParamsZkCredentialsInjector implements ZkCredentialsInjector {

  public static final String DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME = "zkDigestUsername";
  public static final String DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME = "zkDigestPassword";
  public static final String DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME =
      "zkDigestReadonlyUsername";
  public static final String DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME =
      "zkDigestReadonlyPassword";
  public static final String DEFAULT_DIGEST_FILE_VM_PARAM_NAME = "zkDigestCredentialsFile";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final String zkDigestAllUsernameVMParamName;
  final String zkDigestAllPasswordVMParamName;
  final String zkDigestReadonlyUsernameVMParamName;
  final String zkDigestReadonlyPasswordVMParamName;
  final Properties credentialsProps;

  public VMParamsZkCredentialsInjector() {
    this(
        DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME,
        DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
        DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME,
        DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME);
  }

  public VMParamsZkCredentialsInjector(
      String zkDigestAllUsernameVMParamName,
      String zkDigestAllPasswordVMParamName,
      String zkDigestReadonlyUsernameVMParamName,
      String zkDigestReadonlyPasswordVMParamName) {
    this.zkDigestAllUsernameVMParamName = zkDigestAllUsernameVMParamName;
    this.zkDigestAllPasswordVMParamName = zkDigestAllPasswordVMParamName;
    this.zkDigestReadonlyUsernameVMParamName = zkDigestReadonlyUsernameVMParamName;
    this.zkDigestReadonlyPasswordVMParamName = zkDigestReadonlyPasswordVMParamName;

    String pathToFile = System.getProperty(DEFAULT_DIGEST_FILE_VM_PARAM_NAME);
    credentialsProps =
        (pathToFile != null) ? readCredentialsFile(pathToFile) : System.getProperties();
  }

  public static Properties readCredentialsFile(String pathToFile) throws SolrException {
    Properties props = new Properties();
    try (Reader reader =
        new InputStreamReader(new FileInputStream(pathToFile), StandardCharsets.UTF_8)) {
      props.load(reader);
    } catch (IOException ioExc) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, ioExc);
    }
    return props;
  }

  @Override
  public List<ZkCredential> getZkCredentials() {
    final String digestAllUsername = credentialsProps.getProperty(zkDigestAllUsernameVMParamName);
    final String digestAllPassword = credentialsProps.getProperty(zkDigestAllPasswordVMParamName);
    final String digestReadonlyUsername =
        credentialsProps.getProperty(zkDigestReadonlyUsernameVMParamName);
    final String digestReadonlyPassword =
        credentialsProps.getProperty(zkDigestReadonlyPasswordVMParamName);

    List<ZkCredential> zkCredentials =
        List.of(
            new ZkCredential(digestAllUsername, digestAllPassword, Perms.ALL),
            new ZkCredential(digestReadonlyUsername, digestReadonlyPassword, Perms.READ));
    log.info(
        "Using zkCredentials: digestAllUsername: {}, digestReadonlyUsername: {}",
        digestAllUsername,
        digestReadonlyUsername);
    return zkCredentials;
  }
}
