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
package org.apache.solr.common.cloud.acl;

import java.lang.invoke.MethodHandles;
import java.util.List;
import org.apache.solr.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to inject credentials into {@link DigestZkACLProvider} and {@link
 * DigestZkCredentialsProvider} from a Secret Manager. It expects a class implementing {@link
 * SecretCredentialsProvider} passed through System props via the property name: <code>
 * zkSecretCredentialsProvider</code> and a secret name passed with <code>
 * zkSecretCredentialSecretName</code> name
 */
public class SecretCredentialInjector implements ZkCredentialsInjector {

  public static final String SECRET_CREDENTIAL_PROVIDER_CLASS_VM_PARAM =
      "zkSecretCredentialsProvider";
  public static final String SECRET_CREDENTIAL_PROVIDER_SECRET_NAME_VM_PARAM =
      "zkSecretCredentialSecretName";
  private static final String SECRET_CREDENTIAL_PROVIDER_SECRET_NAME_DEFAULT =
      "zkCredentialsSecret";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String secretName;
  private volatile SecretCredentialsProvider secretCredentialProvider;

  public SecretCredentialInjector() {
    String secretNameVmParam =
        System.getProperties().getProperty(SECRET_CREDENTIAL_PROVIDER_SECRET_NAME_VM_PARAM);
    secretName =
        !StringUtils.isEmpty(secretNameVmParam)
            ? secretNameVmParam
            : SECRET_CREDENTIAL_PROVIDER_SECRET_NAME_DEFAULT;
  }

  @Override
  public List<ZkCredential> getZkCredentials() {
    createSecretZkDigestCredentials();
    return secretCredentialProvider.getZkCredentials(secretName);
  }

  protected void createSecretZkDigestCredentials() {
    if (secretCredentialProvider == null) {
      synchronized (this) {
        if (secretCredentialProvider == null) {
          secretCredentialProvider = getSecretCredentialProvider();
        }
      }
    }
  }

  protected SecretCredentialsProvider getSecretCredentialProvider() {
    String secretZkCredentialsProviderClassName =
        System.getProperty(SECRET_CREDENTIAL_PROVIDER_CLASS_VM_PARAM);
    try {
      log.info("Using SecretCredentialProvider: {}", secretZkCredentialsProviderClassName);
      return (SecretCredentialsProvider)
          Class.forName(secretZkCredentialsProviderClassName).getConstructor().newInstance();
    } catch (Throwable t) {
      // Fail-fast
      throw new IllegalArgumentException(
          "Could not instantiate the SecretCredentialProvider class "
              + "with name: "
              + secretZkCredentialsProviderClassName,
          t);
    }
  }
}
