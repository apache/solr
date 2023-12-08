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
package org.apache.solr.secret.zk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import org.apache.solr.common.cloud.ZkCredentialsInjector;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

public class AWSSecretManagerCredentialsInjector implements ZkCredentialsInjector {
  public static final String AWS_SM_CREDENTIALS_SECRET_NAME_VM_PARAM = "zkCredentialsAWSSecretName";
  private static final String AWS_SM_CREDENTIALS_REGION_VM_PARAM = "zkCredentialsAWSRegion";
  private static final String AWS_SM_CREDENTIALS_SECRET_NAME_DEFAULT = "solr.zk.credentials.secret";

  private final ObjectMapper mappers =
      SolrJacksonAnnotationInspector.createObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
          .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
          .disable(MapperFeature.AUTO_DETECT_FIELDS);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile SecretMultiCredentials secretMultiCredentials;
  private final String secretName;
  private final String regionName;

  public AWSSecretManagerCredentialsInjector() {
    String secretNameVmParam =
        System.getProperties().getProperty(AWS_SM_CREDENTIALS_SECRET_NAME_VM_PARAM);
    secretName =
        StrUtils.isNotNullOrEmpty(secretNameVmParam)
            ? secretNameVmParam
            : AWS_SM_CREDENTIALS_SECRET_NAME_DEFAULT;

    regionName = System.getProperties().getProperty(AWS_SM_CREDENTIALS_REGION_VM_PARAM);
  }

  @Override
  public List<ZkCredential> getZkCredentials() {
    createSecretMultiCredentialIfNeeded(secretName);
    List<ZkCredential> zkCredentials = secretMultiCredentials.getZkCredentials();
    log.debug(
        "getZkCredentials for secretName: {} --> zkCredentials: {}", secretName, zkCredentials);
    return zkCredentials != null ? zkCredentials : Collections.emptyList();
  }

  private void createSecretMultiCredentialIfNeeded(String secretName) {
    if (secretMultiCredentials == null) {
      synchronized (this) {
        if (secretMultiCredentials == null) {
          secretMultiCredentials = createSecretMultiCredentials(secretName);
        }
      }
    }
  }

  /**
   * expects jsonSecret in the following format: { "zkCredentials": [ {"username": "admin-user",
   * "password": "ADMIN-PASSWORD", "perms": "all"}, {"username": "readonly-user", "password":
   * "READONLY-PASSWORD", "perms": "read"} ] }
   *
   * @param secretName the AWS Secret Manager secret name used to store ZK credentials
   * @return secret in SecretMultiCredentials format
   */
  protected SecretMultiCredentials createSecretMultiCredentials(String secretName) {
    try {
      String jsonSecret = getSecretValue(secretName);
      return mappers.readValue(jsonSecret, SecretMultiCredentials.class);
    } catch (JsonProcessingException jpe) {
      // fail fast
      throw new IllegalArgumentException(
          "Exception parsing received secret from AWS Secret Manager ", jpe);
    }
  }

  protected String getSecretValue(String secretName) {
    try {
      final SecretsManagerClientBuilder secretsManagerClientBuilder =
          SecretsManagerClient.builder();
      if (StrUtils.isNotNullOrEmpty(regionName)) {
        secretsManagerClientBuilder.region(Region.of(regionName));
      }
      final SecretsManagerClient secretsClient = secretsManagerClientBuilder.build();
      final GetSecretValueRequest valueRequest =
          GetSecretValueRequest.builder().secretId(secretName).build();
      return secretsClient.getSecretValue(valueRequest).secretString();
    } catch (SecretsManagerException | IllegalArgumentException sme) {
      // fail fast
      throw new IllegalArgumentException(
          "Exception retrieving secret from AWS Secret Manager ", sme);
    }
  }

  static class SecretMultiCredentials {

    private final List<ZkCredential> zkCredentials;

    public SecretMultiCredentials() {
      this(Collections.emptyList());
    }

    public SecretMultiCredentials(List<ZkCredential> zkCredentials) {
      this.zkCredentials = zkCredentials;
    }

    public List<ZkCredential> getZkCredentials() {
      return zkCredentials;
    }

    @Override
    public String toString() {
      return "SecretMultiCredential{" + "zkCredentials=" + zkCredentials + '}';
    }
  }
}
