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

package org.apache.solr.client.solrj.impl;

import java.net.URI;
import org.eclipse.jetty.client.HttpAuthenticationStore;
import org.eclipse.jetty.client.api.Authentication;
import org.eclipse.jetty.client.api.AuthenticationStore;

public class AuthenticationStoreHolder implements AuthenticationStore {

  private volatile AuthenticationStore authenticationStore;

  public AuthenticationStoreHolder() {
    this.authenticationStore = new HttpAuthenticationStore();
  }

  public AuthenticationStoreHolder(AuthenticationStore authenticationStore) {
    this.authenticationStore = authenticationStore;
  }

  public void updateAuthenticationStore(AuthenticationStore authenticationStore) {
    this.authenticationStore = authenticationStore;
  }

  @Override
  public void addAuthentication(Authentication authentication) {
    authenticationStore.addAuthentication(authentication);
  }

  @Override
  public void removeAuthentication(Authentication authentication) {
    authenticationStore.removeAuthentication(authentication);
  }

  @Override
  public void clearAuthentications() {
    authenticationStore.clearAuthentications();
  }

  @Override
  public Authentication findAuthentication(String type, URI uri, String realm) {
    return authenticationStore.findAuthentication(type, uri, realm);
  }

  @Override
  public void addAuthenticationResult(Authentication.Result result) {
    authenticationStore.addAuthenticationResult(result);
  }

  @Override
  public void removeAuthenticationResult(Authentication.Result result) {
    authenticationStore.removeAuthenticationResult(result);
  }

  @Override
  public void clearAuthenticationResults() {
    authenticationStore.clearAuthenticationResults();
  }

  @Override
  public Authentication.Result findAuthenticationResult(URI uri) {
    return authenticationStore.findAuthenticationResult(uri);
  }
}
