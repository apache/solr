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
package org.apache.solr.hdfs.cloud;

import java.io.IOException;
import java.security.GeneralSecurityException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.KeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;

public class DisableKeyStoresFactory implements KeyStoresFactory {
  @Override
  public void init(SSLFactory.Mode mode) throws IOException, GeneralSecurityException {}

  @Override
  public void destroy() {}

  @Override
  public KeyManager[] getKeyManagers() {
    return new KeyManager[0];
  }

  @Override
  public TrustManager[] getTrustManagers() {
    return new TrustManager[0];
  }

  @Override
  public void setConf(Configuration conf) {}

  @Override
  public Configuration getConf() {
    return null;
  }
}
