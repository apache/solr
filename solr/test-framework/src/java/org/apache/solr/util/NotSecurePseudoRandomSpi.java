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
package org.apache.solr.util;

import java.security.SecureRandomParameters;
import java.security.SecureRandomSpi;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A mocked up instance of SecureRandom that just uses {@link Random} under the covers. This is to
 * prevent blocking issues that arise in platform default SecureRandom instances due to too many
 * instances / not enough random entropy. Tests do not need secure SSL.
 */
public class NotSecurePseudoRandomSpi extends SecureRandomSpi {

  @Override
  protected void engineSetSeed(byte[] seed) {}

  @Override
  protected void engineNextBytes(byte[] bytes) {
    fillData(bytes);
  }

  @Override
  protected byte[] engineGenerateSeed(int numBytes) {
    return new byte[numBytes];
  }

  /**
   * Helper method that can be used to fill an array with non-zero data. (Attempted workarround of
   * Solaris SSL Padding bug: SOLR-9068)
   */
  private static final byte[] fillData(byte[] data) {
    ThreadLocalRandom.current().nextBytes(data);
    return data;
  }

  public NotSecurePseudoRandomSpi() {
    super();
  }

  /** returns a new byte[] filled with static data */
  public byte[] generateSeed(int numBytes) {
    return fillData(new byte[numBytes]);
  }

  /** fills the byte[] with static data */
  public void nextBytes(byte[] bytes) {
    fillData(bytes);
  }

  public void nextBytes(byte[] bytes, SecureRandomParameters params) {
    fillData(bytes);
  }

  /** NOOP */
  public void setSeed(byte[] seed) {
    /* NOOP */
  }

  /** NOOP */
  public void setSeed(long seed) {
    /* NOOP */
  }

  public void reseed() {
    /* NOOP */
  }

  public void reseed(SecureRandomParameters params) {
    /* NOOP */
  }
}
