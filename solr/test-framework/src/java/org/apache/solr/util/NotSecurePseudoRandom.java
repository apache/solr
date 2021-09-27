package org.apache.solr.util;

import java.security.SecureRandom;
import java.security.SecureRandomParameters;
import java.security.SecureRandomSpi;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A mocked up instance of SecureRandom that just uses {@link Random} under the covers.
 * This is to prevent blocking issues that arise in platform default
 * SecureRandom instances due to too many instances / not enough random entropy.
 * Tests do not need secure SSL.
 */
public class NotSecurePseudoRandom extends SecureRandom {

  static class Holder {
    private static final SecureRandom INSTANCE = new NotSecurePseudoRandom();
  }

  public static SecureRandom getInstance() {
    return Holder.INSTANCE;
  }

  /**
   * Helper method that can be used to fill an array with non-zero data.
   * (Attempted workarround of Solaris SSL Padding bug: SOLR-9068)
   */
  private static final byte[] fillData(byte[] data) {
    ThreadLocalRandom.current().nextBytes(data);
    return data;
  }

  /** SPI Used to init all instances */
  private static final SecureRandomSpi NOT_SECURE_SPI = new SecureRandomSpi() {
    /** returns a new byte[] filled with static data */
    public byte[] engineGenerateSeed(int numBytes) {
      return fillData(new byte[numBytes]);
    }

    /** fills the byte[] with static data */
    public void engineNextBytes(byte[] bytes) {
      fillData(bytes);
    }

    /** NOOP */
    public void engineSetSeed(byte[] seed) { /* NOOP */ }
  };

  private NotSecurePseudoRandom() {
    super(NOT_SECURE_SPI, null);
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
  public void setSeed(byte[] seed) { /* NOOP */ }

  /** NOOP */
  public void setSeed(long seed) { /* NOOP */ }

  public void reseed() {
    /* NOOP */
  }

  public void reseed(SecureRandomParameters params) {
    /* NOOP */
  }

}
