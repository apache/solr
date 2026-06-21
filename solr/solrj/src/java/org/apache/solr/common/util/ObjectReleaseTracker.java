package org.apache.solr.common.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public abstract class ObjectReleaseTracker {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static class ObjectReleaseTrackerHolder {
    public static ObjectReleaseTracker HOLDER_INSTANCE;

    static {
      try {
        HOLDER_INSTANCE = (ObjectReleaseTracker) Class.forName(System.getProperty("solr.objectReleaseTracker", NoOpObjectReleaseTracker.class.getName())).getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        log.warn("Could not find object release tracker class", e);
      }
    }
  }

  public static ObjectReleaseTracker getInstance() {
    return ObjectReleaseTrackerHolder.HOLDER_INSTANCE;
  }

  private static class NoOpObjectReleaseTracker extends ObjectReleaseTracker {
    @Override public boolean track(Object object) {
      return true;
    }

    @Override public boolean release(Object object) {
      return true;
    }

    @Override public void clear() {

    }

    @Override public String checkEmpty() {
      return null;
    }

    @Override public String checkEmpty(String object) {
      return null;
    }
  }

  public abstract boolean track(Object object);

  public abstract boolean release(Object object);

  public abstract void clear();

  /**
   * @return null if ok else error message
   */
  public abstract String checkEmpty();

  /**
   * @param object tmp feature allowing to ignore and close an object
   * @return null if ok else error message
   */
  public abstract String checkEmpty(String object);
}
