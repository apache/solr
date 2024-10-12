package org.apache.solr.core;

/**
 * Interface for classes that will initialize a module. Implementations can
 * provide a constructor that takes a CoreContainer or a no-arg constructor.
 * The core-container constructor will be preferred if both exist.
 */
public interface ModuleLifecycle extends Runnable {


  default void run() {
    start(getCoreContainer());
  }

  CoreContainer getCoreContainer();

  void start(CoreContainer cc);

  void shutdown(CoreContainer cc);
}
