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

package org.apache.solr.core;

import java.util.Collection;
import java.util.Set;

/**
 * The base class for custom transient core maintenance. Any custom plugin that wants to take
 * control of transient caches (i.e. any core defined with transient=true) should override this
 * class.
 *
 * <p>WARNING: There is quite a bit of higher-level locking done by the CoreContainer to avoid
 * various race conditions etc. You should _only_ manipulate them within the method calls designed
 * to change them. E.g. only add to the transient core descriptors in addTransientDescriptor etc.
 *
 * <p>Trust the higher-level code (mainly SolrCores and CoreContainer) to call the appropriate
 * operations when necessary and to coordinate shutting down cores, manipulating the internal
 * structures and the like.
 *
 * <p>The only real action you should _initiate_ is to close a core for whatever reason, and do that
 * by calling notifyCoreCloseListener(coreToClose); The observer will call back to removeCore(name)
 * at the appropriate time. There is no need to directly remove the core _at that time_ from the
 * transientCores list, a call will come back to this class when CoreContainer is closing this core.
 *
 * <p>CoreDescriptors are read-once. During "core discovery" all valid descriptors are enumerated
 * and added to the appropriate list. Thereafter, they are NOT re-read from disk. In those
 * situations where you want to re-define the coreDescriptor, maintain a "side list" of changed core
 * descriptors. Then override getTransientDescriptor to return your new core descriptor. NOTE:
 * assuming you've already closed the core, the _next_ time that core is required
 * getTransientDescriptor will be called and if you return the new core descriptor your
 * re-definition should be honored. You'll have to maintain this list for the duration of this Solr
 * instance running. If you persist the coreDescriptor, then next time Solr starts up the new
 * definition will be read.
 *
 * <p>If you need to manipulate the return, for instance block a core from being loaded for some
 * period of time, override say getTransientDescriptor and return null.
 *
 * <p>In particular, DO NOT reach into the transientCores structure from a method called to
 * manipulate core descriptors or vice-versa.
 */
@Deprecated(since = "9.2")
public abstract class TransientSolrCoreCache {

  /** Adds the newly-opened core to the list of open cores. */
  public abstract SolrCore addCore(String name, SolrCore core);

  /** Returns the names of all possible cores, whether they are currently loaded or not. */
  public abstract Set<String> getAllCoreNames();

  /** Returns the names of all currently loaded cores. */
  public abstract Set<String> getLoadedCoreNames();

  /**
   * Removes a core from the internal structures, presumably it being closed. If the core is
   * re-opened, it will be re-added by CoreContainer.
   */
  public abstract SolrCore removeCore(String name);

  /** Gets the core associated with the name. Returns null if there is none. */
  public abstract SolrCore getCore(String name);

  /** Returns whether the cache contains the named core. */
  public abstract boolean containsCore(String name);

  // These methods allow the implementation to maintain control over the core descriptors.

  /**
   * Adds a new {@link CoreDescriptor}. This method will only be called during core discovery at
   * startup.
   */
  public abstract void addTransientDescriptor(String rawName, CoreDescriptor cd);

  /**
   * Gets the {@link CoreDescriptor} for a transient core (loaded or unloaded). This method is used
   * when opening cores and the like. If you want to change a core's descriptor, override this
   * method and return the current core descriptor.
   */
  public abstract CoreDescriptor getTransientDescriptor(String name);

  /** Gets the {@link CoreDescriptor} for all transient cores (loaded and unloaded). */
  public abstract Collection<CoreDescriptor> getTransientDescriptors();

  /** Removes a {@link CoreDescriptor} from the list of transient cores descriptors. */
  public abstract CoreDescriptor removeTransientDescriptor(String name);

  /** Called in order to free resources. */
  public void close() {
    // Nothing to do currently
  }
  ;

  /**
   * Gets a custom status for the given core name. Allows custom implementations to communicate
   * arbitrary information as necessary.
   */
  public abstract int getStatus(String coreName);

  /**
   * Sets a custom status for the given core name. Allows custom implementations to communicate
   * arbitrary information as necessary.
   */
  public abstract void setStatus(String coreName, int status);
}
