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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/** A {@link SolrCores} that supports {@link CoreDescriptor#isTransient()}. */
@Deprecated(since = "9.2")
public class TransientSolrCores extends SolrCores {

  protected final TransientSolrCoreCache transientSolrCoreCache;

  public TransientSolrCores(CoreContainer container, int cacheSize) {
    super(container);
    transientSolrCoreCache = new TransientSolrCoreCacheDefault(this, cacheSize);
  }

  @Override
  protected void close() {
    super.close();
    transientSolrCoreCache.close();
  }

  @Override
  public void addCoreDescriptor(CoreDescriptor p) {
    if (p.isTransient()) {
      synchronized (modifyLock) {
        transientSolrCoreCache.addTransientDescriptor(p.getName(), p);
      }
    } else {
      super.addCoreDescriptor(p);
    }
  }

  @Override
  public void removeCoreDescriptor(CoreDescriptor p) {
    if (p.isTransient()) {
      synchronized (modifyLock) {
        transientSolrCoreCache.removeTransientDescriptor(p.getName());
      }
    } else {
      super.removeCoreDescriptor(p);
    }
  }

  @Override
  // Returns the old core if there was a core of the same name.
  // WARNING! This should be the _only_ place you put anything into the list of transient cores!
  public SolrCore putCore(CoreDescriptor cd, SolrCore core) {
    if (cd.isTransient()) {
      synchronized (modifyLock) {
        addCoreDescriptor(cd); // cd must always be registered if we register a core
        return transientSolrCoreCache.addCore(cd.getName(), core);
      }
    } else {
      return super.putCore(cd, core);
    }
  }

  @Override
  public List<String> getLoadedCoreNames() {
    synchronized (modifyLock) {
      List<String> coreNames = super.getLoadedCoreNames(); // mutable
      coreNames.addAll(transientSolrCoreCache.getLoadedCoreNames());
      assert isSet(coreNames);
      return coreNames;
    }
  }

  @Override
  public List<String> getAllCoreNames() {
    synchronized (modifyLock) {
      List<String> coreNames = super.getAllCoreNames(); // mutable
      coreNames.addAll(transientSolrCoreCache.getAllCoreNames());
      assert isSet(coreNames);
      return coreNames;
    }
  }

  private static boolean isSet(Collection<?> collection) {
    return collection.size() == new HashSet<>(collection).size();
  }

  @Override
  public int getNumLoadedTransientCores() {
    synchronized (modifyLock) {
      return transientSolrCoreCache.getLoadedCoreNames().size();
    }
  }

  @Override
  public int getNumUnloadedCores() {
    synchronized (modifyLock) {
      return super.getNumUnloadedCores()
          + transientSolrCoreCache.getAllCoreNames().size()
          - transientSolrCoreCache.getLoadedCoreNames().size();
    }
  }

  @Override
  public int getNumAllCores() {
    synchronized (modifyLock) {
      return super.getNumAllCores() + transientSolrCoreCache.getAllCoreNames().size();
    }
  }

  @Override
  public SolrCore remove(String name) {
    synchronized (modifyLock) {
      SolrCore ret = super.remove(name);
      // It could have been a newly-created core. It could have been a transient core. The
      // newly-created cores in particular should be checked. It could have been a dynamic core.
      if (ret == null) {
        ret = transientSolrCoreCache.removeCore(name);
      }
      return ret;
    }
  }

  @Override
  protected SolrCore getLoadedCoreWithoutIncrement(String name) {
    synchronized (modifyLock) {
      final var core = super.getLoadedCoreWithoutIncrement(name);
      return core != null ? core : transientSolrCoreCache.getCore(name);
    }
  }

  @Override
  public boolean isLoaded(String name) {
    synchronized (modifyLock) {
      return super.isLoaded(name) || transientSolrCoreCache.containsCore(name);
    }
  }

  @Override
  public CoreDescriptor getCoreDescriptor(String coreName) {
    synchronized (modifyLock) {
      CoreDescriptor coreDescriptor = super.getCoreDescriptor(coreName);
      if (coreDescriptor != null) {
        return coreDescriptor;
      }
      return transientSolrCoreCache.getTransientDescriptor(coreName);
    }
  }

  @Override
  public List<CoreDescriptor> getCoreDescriptors() {
    synchronized (modifyLock) {
      List<CoreDescriptor> coreDescriptors = new ArrayList<>(getNumAllCores());
      coreDescriptors.addAll(super.getCoreDescriptors());
      coreDescriptors.addAll(transientSolrCoreCache.getTransientDescriptors());
      return coreDescriptors;
    }
  }
}
