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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.solr.common.util.NamedList;

/** A {@link SolrCores} that supports {@link CoreDescriptor#isTransient()}. */
public class TransientSolrCores extends SolrCores {

  public final NamedList<?> initArgs;
  protected TransientSolrCoreCache transientSolrCoreCache;

  public TransientSolrCores(CoreContainer container) {
    super(container);
    initArgs = container.cfg.getCoreManagerConfig().initArgs;
    transientSolrCoreCache = new TransientSolrCoreCacheDefault(this);
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
        getTransientCacheHandler().addTransientDescriptor(p.getName(), p);
      }
    } else {
      super.addCoreDescriptor(p);
    }
  }

  @Override
  public void removeCoreDescriptor(CoreDescriptor p) {
    if (p.isTransient()) {
      synchronized (modifyLock) {
        getTransientCacheHandler().removeTransientDescriptor(p.getName());
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
        return getTransientCacheHandler().addCore(cd.getName(), core);
      }
    } else {
      return super.putCore(cd, core);
    }
  }

  @Override
  public List<String> getLoadedCoreNames() {
    synchronized (modifyLock) {
      return distinctSetsUnion(
          super.getLoadedCoreNames(), getTransientCacheHandler().getLoadedCoreNames());
    }
  }

  @Override
  public List<String> getAllCoreNames() {
    synchronized (modifyLock) {
      return distinctSetsUnion(
          super.getAllCoreNames(), getTransientCacheHandler().getAllCoreNames());
    }
  }

  private List<String> distinctSetsUnion(List<String> listA, Set<String> setB) {
    if (listA.isEmpty()) {
      return new ArrayList<>(setB);
    }
    return distinctSetsUnion(new HashSet<>(listA), setB);
  }

  /**
   * Makes the union of two distinct sets.
   *
   * @return An unsorted list. This list is a new copy, it can be modified by the caller (e.g. it
   *     can be sorted).
   */
  private static <T> List<T> distinctSetsUnion(Set<T> set1, Set<T> set2) {
    assert areSetsDistinct(set1, set2);
    List<T> union = new ArrayList<>(set1.size() + set2.size());
    union.addAll(set1);
    union.addAll(set2);
    return union;
  }

  /** Indicates whether two sets are distinct (intersection is empty). */
  private static <T> boolean areSetsDistinct(Set<T> set1, Set<T> set2) {
    return set1.stream().noneMatch(set2::contains);
  }

  @Override
  public int getNumLoadedTransientCores() {
    synchronized (modifyLock) {
      return getTransientCacheHandler().getLoadedCoreNames().size();
    }
  }

  @Override
  public int getNumUnloadedCores() {
    synchronized (modifyLock) {
      return super.getNumUnloadedCores()
          + getTransientCacheHandler().getAllCoreNames().size()
          - getTransientCacheHandler().getLoadedCoreNames().size();
    }
  }

  @Override
  public int getNumAllCores() {
    synchronized (modifyLock) {
      return super.getNumAllCores() + getTransientCacheHandler().getAllCoreNames().size();
    }
  }

  @Override
  public SolrCore remove(String name) {
    synchronized (modifyLock) {
      SolrCore ret = super.remove(name);
      // It could have been a newly-created core. It could have been a transient core. The
      // newly-created cores in particular should be checked. It could have been a dynamic core.
      if (ret == null) {
        ret = getTransientCacheHandler().removeCore(name);
      }
      return ret;
    }
  }

  @Override
  protected SolrCore getLoadedCoreWithoutIncrement(String name) {
    synchronized (modifyLock) {
      final var core = super.getLoadedCoreWithoutIncrement(name);
      return core != null ? core : getTransientCacheHandler().getCore(name);
    }
  }

  @Override
  public boolean isLoaded(String name) {
    synchronized (modifyLock) {
      return super.isLoaded(name) || getTransientCacheHandler().containsCore(name);
    }
  }

  @Override
  public CoreDescriptor getCoreDescriptor(String coreName) {
    synchronized (modifyLock) {
      CoreDescriptor coreDescriptor = super.getCoreDescriptor(coreName);
      if (coreDescriptor != null) {
        return coreDescriptor;
      }
      return getTransientCacheHandler().getTransientDescriptor(coreName);
    }
  }

  @Override
  public List<CoreDescriptor> getCoreDescriptors() {
    synchronized (modifyLock) {
      List<CoreDescriptor> coreDescriptors = new ArrayList<>(getNumAllCores());
      coreDescriptors.addAll(super.getCoreDescriptors());
      coreDescriptors.addAll(getTransientCacheHandler().getTransientDescriptors());
      return coreDescriptors;
    }
  }

  /**
   * @return the cache holding the transient cores; never null.
   */
  private TransientSolrCoreCache getTransientCacheHandler() {
    return transientSolrCoreCache;
  }
}
