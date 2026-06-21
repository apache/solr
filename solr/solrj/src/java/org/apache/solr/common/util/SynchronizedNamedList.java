package org.apache.solr.common.util;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectLists;

public class SynchronizedNamedList<T> extends NamedList<T> {

  public SynchronizedNamedList(int sz) {
    super(ObjectLists.synchronize(new ObjectArrayList<T>(sz)));
  }

  public SynchronizedNamedList(NamedList nl) {
    super(ObjectLists.synchronize(nl.nvPairs));
  }

}
