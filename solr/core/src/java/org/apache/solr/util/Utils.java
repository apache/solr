package org.apache.solr.util;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.handler.component.FacetComponent;

public class Utils {
  public static final FacetComponent.ShardFacetCount[] EMPTY_SHARD_FACET_CNT = new FacetComponent.ShardFacetCount[0];
  public static final BytesRef[] EMPTY_BYTES_REFS = new BytesRef[0];
}
