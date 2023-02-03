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
package org.apache.solr.response.transform;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/** New instance for each request */
public abstract class TransformerFactory implements NamedListInitializedPlugin {
  protected String defaultUserArgs = null;

  @Override
  public void init(NamedList<?> args) {
    defaultUserArgs = (String) args.get("args");
  }

  public abstract DocTransformer create(String field, SolrParams params, SolrQueryRequest req);

  /**
   * The {@link FieldRenamer} interface should be implemented by any {@link TransformerFactory}
   * capable of generating transformers that might rename fields, and should implement {@link
   * #create(String, SolrParams, SolrQueryRequest, Map, Set)} in place of {@link #create(String,
   * SolrParams, SolrQueryRequest)} (with the latter method overridden to throw {@link
   * UnsupportedOperationException}).
   *
   * <p>{@link DocTransformer}s returned via {@link #create(String, SolrParams, SolrQueryRequest,
   * Map, Set)} will be added in a second pass, allowing simplified logic in {@link
   * TransformerFactory#create(String, SolrParams, SolrQueryRequest)} for non-renaming factories.
   *
   * <p>{@link #create(String, SolrParams, SolrQueryRequest, Map, Set)} must implement extra logic
   * to be aware of preceding field renames, and to make subsequent {@link FieldRenamer}
   * transformers aware of its own field renames.
   *
   * <p>It is harmless for a {@link DocTransformer} that does _not_ in practice rename fields to be
   * returned from a factory that implements this interface (e.g., for conditional renames?); but
   * doing so opens the possibility of {@link #create(String, SolrParams, SolrQueryRequest, Map,
   * Set)} being called _after_ fields have been renamed, so such implementations must still check
   * whether the field with which they are concerned has been renamed ... and if it _has_, must copy
   * the field back to its original name. This situation also demonstrates the motivation for
   * separating the creation of {@link DocTransformer}s into two phases: an initial phase involving
   * no field renames, and a subsequent phase that implement extra logic to properly handle field
   * renames.
   */
  public interface FieldRenamer {
    // TODO: Behavior is undefined in the event of a "destination field" collision (e.g., a user
    // maps two fields to the same "destination field", or maps a field to a top-level requested
    // field). In the future, the easiest way to detect such a case would be by "failing fast" upon
    // renaming to a field that already has an associated value, or support for this feature could
    // be expressly added via a hypothetical `combined_field:[consolidate fl=field_1,field_2]`
    // transformer.
    /**
     * Analogous to {@link TransformerFactory#create(String, SolrParams, SolrQueryRequest)}, but to
     * be implemented by {@link TransformerFactory}s that produce {@link DocTransformer}s that may
     * rename fields.
     *
     * @param field The destination field
     * @param params Local params associated with this transformer (e.g., source field)
     * @param req The current request
     * @param renamedFields Maps source=&gt;dest renamed fields. Implementations should check this
     *     first, updating their own "source" field(s) as necessary, and if renaming (not copying)
     *     fields, should also update this map with the implementations "own" introduced
     *     source=&gt;dest field mapping
     * @param reqFieldNames Set of explicitly requested field names; implementations should consult
     *     this set to determine whether it's appropriate to rename (vs. copy) a field (e.g.: <code>
     *     boolean
     *                      copy = reqFieldNames != null &amp;&amp; reqFieldNames.contains(sourceField)
     *     </code>)
     * @return A transformer to be used in processing field values in returned documents.
     */
    DocTransformer create(
        String field,
        SolrParams params,
        SolrQueryRequest req,
        Map<String, String> renamedFields,
        Set<String> reqFieldNames);

    /**
     * Returns <code>true</code> if implementations of this class may (even subtly) modify field
     * values. ({@link GeoTransformerFactory} may do this, e.g.). To fail safe, the default
     * implementation returns <code>true</code>. This method should be overridden to return <code>
     * false</code> if the implementing class is guaranteed to not modify any values for the fields
     * that it renames.
     */
    default boolean mayModifyValue() {
      return true;
    }
  }

  public static final Map<String, TransformerFactory> defaultFactories = new HashMap<>(9, 1.0f);

  static {
    defaultFactories.put("explain", new ExplainAugmenterFactory());
    defaultFactories.put("value", new ValueAugmenterFactory());
    defaultFactories.put("docid", new DocIdAugmenterFactory());
    defaultFactories.put("shard", new ShardAugmenterFactory());
    defaultFactories.put("child", new ChildDocTransformerFactory());
    defaultFactories.put("subquery", new SubQueryAugmenterFactory());
    defaultFactories.put("json", new RawValueTransformerFactory("json"));
    defaultFactories.put("xml", new RawValueTransformerFactory("xml"));
    defaultFactories.put("geo", new GeoTransformerFactory());
    defaultFactories.put("core", new CoreAugmenterFactory());
  }
}
