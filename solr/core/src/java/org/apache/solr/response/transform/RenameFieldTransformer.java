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

import org.apache.solr.common.SolrDocument;

import java.util.Map;
import java.util.Set;

/**
 * Return a field with a name that is different that what is indexed
 *
 *
 * @since solr 4.0
 */
public class RenameFieldTransformer extends DocTransformer
{
  final String from;
  final String to;
  final boolean copy;
  final String[] ensureFromFieldPresent;

  public RenameFieldTransformer( String from, String to, boolean copy ) {
    this(from, to, copy, false);
  }

  RenameFieldTransformer( String from, String to, boolean copy, boolean ensureFromFieldPresent ) {
    this(from, to, copy, ensureFromFieldPresent ? new String[]{from} : null);
  }

  RenameFieldTransformer( String from, String to, boolean copy, String[] ensureFromFieldPresent ) {
    this.from = from;
    this.to = to;
    this.copy = copy;
    this.ensureFromFieldPresent = ensureFromFieldPresent;
    assert !from.equals(to);
  }

  @Override
  public String getName()
  {
    return "Rename["+from+">>"+to+"]";
  }

  @Override
  public DocTransformer replaceIfNecessary(Map<String, String> renamedFields, Set<String> reqFieldNames) {
    assert !copy; // we should only ever be initially constructed in a context where `copy=false` is assumed
    assert ensureFromFieldPresent != null; // sanity check
    final String replaceFrom = renamedFields.get(from);
    if (replaceFrom != null) {
      // someone else is renaming the `from` field, so use the new name
      // the other party must also be _using_ the result field, so we must now _copy_ (not rename)
      return new RenameFieldTransformer(replaceFrom, to, true, ensureFromFieldPresent);
    } else if (reqFieldNames.contains(from)) {
      // someone else requires our `from` field, so we have to copy, not replace
      return new RenameFieldTransformer(from, to, true, ensureFromFieldPresent);
    } else {
      renamedFields.put(from, to);
      return null;
    }
  }

  @Override
  public void transform(SolrDocument doc, int docid) {
    Object v = (copy)?doc.get(from) : doc.remove( from );
    if( v != null ) {
      doc.setField(to, v);
    }
  }

  @Override
  public String[] getExtraRequestFields() {
    return ensureFromFieldPresent;
  }
}
