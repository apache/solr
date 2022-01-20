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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;

/**
 * @since solr 5.2
 */
public class RawValueTransformerFactory extends TransformerFactory implements TransformerFactory.FieldRenamer
{
  String applyToWT = null;
  
  public RawValueTransformerFactory() {
    
  }

  public RawValueTransformerFactory(String wt) {
    this.applyToWT = wt;
  }
  
  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    if(defaultUserArgs!=null&&defaultUserArgs.startsWith("wt=")) {
      applyToWT = defaultUserArgs.substring(3);
    }
  }
  
  @Override
  public DocTransformer create(String display, SolrParams params, SolrQueryRequest req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean mayModifyValue() {
    // The only thing we may modify is the _serialization_; field values per se are guaranteed to be unmodified.
    return false;
  }

  @Override
  public DocTransformer create(String display, SolrParams params, SolrQueryRequest req,
                               Map<String, String> renamedFields, Set<String> reqFieldNames) {
    String field = params.get("f");
    if(Strings.isNullOrEmpty(field)) {
      field = display;
    }
    field = renamedFields.getOrDefault(field, field);
    final boolean rename = !field.equals(display);
    final boolean copy = rename && reqFieldNames != null && reqFieldNames.contains(field);
    if (!copy) {
      renamedFields.put(field, display);
    }
    // When a 'wt' is specified in the transformer, only apply it to the same wt
    boolean apply = true;
    if(applyToWT!=null) {
      String qwt = req.getParams().get(CommonParams.WT);
      if(qwt==null) {
        QueryResponseWriter qw = req.getCore().getQueryResponseWriter(req);
        QueryResponseWriter dw = req.getCore().getQueryResponseWriter(applyToWT);
        if(qw!=dw) {
          apply = false;
        }
      }
      else {
        apply = applyToWT.equals(qwt);
      }
    }

    if(apply) {
      return new RawTransformer( field, display, copy );
    }
    
    if (!rename) {
      // we have to ensure the field is returned
      return new DocTransformer.NoopFieldTransformer(field);
    }
    return new RenameFieldTransformer( field, display, copy );
  }

  static class RawTransformer extends DocTransformer
  {
    final String field;
    final String display;
    final boolean copy;

    public RawTransformer( String field, String display, boolean copy )
    {
      this.field = field;
      this.display = display;
      this.copy = copy;
    }

    @Override
    public String getName()
    {
      return display;
    }

    @Override
    public Collection<String> getRawFields() {
      return Collections.singleton(display);
    }

    @Override
    public void transform(SolrDocument doc, int docid) {
      Object val = copy ? doc.get(field) : doc.remove(field);
      if(val != null) {
        doc.setField(display, val);
      }
    }

    @Override
    public String[] getExtraRequestFields() {
      return new String[] {this.field};
    }
  }
}


