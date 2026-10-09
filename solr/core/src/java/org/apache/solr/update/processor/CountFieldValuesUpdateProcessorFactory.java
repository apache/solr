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
package org.apache.solr.update.processor;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.update.processor.FieldMutatingUpdateProcessor.mutator;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;
import org.apache.solr.common.SolrDocumentBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * Replaces any list of values for a field matching the specified conditions with the count of the
 * number of values for that field.
 *
 * <p>By default, this processor matches no fields.
 *
 * <p>The typical use case for this processor would be in combination with the {@link
 * CloneFieldUpdateProcessorFactory} so that it's possible to query by the quantity of values in the
 * source field.
 *
 * <p>For example, in the configuration below, the end result will be that the <code>category_count
 * </code> field can be used to search for documents based on how many values they contain in the
 * <code>category</code> field.
 *
 * <pre class="prettyprint">
 * &lt;processor class="solr.CloneFieldUpdateProcessorFactory"&gt;
 *   &lt;str name="source"&gt;category&lt;/str&gt;
 *   &lt;str name="dest"&gt;category_count&lt;/str&gt;
 * &lt;/processor&gt;
 * &lt;processor class="solr.CountFieldValuesUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;category_count&lt;/str&gt;
 * &lt;/processor&gt;
 * &lt;processor class="solr.DefaultValueUpdateProcessorFactory"&gt;
 *   &lt;str name="fieldName"&gt;category_count&lt;/str&gt;
 *   &lt;int name="value"&gt;0&lt;/int&gt;
 * &lt;/processor&gt;</pre>
 *
 * <p><b>NOTE:</b> The use of {@link DefaultValueUpdateProcessorFactory} is important in this
 * example to ensure that all documents have a value for the <code>category_count</code> field,
 * because <code>CountFieldValuesUpdateProcessorFactory</code> only <i>replaces</i> the list of
 * values with the size of that list. If <code>DefaultValueUpdateProcessorFactory</code> was not
 * used, then any document that had no values for the <code>category</code> field, would also have
 * no value in the <code>category_count</code> field.
 *
 * @since 4.0.0
 */
public final class CountFieldValuesUpdateProcessorFactory
    extends FieldMutatingUpdateProcessorFactory {

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    return mutator(
        getSelector(),
        next,
        src -> {
          SolrInputField result = new SolrInputField(src.getName());
          Collection<Object> values = src.getValues();
          if (values != null) {
            for (Object value : values) {
              if (value instanceof Map && !(value instanceof SolrDocumentBase)) {
                return countAtomicUpdate(src, result, values);
              }
            }
          }
          result.setValue(src.getValueCount());
          return result;
        });
  }

  /**
   * An atomic update arrives as one or more maps of operations, not as the values themselves: a
   * field added once per operation carries several maps, and the operations apply in order. Only a
   * {@code set} can be counted, because the count after any other operation depends on the stored
   * document; any other operation is rejected with a BAD_REQUEST error instead of being silently
   * dropped. When several {@code set} operations are present the last one decides the values, as it
   * does when the operations are applied to the stored document. The result stays an atomic {@code
   * set} of the counted operand.
   */
  private static SolrInputField countAtomicUpdate(
      SolrInputField src, SolrInputField result, Collection<Object> values) {
    boolean seenSet = false;
    Object setOperand = null;
    for (Object value : values) {
      if (!(value instanceof Map) || value instanceof SolrDocumentBase) {
        throw new SolrException(
            BAD_REQUEST,
            "Field "
                + src.getName()
                + " mixes atomic update operations with plain values: "
                + values);
      }
      for (Map.Entry<?, ?> operation : ((Map<?, ?>) value).entrySet()) {
        if (!"set".equals(operation.getKey())) {
          throw new SolrException(
              BAD_REQUEST,
              "CountFieldValuesUpdateProcessorFactory cannot count field '"
                  + src.getName()
                  + "': atomic update operation '"
                  + operation.getKey()
                  + "' is not supported; only 'set' can be counted");
        }
        seenSet = true;
        setOperand = operation.getValue();
      }
    }
    if (!seenSet) {
      throw new SolrException(
          BAD_REQUEST,
          "CountFieldValuesUpdateProcessorFactory cannot count field '"
              + src.getName()
              + "': the atomic update contains no operation; only 'set' can be counted");
    }
    result.setValue(Map.of("set", countOperand(setOperand)));
    return result;
  }

  /** The number of values a {@code set} operand stands for: a null operand counts as zero. */
  private static int countOperand(Object operand) {
    if (operand == null) {
      return 0;
    } else if (operand instanceof Collection) {
      return ((Collection<?>) operand).size();
    } else if (operand.getClass().isArray()) {
      return Array.getLength(operand);
    }
    return 1;
  }
}
