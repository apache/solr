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
package org.apache.solr.mappings;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.NumberType;

/** Utilities for mapping-field module tests */
public class MappingsTestUtils {

  private static final SimpleDateFormat format =
      new SimpleDateFormat("yyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);

  /** generate string-string mappings */
  public static List<SolrInputDocument> generateDocs(
      Random random, String field, int nb, boolean multiVal, boolean predictableStrKey) {
    return generateDocs(random, field, nb, multiVal, null, null, predictableStrKey);
  }

  /** generate string-NumberType mappings */
  public static List<SolrInputDocument> generateDocs(
      Random random,
      String field,
      int nb,
      boolean multiVal,
      NumberType subType,
      boolean predictableStrKey) {
    return generateDocs(random, field, nb, multiVal, subType, null, predictableStrKey);
  }

  /** generate solr input documents with the given keyType and subType (value type) */
  public static List<SolrInputDocument> generateDocs(
      Random random,
      String field,
      int nb,
      boolean multiVal,
      NumberType subType,
      NumberType keyType,
      boolean predictableStrKey) {
    List<SolrInputDocument> docs = new ArrayList<>();
    if (multiVal) {
      for (int i = 0; i < nb; i++) {
        SolrInputDocument sdoc = new SolrInputDocument();
        sdoc.addField("id", "" + i);
        for (int j = 0; j < nb; j++) {
          String key = null;
          if (predictableStrKey) {
            key = "key_" + i + "_" + j;
          } else {
            key = getRandomValue(keyType, random);
          }
          String val = getRandomValue(subType, random);
          sdoc.addField(field, "\"" + key + "\",\"" + val + "\"");
        }
        docs.add(sdoc);
      }
    } else {
      for (int i = 0; i < nb; i++) {
        SolrInputDocument sdoc = new SolrInputDocument();
        sdoc.addField("id", "" + i);
        String key = null;
        if (predictableStrKey) {
          key = "key_" + i;
        } else {
          key = getRandomValue(keyType, random);
        }
        String val = getRandomValue(subType, random);
        sdoc.addField(field, "\"" + key + "\",\"" + val + "\"");
        docs.add(sdoc);
      }
    }
    return docs;
  }

  private static String getRandomValue(NumberType nbType, Random random) {
    String str = null;

    if (nbType != null) {
      Double dbl = random.nextDouble() * 10;
      switch (nbType) {
        case NumberType.INTEGER:
          str = String.valueOf(dbl.intValue());
          break;
        case NumberType.LONG:
          str = String.valueOf(dbl.longValue());
          break;
        case NumberType.FLOAT:
          str = String.valueOf(dbl.floatValue());
          break;
        case NumberType.DATE:
          Instant instant =
              Instant.ofEpochSecond(random.nextInt(0, (int) Instant.now().getEpochSecond()));
          Date dt = Date.from(instant);
          str = format.format(dt);
          break;
        default:
          str = String.valueOf(dbl.doubleValue());
          break;
      }
    } else {
      str = RandomStrings.randomAsciiAlphanumOfLengthBetween(random, 5, 10);
    }
    return str;
  }
}
