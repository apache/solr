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
package org.apache.solr.common.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.util.EntityUtils;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.LinkedHashMapWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.MapWriterMap;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SpecProvider;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.params.CommonParams;
import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @SuppressWarnings({"rawtypes"})
  public static Map getDeepCopy(Map<?, ?> map, int maxDepth) {
    return getDeepCopy(map, maxDepth, true, false);
  }

  @SuppressWarnings({"rawtypes"})
  public static Map getDeepCopy(Map<?, ?> map, int maxDepth, boolean mutable) {
    return getDeepCopy(map, maxDepth, mutable, false);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static Map getDeepCopy(Map<?, ?> map, int maxDepth, boolean mutable, boolean sorted) {
    if (map == null) return null;
    if (maxDepth < 1) return map;
    Map copy;
    if (sorted) {
      copy = new TreeMap<>();
    } else {
      copy =
          map instanceof LinkedHashMap
              ? CollectionUtil.newLinkedHashMap(map.size())
              : CollectionUtil.newHashMap(map.size());
    }
    for (Object o : map.entrySet()) {
      Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
      copy.put(e.getKey(), makeDeepCopy(e.getValue(), maxDepth, mutable, sorted));
    }
    return mutable ? copy : Collections.unmodifiableMap(copy);
  }

  public static void forEachMapEntry(
      Object o, String path, @SuppressWarnings({"rawtypes"}) BiConsumer fun) {
    Object val = Utils.getObjectByPath(o, false, path);
    forEachMapEntry(val, fun);
  }

  public static void forEachMapEntry(
      Object o, List<String> path, @SuppressWarnings({"rawtypes"}) BiConsumer fun) {
    Object val = Utils.getObjectByPath(o, false, path);
    forEachMapEntry(val, fun);
  }

  @SuppressWarnings({"unchecked"})
  public static void forEachMapEntry(Object o, @SuppressWarnings({"rawtypes"}) BiConsumer fun) {
    if (o instanceof MapWriter) {
      MapWriter m = (MapWriter) o;
      try {
        m.writeMap(
            new MapWriter.EntryWriter() {
              @Override
              public MapWriter.EntryWriter put(CharSequence k, Object v) {
                fun.accept(k, v);
                return this;
              }
            });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else if (o instanceof Map) {
      ((Map) o).forEach((k, v) -> fun.accept(k, v));
    }
  }

  private static Object makeDeepCopy(Object v, int maxDepth, boolean mutable, boolean sorted) {
    if (v instanceof MapWriter && maxDepth > 1) {
      v = ((MapWriter) v).toMap(new LinkedHashMap<>());
    } else if (v instanceof IteratorWriter && maxDepth > 1) {
      List<Object> l = ((IteratorWriter) v).toList(new ArrayList<>());
      if (sorted) {
        l.sort(null);
      }
      v = l;
    }

    if (v instanceof Map) {
      v = getDeepCopy((Map) v, maxDepth - 1, mutable, sorted);
    } else if (v instanceof Collection) {
      v = getDeepCopy((Collection<?>) v, maxDepth - 1, mutable, sorted);
    }
    return v;
  }

  public static InputStream toJavabin(Object o) throws IOException {
    try (final JavaBinCodec jbc = new JavaBinCodec()) {
      BAOS baos = new BAOS();
      jbc.marshal(o, baos);
      return new ByteArrayInputStream(baos.getbuf(), 0, baos.size());
    }
  }

  public static Object fromJavabin(byte[] bytes) throws IOException {
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      return jbc.unmarshal(bytes);
    }
  }

  public static Collection<?> getDeepCopy(Collection<?> c, int maxDepth, boolean mutable) {
    return getDeepCopy(c, maxDepth, mutable, false);
  }

  public static Collection<?> getDeepCopy(
      Collection<?> c, int maxDepth, boolean mutable, boolean sorted) {
    if (c == null || maxDepth < 1) return c;
    Collection<Object> result =
        c instanceof Set ? (sorted ? new TreeSet<>() : new HashSet<>()) : new ArrayList<>();
    for (Object o : c)
      result.add(makeDeepCopy(o, maxDepth, mutable, sorted)); // TODO should this be maxDepth - 1?
    if (sorted && (result instanceof List)) {
      ((List<Object>) result).sort(null);
    }
    return mutable ? result : Collections.unmodifiableCollection(result);
  }

  public static void writeJson(Object o, OutputStream os, boolean indent) throws IOException {
    writeJson(o, new OutputStreamWriter(os, UTF_8), indent).flush();
  }

  public static Writer writeJson(Object o, Writer writer, boolean indent) throws IOException {
    try (SolrJSONWriter jsonWriter = new SolrJSONWriter(writer)) {
      jsonWriter.setIndent(indent).writeObj(o);
    }
    return writer;
  }

  public static byte[] toJSON(Object o) {
    if (o == null) return new byte[0];
    CharArr out = new CharArr();
    new JSONWriter(out, 2).write(o); // indentation by default
    return toUTF8(out);
  }

  public static String toJSONString(Object o) {
    return new String(toJSON(o), StandardCharsets.UTF_8);
  }

  public static byte[] toUTF8(CharArr out) {
    byte[] arr = new byte[out.size() * 3];
    int nBytes = ByteUtils.UTF16toUTF8(out, 0, out.size(), arr, 0);
    return Arrays.copyOf(arr, nBytes);
  }

  public static Object fromJSON(byte[] utf8) {
    // Need below check in both fromJSON methods since
    // utf8.length returns a NPE without this check.
    if (utf8 == null || utf8.length == 0) {
      return Collections.emptyMap();
    }
    return fromJSON(utf8, 0, utf8.length);
  }

  public static Object fromJSON(byte[] utf8, int offset, int length) {
    return fromJSON(utf8, offset, length, STANDARDOBJBUILDER);
  }

  public static Object fromJSON(
      byte[] utf8, int offset, int length, Function<JSONParser, ObjectBuilder> fun) {
    if (utf8 == null || utf8.length == 0 || length == 0) {
      return Collections.emptyMap();
    }
    // convert directly from bytes to chars
    // and parse directly from that instead of going through
    // intermediate strings or readers
    CharArr chars = new CharArr();
    ByteUtils.UTF8toUTF16(utf8, offset, length, chars);
    JSONParser parser = new JSONParser(chars.getArray(), chars.getStart(), chars.length());
    parser.setFlags(
        parser.getFlags()
            | JSONParser.ALLOW_MISSING_COLON_COMMA_BEFORE_OBJECT
            | JSONParser.OPTIONAL_OUTER_BRACES);
    try {
      return fun.apply(parser).getValStrict();
    } catch (IOException e) {
      throw new RuntimeException(e); // should never happen w/o using real IO
    }
  }

  public static <V> Map<String, V> makeMap(String k1, V v1, String k2, V v2) {
    Map<String, V> map = new LinkedHashMap<>(2, 1);
    map.put(k1, v1);
    map.put(k2, v2);
    return map;
  }

  public static Map<String, Object> makeMap(Object... keyVals) {
    return _makeMap(keyVals);
  }

  public static Map<String, String> makeMap(String... keyVals) {
    return _makeMap(keyVals);
  }

  private static <T> Map<String, T> _makeMap(T[] keyVals) {
    if ((keyVals.length & 0x01) != 0) {
      throw new IllegalArgumentException("arguments should be key,value");
    }
    Map<String, T> propMap =
        new LinkedHashMap<>(); // Cost of oversizing LHM is low, don't compute initialCapacity
    for (int i = 0; i < keyVals.length; i += 2) {
      propMap.put(String.valueOf(keyVals[i]), keyVals[i + 1]);
    }
    return propMap;
  }

  public static Object fromJSON(InputStream is) {
    return fromJSON(new InputStreamReader(is, UTF_8));
  }

  public static Object fromJSON(Reader is) {
    try {
      return STANDARDOBJBUILDER.apply(getJSONParser(is)).getValStrict();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parse error", e);
    }
  }

  public static final Function<JSONParser, ObjectBuilder> STANDARDOBJBUILDER =
      jsonParser -> {
        try {
          return new ObjectBuilder(jsonParser);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };
  public static final Function<JSONParser, ObjectBuilder> MAPWRITEROBJBUILDER =
      jsonParser -> {
        try {
          return new ObjectBuilder(jsonParser) {
            @Override
            public Object newObject() {
              return new LinkedHashMapWriter<>();
            }
          };
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };

  public static final Function<JSONParser, ObjectBuilder> MAPOBJBUILDER =
      jsonParser -> {
        try {
          return new ObjectBuilder(jsonParser) {
            @Override
            public Object newObject() {
              return new HashMap<>();
            }
          };
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };

  /**
   * Util function to convert {@link Object} to {@link String} Specially handles {@link Date} to
   * string conversion
   */
  public static final Function<Object, String> OBJECT_TO_STRING =
      obj ->
          ((obj instanceof Date)
              ? Objects.toString(((Date) obj).toInstant())
              : Objects.toString(obj));

  public static Object fromJSON(
      InputStream is, Function<JSONParser, ObjectBuilder> objBuilderProvider) {
    try {
      return objBuilderProvider
          .apply(getJSONParser((new InputStreamReader(is, StandardCharsets.UTF_8))))
          .getValStrict();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parse error", e);
    }
  }

  public static Object fromJSONResource(ClassLoader loader, String resourceName) {
    final URL resource = loader.getResource(resourceName);
    if (null == resource) {
      throw new IllegalArgumentException("invalid resource name: " + resourceName);
    }
    try (InputStream stream = resource.openStream()) {
      return fromJSON(stream);
    } catch (IOException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Resource error: " + e.getMessage(), e);
    }
  }

  public static JSONParser getJSONParser(Reader reader) {
    JSONParser parser = new JSONParser(reader);
    parser.setFlags(
        parser.getFlags()
            | JSONParser.ALLOW_MISSING_COLON_COMMA_BEFORE_OBJECT
            | JSONParser.OPTIONAL_OUTER_BRACES);
    return parser;
  }

  public static Object fromJSONString(String json) {
    try {
      return STANDARDOBJBUILDER.apply(getJSONParser(new StringReader(json))).getValStrict();
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Parse error : " + json, e);
    }
  }

  public static Object getObjectByPath(Object root, boolean onlyPrimitive, String hierarchy) {
    if (hierarchy == null) return getObjectByPath(root, onlyPrimitive, singletonList(null));
    List<String> parts = StrUtils.splitSmart(hierarchy, '/', true);
    return getObjectByPath(root, onlyPrimitive, parts);
  }

  public static boolean setObjectByPath(Object root, String hierarchy, Object value) {
    List<String> parts = StrUtils.splitSmart(hierarchy, '/', true);
    return setObjectByPath(root, parts, value);
  }

  public static boolean setObjectByPath(Object root, List<String> hierarchy, Object value) {
    if (root == null) return false;
    if (!isMapLike(root)) throw new RuntimeException("must be a Map or NamedList");
    Object obj = root;
    for (int i = 0; i < hierarchy.size(); i++) {
      int idx = -2; // -1 means append to list, -2 means not found
      String s = hierarchy.get(i);
      if (s.endsWith("]")) {
        Matcher matcher = ARRAY_ELEMENT_INDEX.matcher(s);
        if (matcher.find()) {
          s = matcher.group(1);
          idx = Integer.parseInt(matcher.group(2));
        }
      }
      if (i < hierarchy.size() - 1) {
        Object o = getVal(obj, s, -1);
        if (o == null) return false;
        if (idx > -1) {
          List<?> l = (List<?>) o;
          o = idx < l.size() ? l.get(idx) : null;
        }
        if (!isMapLike(o)) return false;
        obj = o;
      } else {
        if (idx == -2) {
          if (obj instanceof NamedList) {
            @SuppressWarnings("unchecked")
            NamedList<Object> namedList = (NamedList<Object>) obj;
            int location = namedList.indexOf(s, 0);
            if (location == -1) namedList.add(s, value);
            else namedList.setVal(location, value);
          } else if (obj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = ((Map<String, Object>) obj);
            map.put(s, value);
          }
          return true;
        } else {
          Object v = getVal(obj, s, -1);
          if (v instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) v;
            if (idx == -1) {
              list.add(value);
            } else {
              if (idx < list.size()) list.set(idx, value);
              else return false;
            }
            return true;
          } else {
            return false;
          }
        }
      }
    }

    return false;
  }

  public static Object getObjectByPath(Object root, boolean onlyPrimitive, List<String> hierarchy) {
    if (root == null) return null;
    if (!isMapLike(root)) return null;
    Object obj = root;
    for (int i = 0; i < hierarchy.size(); i++) {
      int idx = -1;
      String s = hierarchy.get(i);
      if (s != null && s.endsWith("]")) {
        Matcher matcher = ARRAY_ELEMENT_INDEX.matcher(s);
        if (matcher.find()) {
          s = matcher.group(1);
          idx = Integer.parseInt(matcher.group(2));
        }
      }
      if (i < hierarchy.size() - 1) {
        Object o = getVal(obj, s, -1);
        if (o == null) return null;
        if (idx > -1) {
          if (o instanceof List) {
            List<?> l = (List<?>) o;
            o = idx < l.size() ? l.get(idx) : null;
          } else if (o instanceof IteratorWriter) {
            o = getValueAt((IteratorWriter) o, idx);
          } else if (o instanceof MapWriter) {
            o = getVal(o, null, idx);
          } else if (o instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) o;
            o = getVal(new MapWriterMap(map), null, idx);
          } else {
            return null;
          }
        }
        if (!isMapLike(o)) return null;
        obj = o;
      } else {
        Object val = getVal(obj, s, -1);
        if (val == null) return null;
        if (idx > -1) {
          if (val instanceof IteratorWriter) {
            val = getValueAt((IteratorWriter) val, idx);
          } else {
            List<?> l = (List<?>) val;
            val = idx < l.size() ? l.get(idx) : null;
          }
        }
        if (onlyPrimitive && isMapLike(val)) {
          return null;
        }
        return val;
      }
    }

    return false;
  }

  private static Object getValueAt(IteratorWriter iteratorWriter, int idx) {
    Object[] result = new Object[1];
    try {
      iteratorWriter.writeIter(
          new IteratorWriter.ItemWriter() {
            int i = -1;

            @Override
            public IteratorWriter.ItemWriter add(Object o) {
              ++i;
              if (i > idx) return this;
              if (i == idx) result[0] = o;
              return this;
            }
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return result[0];
  }

  static class MapWriterEntry<V> extends AbstractMap.SimpleEntry<CharSequence, V>
      implements MapWriter, Map.Entry<CharSequence, V> {
    MapWriterEntry(CharSequence key, V value) {
      super(key, value);
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put("key", getKey());
      ew.put("value", getValue());
    }
  }

  private static boolean isMapLike(Object o) {
    return o instanceof Map || o instanceof NamedList || o instanceof MapWriter;
  }

  private static Object getVal(Object obj, String key, int idx) {
    if (obj instanceof MapWriter) {
      Object[] result = new Object[1];
      try {
        ((MapWriter) obj)
            .writeMap(
                new MapWriter.EntryWriter() {
                  int count = -1;

                  @Override
                  public MapWriter.EntryWriter put(CharSequence k, Object v) {
                    if (result[0] != null) return this;
                    if (idx < 0) {
                      if (key.contentEquals(k)) result[0] = v;
                    } else {
                      if (++count == idx) result[0] = new MapWriterEntry<>(k, v);
                    }
                    return this;
                  }
                });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return result[0];
    } else if (obj instanceof Map) return ((Map<?, ?>) obj).get(key);
    else throw new RuntimeException("must be a NamedList or Map");
  }

  /**
   * If the passed entity has content, make sure it is fully read and closed.
   *
   * @param entity to consume or null
   */
  public static void consumeFully(HttpEntity entity) {
    if (entity != null) {
      try {
        // make sure the stream is full read
        readFully(entity.getContent());
      } catch (UnsupportedOperationException e) {
        // nothing to do then
      } catch (IOException e) {
        // quiet
      } finally {
        // close the stream
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  /**
   * Make sure the InputStream is fully read.
   *
   * @param is to read
   * @throws IOException on problem with IO
   */
  public static void readFully(InputStream is) throws IOException {
    is.skip(is.available());
    while (is.read() != -1) {}
  }

  public static final Pattern ARRAY_ELEMENT_INDEX = Pattern.compile("(\\S*?)\\[([-]?\\d+)\\]");

  public static SpecProvider getSpec(final String name) {
    return () -> {
      return ValidatingJsonMap.parse(
          CommonParams.APISPEC_LOCATION + name + ".json", CommonParams.APISPEC_LOCATION);
    };
  }

  public static String parseMetricsReplicaName(String collectionName, String coreName) {
    if (collectionName == null || !coreName.startsWith(collectionName)) {
      return null;
    } else {
      // split "collection1_shard1_1_replica1" into parts
      if (coreName.length() > collectionName.length()) {
        String str = coreName.substring(collectionName.length() + 1);
        int pos = str.lastIndexOf("_replica");
        if (pos == -1) { // ?? no _replicaN part ??
          return str;
        } else {
          return str.substring(pos + 1);
        }
      } else {
        return null;
      }
    }
  }

  /**
   * Applies one json over other. The 'input' is applied over the sink The values in input isapplied
   * over the values in 'sink' . If a value is 'null' that value is removed from sink
   *
   * @param sink the original json object to start with. Ensure that this Map is mutable
   * @param input the json with new values
   * @return whether there was any change made to sink or not.
   */
  @SuppressWarnings({"unchecked"})
  public static boolean mergeJson(Map<String, Object> sink, Map<String, Object> input) {
    boolean isModified = false;
    for (Map.Entry<String, Object> e : input.entrySet()) {
      if (sink.get(e.getKey()) != null) {
        Object sinkVal = sink.get(e.getKey());
        if (e.getValue() == null) {
          sink.remove(e.getKey());
          isModified = true;
        } else {
          if (e.getValue() instanceof Map) {
            Map<String, Object> mapInputVal = (Map<String, Object>) e.getValue();
            if (sinkVal instanceof Map) {
              if (mergeJson((Map<String, Object>) sinkVal, mapInputVal)) isModified = true;
            } else {
              sink.put(e.getKey(), mapInputVal);
              isModified = true;
            }
          } else {
            sink.put(e.getKey(), e.getValue());
            isModified = true;
          }
        }
      } else if (e.getValue() != null) {
        sink.put(e.getKey(), e.getValue());
        isModified = true;
      }
    }

    return isModified;
  }

  /**
   * Given a URL string with or without a scheme, return a new URL with the correct scheme applied.
   *
   * @param url A URL to change the scheme (http|https)
   * @return A new URL with the correct scheme
   */
  public static String applyUrlScheme(final String url, final String urlScheme) {
    Objects.requireNonNull(url, "URL must not be null!");
    // heal an incorrect scheme if needed, otherwise return null indicating no change
    final int at = url.indexOf("://");
    return (at == -1) ? (urlScheme + "://" + url) : urlScheme + url.substring(at);
  }

  public static String getBaseUrlForNodeName(final String nodeName, final String urlScheme) {
    return getBaseUrlForNodeName(nodeName, urlScheme, false);
  }

  public static String getBaseUrlForNodeName(
      final String nodeName, final String urlScheme, boolean isV2) {
    final int colonAt = nodeName.indexOf(':');
    if (colonAt == -1) {
      throw new IllegalArgumentException(
          "nodeName does not contain expected ':' separator: " + nodeName);
    }

    final int _offset = nodeName.indexOf('_', colonAt);
    if (_offset < 0) {
      throw new IllegalArgumentException(
          "nodeName does not contain expected '_' separator: " + nodeName);
    }
    final String hostAndPort = nodeName.substring(0, _offset);
    final String path = URLDecoder.decode(nodeName.substring(1 + _offset), UTF_8);
    return urlScheme + "://" + hostAndPort + (path.isEmpty() ? "" : ("/" + (isV2 ? "api" : path)));
  }

  public static long time(TimeSource timeSource, TimeUnit unit) {
    return unit.convert(timeSource.getTimeNs(), TimeUnit.NANOSECONDS);
  }

  public static long timeElapsed(TimeSource timeSource, long start, TimeUnit unit) {
    return unit.convert(timeSource.getTimeNs() - NANOSECONDS.convert(start, unit), NANOSECONDS);
  }

  public static <T> T handleExp(Logger logger, T def, Callable<T> c) {
    try {
      return c.call();
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
    return def;
  }

  public interface InputStreamConsumer<T> {

    T accept(InputStream is) throws IOException;
  }

  public static final InputStreamConsumer<?> JAVABINCONSUMER =
      is -> new JavaBinCodec().unmarshal(is);
  public static final InputStreamConsumer<?> JSONCONSUMER = Utils::fromJSON;

  public static InputStreamConsumer<ByteBuffer> newBytesConsumer(int maxSize) {
    return is -> {
      try (BAOS bos = new BAOS()) {
        long sz = 0;
        int next = is.read();
        while (next > -1) {
          if (++sz > maxSize) throw new BufferOverflowException();
          bos.write(next);
          next = is.read();
        }
        bos.flush();
        return ByteBuffer.wrap(bos.getbuf(), 0, bos.size());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  public static <T> T executeGET(HttpClient client, String url, InputStreamConsumer<T> consumer)
      throws SolrException {
    return executeHttpMethod(client, url, consumer, new HttpGet(url));
  }

  public static <T> T executeHttpMethod(
      HttpClient client, String url, InputStreamConsumer<T> consumer, HttpRequestBase httpMethod) {
    T result = null;
    HttpResponse rsp = null;
    try {
      rsp = client.execute(httpMethod);
    } catch (IOException e) {
      log.error("Error in request to url : {}", url, e);
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, "Error sending request");
    }
    int statusCode = rsp.getStatusLine().getStatusCode();
    if (statusCode != 200) {
      try {
        log.error(
            "Failed a request to: {}, status: {}, body: {}",
            url,
            rsp.getStatusLine(),
            EntityUtils.toString(rsp.getEntity(), StandardCharsets.UTF_8)); // nowarn
      } catch (IOException e) {
        log.error("could not print error", e);
      }
      throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode), "Unknown error");
    }
    HttpEntity entity = rsp.getEntity();
    try {
      InputStream is = entity.getContent();
      if (consumer != null) {

        result = consumer.accept(is);
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, e);
    } finally {
      Utils.consumeFully(entity);
    }
    return result;
  }

  /**
   * Convert the input object to a map, writing only those fields annotated with a {@link
   * JsonProperty} annotation
   *
   * @param ew an {@link org.apache.solr.common.MapWriter.EntryWriter} to do the actual map
   *     insertion/writing
   * @param o the object to be converted
   */
  public static void reflectWrite(MapWriter.EntryWriter ew, Object o) {
    reflectWrite(
        ew,
        o,
        field -> field.getAnnotation(JsonProperty.class) != null,
        null, // No catch-all/unknown-field support for objects annotated with our mimic
        // annotations.
        field -> {
          final JsonProperty prop = field.getAnnotation(JsonProperty.class);
          return prop.value().isEmpty() ? field.getName() : prop.value();
        });
  }

  public static final String CATCH_ALL_PROPERTIES_METHOD_NAME = "unknownProperties";

  /**
   * Return a writable object that will be serialized using the reflection-friendly properties of
   * the class, notably the fields that have the {@link JsonProperty} annotation.
   *
   * <p>If the class has no reflection-friendly fields, then it will be serialized as a string,
   * using the class's name and {@code toString()} method.
   *
   * @param o the object to get a serializable version of
   * @return a serializable version of the object
   */
  public static Object getReflectWriter(Object o) {
    List<FieldWriter> fieldWriters = null;
    try {
      fieldWriters =
          Utils.getReflectData(
              o.getClass(),
              // TODO Should we be lenient here and accept both the Jackson and our homegrown
              // annotation?
              field ->
                  field.getAnnotation(com.fasterxml.jackson.annotation.JsonProperty.class) != null,
              JsonAnyGetter.class,
              field -> {
                final com.fasterxml.jackson.annotation.JsonProperty prop =
                    field.getAnnotation(com.fasterxml.jackson.annotation.JsonProperty.class);
                return prop.value().isEmpty() ? field.getName() : prop.value();
              });
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    if (!fieldWriters.isEmpty()) {
      return new DelegateReflectWriter(o, fieldWriters);
    } else {
      // Do not serialize an empty class, use a string representation instead.
      // This is because the class is likely not using the serialization annotations
      // and still expects to provide some information.
      return o.getClass().getName() + ':' + o;
    }
  }

  /**
   * Convert an input object to a map, writing only those fields that match a provided {@link
   * Predicate}
   *
   * @param ew an {@link org.apache.solr.common.MapWriter.EntryWriter} to do the actual map
   *     insertion/writing
   * @param o the object to be converted
   * @param fieldFilterer a predicate used to identify which fields of the object to write
   * @param catchAllAnnotation the annotation used to identify a method that can return a Map of
   *     "catch-all" properties. Method is expected to be named "unknownProperties"
   * @param fieldNamer a callback that allows changing field names
   */
  public static void reflectWrite(
      MapWriter.EntryWriter ew,
      Object o,
      Predicate<Field> fieldFilterer,
      Class<? extends Annotation> catchAllAnnotation,
      Function<Field, String> fieldNamer) {
    List<FieldWriter> fieldWriters = null;
    try {
      fieldWriters = getReflectData(o.getClass(), fieldFilterer, catchAllAnnotation, fieldNamer);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    reflectWrite(ew, o, fieldWriters);
  }

  private static List<FieldWriter> getReflectData(
      Class<?> c,
      Predicate<Field> fieldFilterer,
      Class<? extends Annotation> catchAllAnnotation,
      Function<Field, String> fieldNamer)
      throws IllegalAccessException {
    boolean sameClassLoader = c.getClassLoader() == Utils.class.getClassLoader();
    // we should not cache the class references of objects loaded from packages because they will
    // not get garbage collected
    // TODO fix that later
    List<FieldWriter> cachedReflectData = sameClassLoader ? storedReflectData.get(c) : null;
    MethodHandles.Lookup lookup = MethodHandles.publicLookup();
    if (cachedReflectData == null) {
      cachedReflectData = addTraditionalFieldWriters(c, lookup, fieldFilterer, fieldNamer);
      if (sameClassLoader) {
        storedReflectData.put(c, Collections.unmodifiableList(new ArrayList<>(cachedReflectData)));
      }
    }

    // Add in any 'catch-all' methods used to support additional or unknown properties.
    // (These can't be cached, as they would change request-by-request.)
    final List<FieldWriter> mutableFieldWriters = new ArrayList<>(cachedReflectData);
    addCatchAllFieldWriter(mutableFieldWriters, catchAllAnnotation, lookup, c);
    return Collections.unmodifiableList(mutableFieldWriters);
  }

  /**
   * Convert an input object to a map, using the provided {@link FieldWriter}s.
   *
   * @param ew an {@link org.apache.solr.common.MapWriter.EntryWriter} to do the actual map
   *     insertion/writing
   * @param o the object to be converted
   * @param fieldWriters a list of fields to write and how to write them
   */
  private static void reflectWrite(
      MapWriter.EntryWriter ew, Object o, List<FieldWriter> fieldWriters) {
    for (FieldWriter fieldWriter : fieldWriters) {
      try {
        fieldWriter.write(ew, o);
      } catch (Throwable e) {
        throw new RuntimeException(e);
        // should not happen
      }
    }
  }

  private static List<FieldWriter> addTraditionalFieldWriters(
      Class<?> c,
      MethodHandles.Lookup lookup,
      Predicate<Field> fieldFilterer,
      Function<Field, String> fieldNamer)
      throws IllegalAccessException {

    final ArrayList<FieldWriter> fieldWriters = new ArrayList<>();
    for (Field field : lookup.accessClass(c).getFields()) {
      if (!fieldFilterer.test(field)) continue;
      int modifiers = field.getModifiers();
      if (Modifier.isPublic(modifiers) && !Modifier.isStatic(modifiers)) {
        final String fname = fieldNamer.apply(field);
        try {
          if (field.getType() == int.class) {
            MethodHandle mh = lookup.findGetter(c, field.getName(), int.class);
            fieldWriters.add((ew, inst) -> ew.put(fname, (int) mh.invoke(inst)));
          } else if (field.getType() == long.class) {
            MethodHandle mh = lookup.findGetter(c, field.getName(), long.class);
            fieldWriters.add((ew, inst) -> ew.put(fname, (long) mh.invoke(inst)));
          } else if (field.getType() == boolean.class) {
            MethodHandle mh = lookup.findGetter(c, field.getName(), boolean.class);
            fieldWriters.add((ew, inst) -> ew.put(fname, (boolean) mh.invoke(inst)));
          } else if (field.getType() == double.class) {
            MethodHandle mh = lookup.findGetter(c, field.getName(), double.class);
            fieldWriters.add((ew, inst) -> ew.put(fname, (double) mh.invoke(inst)));
          } else if (field.getType() == float.class) {
            MethodHandle mh = lookup.findGetter(c, field.getName(), float.class);
            fieldWriters.add((ew, inst) -> ew.put(fname, (float) mh.invoke(inst)));
          } else {
            MethodHandle mh = lookup.findGetter(c, field.getName(), field.getType());
            fieldWriters.add((ew, inst) -> ew.putIfNotNull(fname, mh.invoke(inst)));
          }
        } catch (NoSuchFieldException e) {
          // this is unlikely
          throw new RuntimeException(e);
        }
      }
    }
    return fieldWriters;
  }

  // Look for a method titled $CATCH_ALL_PROPERTIES_METHOD_NAME annotated with the expected flag
  // annotation that returns a Map of "unknown properties" that should also be written out.
  private static void addCatchAllFieldWriter(
      List<FieldWriter> fieldWriters,
      Class<? extends Annotation> catchAllAnnotation,
      MethodHandles.Lookup lookup,
      Class<?> c)
      throws IllegalAccessException {

    if (catchAllAnnotation != null) {
      try {
        final Method catchAllMethod =
            lookup.accessClass(c).getDeclaredMethod(CATCH_ALL_PROPERTIES_METHOD_NAME);
        if (catchAllMethod.getAnnotation(catchAllAnnotation) != null) {
          final MethodType catchAllMethodType = MethodType.methodType(Map.class);
          final MethodHandle catchAllHandle =
              lookup.findVirtual(c, CATCH_ALL_PROPERTIES_METHOD_NAME, catchAllMethodType);
          fieldWriters.add(
              (ew, inst) -> {
                final Map<String, Object> unknownProperties =
                    (Map<String, Object>) catchAllHandle.invoke(inst);
                for (Map.Entry<String, Object> entry : unknownProperties.entrySet()) {
                  ew.put(entry.getKey(), entry.getValue());
                }
              });
        }
      } catch (NoSuchMethodException e) {
        // No-op - if the object has no 'unknownProperties' method, then nothing needs to be done.
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static Map<String, Object> convertToMap(MapWriter m, Map<String, Object> map) {
    try {
      m.writeMap(
          new MapWriter.EntryWriter() {
            @Override
            public MapWriter.EntryWriter put(CharSequence k, Object v) {
              return writeEntry(k, v);
            }

            private MapWriter.EntryWriter writeEntry(CharSequence k, Object v) {
              if (v instanceof MapWriter) v = ((MapWriter) v).toMap(new LinkedHashMap<>());
              if (v instanceof IteratorWriter) v = ((IteratorWriter) v).toList(new ArrayList<>());
              if (v instanceof Iterable) {
                List lst = new ArrayList();
                for (Object vv : (Iterable) v) {
                  if (vv instanceof MapWriter) vv = ((MapWriter) vv).toMap(new LinkedHashMap<>());
                  if (vv instanceof IteratorWriter)
                    vv = ((IteratorWriter) vv).toList(new ArrayList<>());
                  lst.add(vv);
                }
                v = lst;
              }
              if (v instanceof Map) {
                Map map = new LinkedHashMap();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) v).entrySet()) {
                  Object vv = entry.getValue();
                  if (vv instanceof MapWriter) vv = ((MapWriter) vv).toMap(new LinkedHashMap<>());
                  if (vv instanceof IteratorWriter)
                    vv = ((IteratorWriter) vv).toList(new ArrayList<>());
                  map.put(entry.getKey(), vv);
                }
                v = map;
              }
              map.put(k == null ? null : k.toString(), v);
              // note: It'd be nice to assert that there is no previous value at 'k' but it's
              // possible the passed map is already populated and the intention is to overwrite.
              return this;
            }
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return map;
  }

  private static final Map<Class<?>, List<FieldWriter>> storedReflectData =
      new ConcurrentHashMap<>();

  public static class DelegateReflectWriter implements MapWriter {
    private final Object object;
    private final List<Utils.FieldWriter> fieldWriters;

    DelegateReflectWriter(Object object, List<Utils.FieldWriter> fieldWriters) {
      this.object = object;
      this.fieldWriters = fieldWriters;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      Utils.reflectWrite(ew, object, fieldWriters);
    }
  }

  interface FieldWriter {
    void write(MapWriter.EntryWriter ew, Object inst) throws Throwable;
  }

  public static class BAOS extends ByteArrayOutputStream {
    public ByteBuffer getByteBuffer() {
      return ByteBuffer.wrap(super.buf, 0, super.count);
    }

    /*
     * A hack to get access to the protected internal buffer and avoid an additional copy
     */
    public byte[] getbuf() {
      return super.buf;
    }
  }

  public static ByteBuffer toByteArray(InputStream is) throws IOException {
    return toByteArray(is, Integer.MAX_VALUE);
  }

  /**
   * Reads an input stream into a byte array
   *
   * @param is the input stream
   * @return the byte array
   * @throws IOException If there is a low-level I/O error.
   */
  public static ByteBuffer toByteArray(InputStream is, long maxSize) throws IOException {
    try (BAOS bos = new BAOS()) {
      long sz = 0;
      int next = is.read();
      while (next > -1) {
        if (++sz > maxSize) {
          throw new BufferOverflowException();
        }
        bos.write(next);
        next = is.read();
      }
      bos.flush();
      return bos.getByteBuffer();
    }
  }
}
