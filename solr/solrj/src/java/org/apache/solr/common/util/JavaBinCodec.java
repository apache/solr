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

import static org.apache.solr.common.util.ByteArrayUtf8CharSequence.convertCharSeq;

import com.google.errorprone.annotations.DoNotCall;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.solr.common.ConditionalKeyMapWriter;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.IteratorWriter.ItemWriter;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.PushWriter;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CommonParams;
import org.eclipse.jetty.io.RuntimeIOException;
import org.noggit.CharArr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines a space-efficient serialization/deserialization format for transferring data.
 *
 * <p>JavaBinCodec has built in support many commonly used types. This includes primitive types
 * (boolean, byte, short, double, int, long, float), common Java containers/utilities (Date, Map,
 * Collection, Iterator, String, Object[], byte[]), and frequently used Solr types ({@link
 * NamedList}, {@link SolrDocument}, {@link SolrDocumentList}). Each of the above types has a pair
 * of associated methods which read and write that type to a stream. b
 *
 * <p>Classes that aren't supported natively can still be serialized/deserialized by providing an
 * {@link JavaBinCodec.ObjectResolver} object that knows how to work with the unsupported class.
 * This allows {@link JavaBinCodec} to be used to marshall/unmarshall arbitrary content.
 *
 * <p>NOTE -- {@link JavaBinCodec} instances cannot be reused for more than one marshall or
 * unmarshall operation.
 */
public class JavaBinCodec implements PushWriter {

  // WARNING! this class is heavily optimized and balancing a wide variety of use cases, data, and tradeoffs
  //          please be thorough and careful with changes

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String IGNORING_BOOST_AS_INDEX_TIME_BOOSTS_ARE_NOT_SUPPORTED_ANYMORE =
      "Ignoring boost: {} as index-time boosts are not supported anymore";
  private static final AtomicBoolean WARNED_ABOUT_INDEX_TIME_BOOSTS = new AtomicBoolean();

  // Solr encode / decode is only a win at fairly small values
  private static final int MAX_SZ_BEFORE_STRING_UTF8_ENCODE_OVER_SOLR_DEFAULT = 64;
  private static final int MAX_SZ_BEFORE_SLOW_MULTI_STAGE_UTF8_ENCODE = 131072;
  private static final int MAX_SZ_BEFORE_STRING_UTF8_ENCODE_OVER_SOLR =  Integer.getInteger("maxSzBeforeStringUTF8EncodeOverSolr", MAX_SZ_BEFORE_STRING_UTF8_ENCODE_OVER_SOLR_DEFAULT);

  private static final int MAX_STRING_SZ_TO_TRY_KEEPING_AS_UTF8_WO_CONVERT_BYTES = 1024 << 4; // can cause too much memory allocation if too large

  private static final byte VERSION = 2;

  private static final Integer ZERO = 0;
  private static final Float FLOAT_1 = 1f;
  protected static final Object END_OBJ = new Object();

  public static final byte NULL = 0,
      BOOL_TRUE = 1,
      BOOL_FALSE = 2,
      BYTE = 3,
      SHORT = 4,
      DOUBLE = 5,
      INT = 6,
      LONG = 7,
      FLOAT = 8,
      DATE = 9,
      MAP = 10,
      SOLRDOC = 11,
      SOLRDOCLST = 12,
      BYTEARR = 13,
      ITERATOR = 14,
      /** this is a special tag signals an end. No value is associated with it */
      END = 15,
      SOLRINPUTDOC = 16,
      MAP_ENTRY_ITER = 17,
      ENUM_FIELD_VALUE = 18,
      MAP_ENTRY = 19,
      UUID = 20, // This is reserved to be used only in LogCodec
      // types that combine tag + length (or other info) in a single byte
      TAG_AND_LEN = (byte) (1 << 5),
      STR = (byte) (1 << 5),
      SINT = (byte) (2 << 5),
      SLONG = (byte) (3 << 5),
      ARR = (byte) (4 << 5), //
      ORDERED_MAP = (byte) (5 << 5), // SimpleOrderedMap (a NamedList subclass, and more common)
      NAMED_LST = (byte) (6 << 5), // NamedList
      EXTERN_STRING = (byte) (7 << 5);

  private final ObjectResolver resolver;
  private final StringCache stringCache;
  private WritableDocFields writableDocFields;
  private boolean alreadyMarshalled;
  private boolean alreadyUnmarshalled;
  protected boolean readStringAsCharSeq = false;


  protected byte tagByte;

  // extern string structures
  private int stringsCount = 0;
  private HashMap<CharSequence, Integer> stringsMap;
  private ArrayList<CharSequence> stringsList;

  public final BinEntryWriter ew = new BinEntryWriter();

  // caching objects
  protected byte[] bytes;
  private final CharArr arr = new CharArr();
  private final StringBytes bytesRef = new StringBytes(null, 0, 0);

  // caching UTF-8 bytes and lazy conversion to UTF-16
  private Function<ByteArrayUtf8CharSequence, String> stringProvider;
  private BytesBlock bytesBlock;

  // internal stream wrapper classes
  private OutputStream out = null;
  private boolean isFastOutputStream;
  private byte[] buf;
  private int pos;

  public JavaBinCodec() {
    resolver = null;
    writableDocFields = null;
    stringCache = null;
  }

  public JavaBinCodec setReadStringAsCharSeq(boolean flag) {
    readStringAsCharSeq = flag;
    return this;
  }

  /**
   * Instantiates a new Java bin codec to be used as a PushWriter.
   *
   * <p>Ensure that close() is called explicitly after use.
   *
   * @param os the OutputStream to marshal to
   * @param resolver a resolver to be used for resolving Objects
   */
  public JavaBinCodec(OutputStream os, ObjectResolver resolver) {
    this.resolver = resolver;
    stringCache = null;
    initWrite(os, false);
  }

  /**
   * Instantiates a new Java bin codec to be used as a PushWriter.
   *
   * <p>Ensure that close() is called explicitly after use.
   *
   * @param os the OutputStream to marshal to
   * @param resolver a resolver to be used for resolving Objects
   * @param streamIsBuffered if true, no additional buffering for the OutputStream will be
   *     considered necessary.
   */
  public JavaBinCodec(
      OutputStream os,
      ObjectResolver resolver,
      boolean streamIsBuffered) {
    this.resolver = resolver;
    stringCache = null;
    initWrite(os, streamIsBuffered);
  }

  public JavaBinCodec(ObjectResolver resolver) {
    this(resolver, null);
  }

  public JavaBinCodec(ObjectResolver resolver, boolean externStringLimits) {
    this(resolver, null, externStringLimits);
  }

  public JavaBinCodec setWritableDocFields(WritableDocFields writableDocFields) {
    this.writableDocFields = writableDocFields;
    return this;
  }

  public JavaBinCodec(
      ObjectResolver resolver, StringCache stringCache, boolean externStringLimits) {
    this.resolver = resolver;
    this.stringCache = stringCache;
  }

  public JavaBinCodec(ObjectResolver resolver, StringCache stringCache) {
    this(resolver, stringCache, true);
  }

  public ObjectResolver getResolver() {
    return resolver;
  }


  /**
   * Marshals a given primitive or collection to an OutputStream.
   *
   * Collections may be nested amd {@link NamedList} is a supported collection.
   *
   * @param object the primitive or Collection to marshal
   * @param outputStream the OutputStream to marshal to
   * @throws IOException on IO failure
   */
  public void marshal(Object object, OutputStream outputStream) throws IOException {
    marshal(object, outputStream, false);
  }

  /**
   * Marshals a given primitive or collection to an OutputStream.
   *
   * Collections may be nested amd {@link NamedList} is a supported collection.
   *
   * @param object the primitive or Collection to marshal
   * @param outputStream the OutputStream to marshal to
   * @param streamIsBuffered a hint indicating whether the OutputStream is already buffered or not
   * @throws IOException on IO failure
   */
  public void marshal(Object object, OutputStream outputStream, boolean streamIsBuffered) throws IOException {
    try {
      initWrite(outputStream, streamIsBuffered);
      writeVal(object);
    } finally {
      alreadyMarshalled = true;
      flushBufferOS(this);
    }
  }

  private void initWrite(OutputStream os, boolean streamIsBuffered) {
    assert !alreadyMarshalled;

    if (streamIsBuffered) {
      initOutStream(os, null);
    } else {
      initOutStream(os);
    }
    try {
      writeByteToOS(this, VERSION);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  /**
   * Allows initializing the OutputStream independently of an Object to be marshalled.
   *
   * @param outputStream the OutputStream to marshal to
   */
  public void init(OutputStream outputStream) {
    if (outputStream instanceof FastOutputStream) {
      initOutStream(outputStream, null);
    } else {
      initOutStream(outputStream);
    }
  }

  /**
   * Unmarshalls a primitive or collection from a byte array to an Object.
   *
   * @param buffer a byte buffer containing the marshaled Object
   * @return the unmarshalled Object
   * @throws IOException on IO failure
   */
  public Object unmarshal(byte[] buffer) throws IOException {
    FastInputStream dis = initRead(buffer);
    return readVal(dis);
  }

  /**
   * Unmarshalls a primitive or collection from an InputStream to an Object.
   *
   * @param inputStream an InputStream containing the marshaled Object
   * @return the unmarshalled Object
   * @throws IOException on IO failure
   */
  public Object unmarshal(InputStream inputStream) throws IOException {
    FastInputStream dis = initRead(inputStream);
    return readVal(dis);
  }

  protected FastInputStream initRead(InputStream is) throws IOException {
    assert !alreadyUnmarshalled;
    FastInputStream dis =
        is instanceof FastInputStream ? (FastInputStream) is : FastInputStream.wrap(is);
    return init(dis);
  }

  protected FastInputStream initRead(byte[] buf) throws IOException {
    assert !alreadyUnmarshalled;
    FastInputStream dis = FastInputStream.wrap(new ByteArrayInputStream(buf));
    return init(dis);
  }

  protected FastInputStream init(FastInputStream dis) throws IOException {
    byte version = dis.readByte();
    if (version != VERSION) {
      throw new InvalidEncodingException(
          "Invalid version (expected "
              + VERSION
              + ", but "
              + version
              + ") or the data in not in 'javabin' format");
    }

    alreadyUnmarshalled = true;
    return dis;
  }

  static SimpleOrderedMap<Object> readOrderedMap(JavaBinCodec javaBinCodec,
      DataInputInputStream dis) throws IOException {
    int sz = readSize(javaBinCodec, dis);
    SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>(sz);
    for (int i = 0; i < sz; i++) {
      String name = (String) javaBinCodec.readVal(dis);
      Object val = javaBinCodec.readVal(dis);
      nl.add(name, val);
    }
    return nl;
  }

  public NamedList<Object> readNamedList(DataInputInputStream dis) throws IOException {
    int sz = readSize(this, dis);
    NamedList<Object> nl = new NamedList<>(sz);
    for (int i = 0; i < sz; i++) {
      String name = (String) readVal(dis);
      Object val = readVal(dis);
      nl.add(name, val);
    }
    return nl;
  }

  public static void writeNamedList(JavaBinCodec javaBinCodec, NamedList<?> nl) throws IOException {
    int size = nl.size();
    writeTag(javaBinCodec, nl instanceof SimpleOrderedMap ? ORDERED_MAP : NAMED_LST, size);
    for (int i = 0; i < size; i++) {
      String name = nl.getName(i);
      javaBinCodec.writeExternString(name);
      Object val = nl.getVal(i);
      javaBinCodec.writeVal(val);
    }
  }

  public void writeVal(Object val) throws IOException {
    if (writeKnownType(val)) {
      return;
    } else {
      ObjectResolver resolver;
      if (val instanceof ObjectResolver) {
        resolver = (ObjectResolver) val;
      } else {
        resolver = this.resolver;
      }
      if (resolver != null) {
        Object tmpVal = resolver.resolve(val, this);
        if (tmpVal == null) return; // null means the resolver took care of it fully
        if (writeKnownType(tmpVal)) return;
      }
    }

    /* NOTE: if the user of this codec doesn't want this (e.g. UpdateLog) they can supply an
    ObjectResolver that does something else, like throw an exception.*/
    writeVal(val.getClass().getName() + ':' + val);
  }

  public Object readVal(DataInputInputStream dis) throws IOException {
    tagByte = dis.readByte();
    return readObject(dis);
  }

  protected Object readObject(DataInputInputStream dis) throws IOException {

    /*
     NOTE: this method is broken up just a bit (ie checkLessCommonTypes) to get
     the method size under the limit for inlining by the C2 compiler

     FYI NOTE: if top 3 bits are clear, this is a normal tag
               i.e.  if ((tagByte & 0xe0) == 0)
    */

    // try type + size in single byte
    switch (tagByte >>> 5) {
      case STR >>> 5:
        return readStr(this, dis, stringCache, readStringAsCharSeq);
      case SINT >>> 5:
        return readSmallInt(this, dis);
      case SLONG >>> 5:
        return readSmallLong(this, dis);
      case ARR >>> 5:
        return readArray(this, dis);
      case ORDERED_MAP >>> 5:
        return readOrderedMap(this, dis);
      case NAMED_LST >>> 5:
        return readNamedList(dis);
      case EXTERN_STRING >>> 5:
        return readExternString(dis);
    }

    switch (tagByte) {
      case INT:
        return readIntFromIS(dis);
      case LONG:
        return readLongFromIS(dis);
      case DATE:
        return new Date(readLongFromIS(dis));
      case SOLRDOC:
        return readSolrDocument(dis);
      case SOLRDOCLST:
        return readSolrDocumentList(dis);
      case SOLRINPUTDOC:
        return readSolrInputDocument(dis);
      case MAP:
        return readMap(this, dis);
      case MAP_ENTRY:
        return readMapEntry(this, dis);
      case MAP_ENTRY_ITER:
        return readMapIter(dis);
    }

    return readLessCommonTypes(this, dis);
  }

  private static Long readLongFromIS(DataInputInputStream dis) throws IOException {
    return  (((long)dis.readUnsignedByte()) << 56)
        | (((long)dis.readUnsignedByte()) << 48)
        | (((long)dis.readUnsignedByte()) << 40)
        | (((long)dis.readUnsignedByte()) << 32)
        | (((long)dis.readUnsignedByte()) << 24)
        | (dis.readUnsignedByte() << 16)
        | (dis.readUnsignedByte() << 8)
        | (dis.readUnsignedByte());
  }

  private static Integer readIntFromIS(DataInputInputStream dis) throws IOException {
    return  ((dis.readUnsignedByte() << 24)
        |(dis.readUnsignedByte() << 16)
        |(dis.readUnsignedByte() << 8)
        | dis.readUnsignedByte());
  }

  private static Object readTagThenStringOrSolrDocument(JavaBinCodec javaBinCodec,
      DataInputInputStream dis) throws IOException {
    javaBinCodec.tagByte = dis.readByte();
    return readStringOrSolrDocument(javaBinCodec, dis);
  }

  private static Object readStringOrSolrDocument(JavaBinCodec javaBinCodec,
      DataInputInputStream dis) throws IOException {
    if (javaBinCodec.tagByte >>> 5 == STR >>> 5) {
      return readStr(javaBinCodec, dis, javaBinCodec.stringCache, javaBinCodec.readStringAsCharSeq);
    } else if (javaBinCodec.tagByte >>> 5 == EXTERN_STRING >>> 5) {
      return javaBinCodec.readExternString(dis);
    }

    switch (javaBinCodec.tagByte) {
      case SOLRDOC:
        return javaBinCodec.readSolrDocument(dis);
      case SOLRINPUTDOC:
        return javaBinCodec.readSolrInputDocument(dis);
      case FLOAT:
        return dis.readFloat();
    }

    throw new UnsupportedEncodingException("Unknown or unexpected type " + javaBinCodec.tagByte);
  }

  private static Object readLessCommonTypes(JavaBinCodec javaBinCodec, DataInputInputStream dis) throws IOException {
    switch (javaBinCodec.tagByte) {
      case BOOL_TRUE:
        return Boolean.TRUE;
      case BOOL_FALSE:
        return Boolean.FALSE;
      case NULL:
        return null;
      case DOUBLE:
        return dis.readDouble();
      case FLOAT:
        return dis.readFloat();
      case BYTE:
        return dis.readByte();
      case SHORT:
        return dis.readShort();
      case BYTEARR:
        return readByteArray(dis);
      case ITERATOR:
        return javaBinCodec.readIterator(dis);
      case END:
        return END_OBJ;
      case ENUM_FIELD_VALUE:
        return readEnumFieldValue(javaBinCodec, dis);
    }

    throw new UnsupportedEncodingException("Unknown type " + javaBinCodec.tagByte);
  }

  @SuppressWarnings("rawtypes")
  private boolean writeKnownType(Object val) throws IOException {
    while (true) {
      if (writePrimitive(val)) {
        return true;
      } else if (val instanceof NamedList) {
        writeNamedList(this, (NamedList<?>) val);
        return true;
      } else if (val instanceof SolrInputField) {
        val = ((SolrInputField) val).getValue();
        continue;
      } else if (val
          instanceof
          SolrDocumentList) { // SolrDocumentList is a List, so must come before List check
        writeSolrDocumentList((SolrDocumentList) val);
        return true;
      } else if (val instanceof SolrDocument) {
        // this needs special treatment to know which fields are to be written
        writeSolrDocument((SolrDocument) val);
        return true;
      } else if (val instanceof SolrInputDocument) {
        writeSolrInputDocument((SolrInputDocument) val);
        return true;
      } else if (val instanceof Iterator) {
        writeIterator(this, (Iterator) val);
        return true;
      } else if (val instanceof Map.Entry) {
        writeMapEntry((Map.Entry) val);
        return true;
      } else if (val instanceof MapWriter) {
        writeMap((MapWriter) val);
        return true;
      } else if (val instanceof Map) {
        writeMap(this, (Map) val);
        return true;
      } else if (writeLessCommonPrimitive(this, val)) return true;

      return writeLessCommonKnownType(this, val);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static boolean writeLessCommonKnownType(JavaBinCodec javaBinCodec, Object val) throws IOException {
    if (val instanceof Collection) {
      writeArray(javaBinCodec, (Collection) val);
      return true;
    } else if (val instanceof IteratorWriter) {
      javaBinCodec.writeIterator((IteratorWriter) val);
      return true;
    } else if (val instanceof Object[]) {
      writeArray(javaBinCodec, (Object[]) val);
      return true;
    } else if (val instanceof Path) {
      writeStr(javaBinCodec, ((Path) val).toAbsolutePath().toString());
      return true;
    } else if (val instanceof Iterable) {
      writeIterator(javaBinCodec, ((Iterable) val).iterator());
      return true;
    } else if (val instanceof EnumFieldValue) {
      javaBinCodec.writeEnumFieldValue((EnumFieldValue) val);
      return true;
    } else if (val instanceof MapSerializable) {
      // todo find a better way to reuse the map more efficiently
      writeMap(javaBinCodec, ((MapSerializable) val).toMap(new NamedList().asShallowMap()));
      return true;
    } else if (val instanceof AtomicInteger) {
      writeInt(javaBinCodec, ((AtomicInteger) val).get());
      return true;
    } else if (val instanceof AtomicLong) {
      writeLong(javaBinCodec, ((AtomicLong) val).get());
      return true;
    } else if (val instanceof AtomicBoolean) {
      writeBoolean(javaBinCodec, ((AtomicBoolean) val).get());
      return true;
    }
    return false;
  }

  private static class MapEntry implements Entry<Object, Object> {

    private final Object key;
    private final Object value;

    MapEntry(Object key, Object value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public Object getKey() {
      return key;
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "MapEntry[" + key + ':' + value + ']';
    }

    @DoNotCall
    @Override
    public final Object setValue(Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      int result = 31;
      result *= 31 + key.hashCode();
      result *= 31 + value.hashCode();
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof Map.Entry<?, ?>) {
        Entry<?, ?> entry = (Entry<?, ?>) obj;
        return (key.equals(entry.getKey()) && value.equals(entry.getValue()));
      }
      return false;
    }
  }

  public class BinEntryWriter implements MapWriter.EntryWriter {
    @Override
    public MapWriter.EntryWriter put(CharSequence k, Object v) throws IOException {
      writeExternString(k);
      writeVal(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, int v) throws IOException {
      writeExternString(k);
      writeInt(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, long v) throws IOException {
      writeExternString(k);
      writeLong(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, float v) throws IOException {
      writeExternString(k);
      writeFloat(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, double v) throws IOException {
      writeExternString(k);
      writeDouble(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, boolean v) throws IOException {
      writeExternString(k);
      writeBoolean(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, CharSequence v) throws IOException {
      writeExternString(k);
      writeStr(JavaBinCodec.this, v);
      return this;
    }

    private BiConsumer<CharSequence, Object> biConsumer;

    @Override
    public BiConsumer<CharSequence, Object> getBiConsumer() {
      if (biConsumer == null) biConsumer = MapWriter.EntryWriter.super.getBiConsumer();
      return biConsumer;
    }
  }

  public void writeMap(MapWriter val) throws IOException {
    writeTag(this, MAP_ENTRY_ITER);
    val.writeMap(ew);
    writeTag(this, END);
  }

  public static void writeTag(JavaBinCodec javaBinCodec, byte tag) throws IOException {
    writeByteToOS(javaBinCodec, tag);
  }

  public static void writeTag(JavaBinCodec javaBinCodec, byte tag, int size) throws IOException {
    if ((tag & 0xe0) != 0) {
      if (size < 0x1f) {
        writeByteToOS(javaBinCodec, tag | size);
      } else {
        writeByteToOS(javaBinCodec, tag | 0x1f);
        writeVInt(javaBinCodec, size - 0x1f);
      }
    } else {
      writeByteToOS(javaBinCodec, tag);
      writeVInt(javaBinCodec, size);
    }
  }

  public static void writeByteArray(JavaBinCodec javaBinCodec, byte[] arr, int offset, int len) throws IOException {
    writeTag(javaBinCodec, BYTEARR, len);
    writeToOS(javaBinCodec, arr, offset, len);
  }

  private static byte[] readByteArray(DataInputInputStream dis) throws IOException {
    byte[] arr = new byte[readVInt(dis)];
    dis.readFully(arr);
    return arr;
  }

  // children will return false in the predicate passed to EntryWriterWrapper e.g. NOOP
  private boolean isChildDoc = false;
  private MapWriter.EntryWriter cew;

  public void writeSolrDocument(SolrDocument doc) throws IOException {
    List<SolrDocument> children = doc.getChildDocuments();
    int fieldsCount = 0;
    if (writableDocFields == null || writableDocFields.wantsAllFields() || isChildDoc) {
      fieldsCount = doc.size();
    } else {
      for (Entry<String, Object> e : doc) {
        if (toWrite(e.getKey())) fieldsCount++;
      }
    }
    int sz = fieldsCount + (children == null ? 0 : children.size());
    writeTag(this, SOLRDOC);
    writeTag(this, ORDERED_MAP, sz);
    if (cew == null)
      cew = new ConditionalKeyMapWriter.EntryWriterWrapper(ew, k -> toWrite(k.toString()));
    doc.writeMap(cew);
    if (children != null) {
      try {
        isChildDoc = true;
        for (SolrDocument child : children) {
          writeSolrDocument(child);
        }
      } finally {
        isChildDoc = false;
      }
    }
  }

  private boolean toWrite(String key) {
    return writableDocFields == null || isChildDoc || writableDocFields.isWritable(key);
  }

  public SolrDocument readSolrDocument(DataInputInputStream dis) throws IOException {
    tagByte = dis.readByte();
    int size = readSize(this, dis);
    SolrDocument doc = new SolrDocument(new LinkedHashMap<>(size));
    for (int i = 0; i < size; i++) {
      String fieldName;
      Object obj =
          readTagThenStringOrSolrDocument(this, dis); // could be a field name, or a child document
      if (obj instanceof SolrDocument) {
        doc.addChildDocument((SolrDocument) obj);
        continue;
      } else {
        fieldName = (String) obj;
      }
      Object fieldVal = readVal(dis);
      doc.setField(fieldName, fieldVal);
    }
    return doc;
  }

  public SolrDocumentList readSolrDocumentList(DataInputInputStream dis) throws IOException {

    tagByte = dis.readByte();
    @SuppressWarnings("unchecked")
    List<Object> list = readArray(this, dis, readSize(this, dis));

    tagByte = dis.readByte();
    @SuppressWarnings("unchecked")
    List<Object> l = readArray(this, dis, readSize(this, dis));
    SolrDocumentList solrDocs = new SolrDocumentList(l.size());
    solrDocs.setNumFound((Long) list.get(0));
    solrDocs.setStart((Long) list.get(1));
    solrDocs.setMaxScore((Float) list.get(2));
    if (list.size() > 3) { // needed for back compatibility
      solrDocs.setNumFoundExact((Boolean) list.get(3));
    }

    l.forEach(doc -> solrDocs.add((SolrDocument) doc));

    return solrDocs;
  }

  public void writeSolrDocumentList(SolrDocumentList docs) throws IOException {
    writeTag(this, SOLRDOCLST);
    List<Object> l = new ArrayList<>(4);
    l.add(docs.getNumFound());
    l.add(docs.getStart());
    l.add(docs.getMaxScore());
    l.add(docs.getNumFoundExact());
    writeArray(this, l);
    writeArray(this, docs);
  }

  public SolrInputDocument readSolrInputDocument(DataInputInputStream dis) throws IOException {
    int sz = readVInt(dis);
    Float docBoost = (Float) readVal(dis); // avoid auto boxing
    if (!(docBoost.equals(FLOAT_1))) {
      if (WARNED_ABOUT_INDEX_TIME_BOOSTS.compareAndSet(false, true)) {
        log.warn(IGNORING_BOOST_AS_INDEX_TIME_BOOSTS_ARE_NOT_SUPPORTED_ANYMORE, docBoost);
      } else {
        if (log.isDebugEnabled()) {
          log.debug(IGNORING_BOOST_AS_INDEX_TIME_BOOSTS_ARE_NOT_SUPPORTED_ANYMORE, docBoost);
        }
      }
    }
    SolrInputDocument solrDoc = createSolrInputDocument(sz);
    for (int i = 0; i < sz; i++) {
      String fieldName;
      // we know we are expecting to read a String key, a child document (or a back compat boost)
      Object obj = readTagThenStringOrSolrDocument(this, dis);
      if (obj instanceof Float) {
        Float boost = (Float) obj;
        if (!(boost.equals(FLOAT_1))) {
          if (WARNED_ABOUT_INDEX_TIME_BOOSTS.compareAndSet(false, true)) {
            log.warn(IGNORING_BOOST_AS_INDEX_TIME_BOOSTS_ARE_NOT_SUPPORTED_ANYMORE, docBoost);
          } else {
            if (log.isDebugEnabled()) {
              log.debug(IGNORING_BOOST_AS_INDEX_TIME_BOOSTS_ARE_NOT_SUPPORTED_ANYMORE, docBoost);
            }
          }
        }

        // same as above, key, child doc, or back compat boost
        fieldName = (String) readTagThenStringOrSolrDocument(this, dis);
      } else if (obj instanceof SolrInputDocument) {
        solrDoc.addChildDocument((SolrInputDocument) obj);
        continue;
      } else {
        fieldName = (String) obj;
      }
      Object fieldVal = readVal(dis);
      solrDoc.setField(fieldName, fieldVal);
    }
    return solrDoc;
  }

  protected SolrInputDocument createSolrInputDocument(int sz) {
    return new SolrInputDocument(new LinkedHashMap<>(sz));
  }

  /**
   * Writes a {@link SolrInputDocument}.
   */
  public void writeSolrInputDocument(SolrInputDocument sdoc) throws IOException {
    List<SolrInputDocument> children = sdoc.getChildDocuments();
    int sz = sdoc.size() + (children == null ? 0 : children.size());
    writeTag(this, SOLRINPUTDOC, sz);
    writeFloat(1f); // placeholder document boost for back compat
    sdoc.writeMap(
        new ConditionalKeyMapWriter.EntryWriterWrapper(
            ew, it -> !CommonParams.CHILDDOC.equals(it.toString())));
    if (children != null) {
      for (SolrInputDocument child : children) {
        writeSolrInputDocument(child);
      }
    }
  }

  Map<Object, Object> readMapIter(DataInputInputStream dis) throws IOException {
    Map<Object, Object> m = newMap(-1);
    for (; ; ) {
      Object key = readVal(dis);
      if (key == END_OBJ) break;
      Object val = readVal(dis);
      m.put(key, val);
    }
    return m;
  }

  /**
   * Creates new Map implementations for unmarshalled Maps.
   *
   * @param size the expected size or -1 for unknown size
   */
  protected Map<Object, Object> newMap(int size) {
    return size < 0 ? new LinkedHashMap<>(8) : new LinkedHashMap<>(size);
  }

  private static Map<Object, Object> readMap(JavaBinCodec javaBinCodec, DataInputInputStream dis) throws IOException {
    int sz = readVInt(dis);
    return readMap(javaBinCodec, dis, sz);
  }

  static Map<Object, Object> readMap(JavaBinCodec javaBinCodec, DataInputInputStream dis, int sz) throws IOException {
    Map<Object, Object> m = javaBinCodec.newMap(sz);
    for (int i = 0; i < sz; i++) {
      Object key = javaBinCodec.readVal(dis);
      Object val = javaBinCodec.readVal(dis);
      m.put(key, val);
    }
    return m;
  }

  private final ItemWriter itemWriter = new ObjectItemWriter();

  public void writeIterator(IteratorWriter val) throws IOException {
    writeTag(this, ITERATOR);
    val.writeIter(itemWriter);
    writeTag(this, END);
  }

  public static void writeIterator(JavaBinCodec javaBinCodec, Iterator<?> iter) throws IOException {
    writeTag(javaBinCodec, ITERATOR);
    while (iter.hasNext()) {
      javaBinCodec.writeVal(iter.next());
    }
    writeTag(javaBinCodec, END);
  }

  /**
   * Unmarshalls an Iterator from the DataInputInputStream into a List.
   *
   * @param dataInputInputStream the stream to unmarshal from
   * @return a list containing the Objects from the unmarshalled Iterator
   * @throws IOException on IO failure
   */
  public List<Object> readIterator(DataInputInputStream dataInputInputStream) throws IOException {
    List<Object> l = new ArrayList<>(8);
    while (true) {
      Object o = readVal(dataInputInputStream);
      if (o == END_OBJ) break;
      l.add(o);
    }
    return l;
  }

  public static void writeArray(JavaBinCodec javaBinCodec, List<?> l) throws IOException {
    writeTag(javaBinCodec, ARR, l.size());
    for (Object o : l) {
      javaBinCodec.writeVal(o);
    }
  }

  public static void writeArray(JavaBinCodec javaBinCodec, Collection<?> coll) throws IOException {
    writeTag(javaBinCodec, ARR, coll.size());
    for (Object o : coll) {
      javaBinCodec.writeVal(o);
    }
  }

  public static void writeArray(JavaBinCodec javaBinCodec, Object[] arr) throws IOException {
    writeTag(javaBinCodec, ARR, arr.length);
    for (Object o : arr) {
      javaBinCodec.writeVal(o);
    }
  }

  @SuppressWarnings("unchecked")
  public static List<Object> readArray(JavaBinCodec javaBinCodec, DataInputInputStream dis) throws IOException {
    int sz = readSize(javaBinCodec, dis);
    return readArray(javaBinCodec, dis, sz);
  }

  @SuppressWarnings("rawtypes")
  protected static List readArray(JavaBinCodec javaBinCodec, DataInputInputStream dis, int sz) throws IOException {
    List<Object> l = new ArrayList<>(sz);
    for (int i = 0; i < sz; i++) {
      l.add(javaBinCodec.readVal(dis));
    }
    return l;
  }

  /**
   * write {@link EnumFieldValue} as tag+int value+string value
   *
   * @param enumFieldValue to write
   */
  private void writeEnumFieldValue(EnumFieldValue enumFieldValue) throws IOException {
    writeTag(this, ENUM_FIELD_VALUE);
    writeInt(this, enumFieldValue.toInt());
    writeStr(this, enumFieldValue.toString());
  }

  private void writeMapEntry(Map.Entry<?, ?> val) throws IOException {
    writeTag(this, MAP_ENTRY);
    writeVal(val.getKey());
    writeVal(val.getValue());
  }

  static EnumFieldValue readEnumFieldValue(JavaBinCodec javaBinCodec, DataInputInputStream dis) throws IOException {
    Integer intValue = (Integer) javaBinCodec.readVal(dis);
    String stringValue = (String) convertCharSeq(javaBinCodec.readVal(dis));
    return new EnumFieldValue(intValue, stringValue);
  }

  public static Map.Entry<Object, Object> readMapEntry(JavaBinCodec javaBinCodec,
      DataInputInputStream dis) throws IOException {
    Object key = javaBinCodec.readVal(dis);
    Object value = javaBinCodec.readVal(dis);
    return new MapEntry(key, value);
  }

  public static void writeStr(JavaBinCodec javaBinCodec, CharSequence s) throws IOException {

    // writes the string as tag+length, with length being the number of UTF-8 bytes

    if (s == null) {
      writeTag(javaBinCodec, NULL);
      return;
    }
    if (s instanceof Utf8CharSequence) {
      writeUTF8Str(javaBinCodec, (Utf8CharSequence) s);
      return;
    }
    int end = s.length();

    // Testing has not yet shown Solr decode to be a win here,
    // so currently 0
    if (end > MAX_SZ_BEFORE_STRING_UTF8_ENCODE_OVER_SOLR) {

      // however, when going too large, breaking up the conversion can allow
      // for much better scale and behavior regardless of it's performance
      if (end > MAX_SZ_BEFORE_SLOW_MULTI_STAGE_UTF8_ENCODE) {

        // the previous internal length calc method we used was very costly - often
        // approaching what the actual conversion costs - this is from Guava
        int sz = ByteUtils.calcUTF16toUTF8LengthGuava(s);

        int readSize = Math.min(sz, MAX_SZ_BEFORE_SLOW_MULTI_STAGE_UTF8_ENCODE);
        if (javaBinCodec.bytes == null || javaBinCodec.bytes.length < readSize) javaBinCodec.bytes = new byte[readSize];

        writeTag(javaBinCodec, STR, sz);
        flushBufferOS(javaBinCodec);
        ByteUtils.writeUTF16toUTF8(s, 0, end, javaBinCodec.out, javaBinCodec.bytes);
        return;
      }

      byte[] stringBytes = s.toString().getBytes(StandardCharsets.UTF_8);

      writeTag(javaBinCodec, STR, stringBytes.length);
      writeToOS(javaBinCodec, stringBytes);
    } else {
      int sz = ByteUtils.calcUTF16toUTF8LengthGuava(s);
      if (javaBinCodec.bytes == null || javaBinCodec.bytes.length < sz) javaBinCodec.bytes = new byte[sz];
      ByteUtils.UTF16toUTF8(s, 0, end, javaBinCodec.bytes, 0);

      writeTag(javaBinCodec, STR, sz);
      writeToOS(javaBinCodec, javaBinCodec.bytes, 0, sz);
    }
  }

  public static CharSequence readStr(JavaBinCodec javaBinCodec, DataInputInputStream dis) throws IOException {
    return readStr(javaBinCodec, dis, null, javaBinCodec.readStringAsCharSeq);
  }

  public static CharSequence readStr(JavaBinCodec javaBinCodec,
      DataInputInputStream dis, StringCache stringCache, boolean readStringAsCharSeq)
      throws IOException {
    if (readStringAsCharSeq) {
      return readUtf8(javaBinCodec, dis);
    }
    int sz = readSize(javaBinCodec, dis);
    return readStr(javaBinCodec, dis, stringCache, sz);
  }

  private static CharSequence readStr(JavaBinCodec javaBinCodec, DataInputInputStream dis,
      StringCache stringCache, int sz)
      throws IOException {
    if (javaBinCodec.bytes == null || javaBinCodec.bytes.length < sz) javaBinCodec.bytes = new byte[sz];
    dis.readFully(javaBinCodec.bytes, 0, sz);
    if (stringCache != null) {
      return stringCache.get(javaBinCodec.bytesRef.reset(javaBinCodec.bytes, 0, sz));
    } else {
      if (sz < MAX_SZ_BEFORE_STRING_UTF8_ENCODE_OVER_SOLR) {
        javaBinCodec.arr.reset();
        ByteUtils.UTF8toUTF16(javaBinCodec.bytes, 0, sz, javaBinCodec.arr);
        return javaBinCodec.arr.toString();
      }
      return new String(javaBinCodec.bytes, 0, sz, StandardCharsets.UTF_8);
      /*
       NOTE: Until Java 9, you had to use "UTF-8" vs passing an Encoder or you would not
       get Encoder caching. However, as part of 'compact strings', UTF-8 was
       special cased, and now passing an Encoder is okay. Additionally, this path
       also now hits intrinsics for SIMD. It has been juiced beyond a developers
       reach even though it still almost always requires returning a defensive array copy.
      */
    }
  }

  static CharSequence readUtf8(JavaBinCodec javaBinCodec, DataInputInputStream dis) throws IOException {
    int sz = readSize(javaBinCodec, dis);
    return readUtf8(javaBinCodec, dis, sz);
  }

  private static CharSequence readUtf8(JavaBinCodec javaBinCodec, DataInputInputStream dis, int sz) throws IOException {
    ByteArrayUtf8CharSequence result = new ByteArrayUtf8CharSequence(null, 0, 0);
    if (dis.readDirectUtf8(result, sz)) {
      result.stringProvider = javaBinCodec.getStringProvider();
      return result;
    }

    if (sz > MAX_STRING_SZ_TO_TRY_KEEPING_AS_UTF8_WO_CONVERT_BYTES) return readStr(
        javaBinCodec, dis, null, sz);

    if (javaBinCodec.bytesBlock == null) javaBinCodec.bytesBlock = new BytesBlock(Math.max(sz, 1024 << 2));

    BytesBlock block = javaBinCodec.bytesBlock.expand(sz);
    dis.readFully(block.getBuf(), block.getStartPos(), sz);
    result.reset(block.getBuf(), block.getStartPos(), sz, null);
    result.stringProvider = javaBinCodec.getStringProvider();
    return result;
  }

  private Function<ByteArrayUtf8CharSequence, String> getStringProvider() {
    if (stringProvider == null) {
      stringProvider = new ByteArrayUtf8CharSequenceStringFunction();
    }
    return stringProvider;
  }

  public static void writeInt(JavaBinCodec javaBinCodec, int val) throws IOException {
    if (val > 0) {
      int b = SINT | (val & 0x0f);
      if (val >= 0x0f) {
        b |= 0x10;
        writeByteToOS(javaBinCodec, b);
        writeVInt(javaBinCodec, val >>> 4);
      } else {
        writeByteToOS(javaBinCodec, b);
      }
    } else {
      writeByteToOS(javaBinCodec, INT);
      writeIntToOS(javaBinCodec, val);
    }
  }

  public static int readSmallInt(JavaBinCodec javaBinCodec, DataInputInputStream dis) throws IOException {
    int v = javaBinCodec.tagByte & 0x0F;
    if ((javaBinCodec.tagByte & 0x10) != 0) v = (readVInt(dis) << 4) | v;
    return v;
  }

  public static void writeLong(JavaBinCodec javaBinCodec, long val) throws IOException {
    if ((val & 0xff00000000000000L) == 0) {
      int b = SLONG | ((int) val & 0x0f);
      if (val >= 0x0f) {
        b |= 0x10;
        writeByteToOS(javaBinCodec, b);
        writeVLong(javaBinCodec, val >>> 4);
      } else {
        writeByteToOS(javaBinCodec, b);
      }
    } else {
      writeByteToOS(javaBinCodec, LONG);
      writeLongToOS(javaBinCodec, val);
    }
  }

  static long readSmallLong(JavaBinCodec javaBinCodec, DataInputInputStream dis) throws IOException {
    long v = javaBinCodec.tagByte & 0x0F;
    if ((javaBinCodec.tagByte & 0x10) != 0) v = (readVLong(dis) << 4) | v;
    return v;
  }

  public void writeFloat(float val) throws IOException {
    writeByteToOS(this, FLOAT);
    writeFloatToOS(this, val);
  }

  public boolean writePrimitive(Object val) throws IOException {
    if (val instanceof CharSequence) {
      writeStr(this, (CharSequence) val);
      return true;
    } else if (val instanceof Integer) {
      writeInt(this, (Integer) val);
      return true;
    } else if (val instanceof Long) {
      writeLong(this, (Long) val);
      return true;
    } else if (val instanceof Float) {
      writeFloat((Float) val);
      return true;
    } else if (val instanceof Date) {
      writeByteToOS(this, DATE);
      writeLongToOS(this, ((Date) val).getTime());
      return true;
    } else if (val instanceof Boolean) {
      writeBoolean(this, (Boolean) val);
      return true;
    }
    return false;
  }

  private static boolean writeLessCommonPrimitive(JavaBinCodec javaBinCodec, Object val) throws IOException {
    if (val == null) {
      writeByteToOS(javaBinCodec, NULL);
      return true;
    } else if (val instanceof Double) {
      writeDouble(javaBinCodec, (Double) val);
      return true;
    } else if (val instanceof Short) {
      writeByteToOS(javaBinCodec, SHORT);
      writeShortToOS(javaBinCodec, ((Short) val).intValue());
      return true;
    } else if (val instanceof Byte) {
      writeByteToOS(javaBinCodec, BYTE);
      writeByteToOS(javaBinCodec, ((Byte) val).intValue());
      return true;
    } else if (val instanceof byte[]) {
      writeByteArray(javaBinCodec, (byte[]) val, 0, ((byte[]) val).length);
      return true;
    } else if (val instanceof ByteBuffer) {
      ByteBuffer buffer = (ByteBuffer) val;
      writeByteArray(javaBinCodec, buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.limit() - buffer.position());
      return true;
    } else if (val == END_OBJ) {
      writeTag(javaBinCodec, END);
      return true;
    }
    return false;
  }

  protected static void writeBoolean(JavaBinCodec javaBinCodec, boolean val) throws IOException {
    if (val) writeByteToOS(javaBinCodec, BOOL_TRUE);
    else writeByteToOS(javaBinCodec, BOOL_FALSE);
  }

  protected static void writeDouble(JavaBinCodec javaBinCodec, double val) throws IOException {
    writeByteToOS(javaBinCodec, DOUBLE);
    writeDoubleToOS(javaBinCodec, val);
  }

  public static void writeMap(JavaBinCodec javaBinCodec, Map<?, ?> val) throws IOException {
    writeTag(javaBinCodec, MAP, val.size());
    if (val instanceof MapWriter) {
      ((MapWriter) val).writeMap(javaBinCodec.ew);
      return;
    }
    for (Map.Entry<?, ?> entry : val.entrySet()) {
      Object key = entry.getKey();
      if (key instanceof String) {
        javaBinCodec.writeExternString((CharSequence) key);
      } else {
        javaBinCodec.writeVal(key);
      }
      javaBinCodec.writeVal(entry.getValue());
    }
  }

  public static int readSize(JavaBinCodec javaBinCodec, DataInputInputStream in) throws IOException {
    int sz = javaBinCodec.tagByte & 0x1f;
    if (sz == 0x1f) sz += readVInt(in);
    return sz;
  }

  /**
   * Special method for variable length int (copied from lucene). Usually used for writing the
   * length of a collection/array/map In most of the cases the length can be represented in one byte
   * (length &lt; 127) so it saves 3 bytes/object
   *
   * @throws IOException If there is a low-level I/O error.
   */
  private static void writeVInt(JavaBinCodec javaBinCodec, int i) throws IOException {
    while ((i & ~0x7F) != 0) {
      writeByteToOS(javaBinCodec, (byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByteToOS(javaBinCodec, (byte) i);
  }

  /**
   * The counterpart for {@link JavaBinCodec#writeVInt(JavaBinCodec, int)}
   *
   * @throws IOException If there is a low-level I/O error.
   */
  protected static int readVInt(DataInputInputStream in) throws IOException {
    byte b = in.readByte();
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = in.readByte();
      i |= (b & 0x7F) << shift;
    }
    return i;
  }

  private static void writeVLong(JavaBinCodec javaBinCodec, long i) throws IOException {
    while ((i & ~0x7F) != 0) {
      writeByteToOS(javaBinCodec, (byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByteToOS(javaBinCodec, (byte) i);
  }

  private static long readVLong(DataInputInputStream in) throws IOException {
    byte b = in.readByte();
    long i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = in.readByte();
      i |= (long) (b & 0x7F) << shift;
    }
    return i;
  }

  public void writeExternString(CharSequence str) throws IOException {
    if (str == null) {
      writeTag(this, NULL);
      return;
    }

    Integer idx = stringsMap == null ? null : stringsMap.get(str);
    if (idx == null) idx = ZERO;
    writeTag(this, EXTERN_STRING, idx);
    if (idx.equals(ZERO)) {
      writeStr(this, str);
      if (stringsMap == null) stringsMap = new HashMap<>(16, 0.7f);
      stringsMap.put(str.toString(), ++stringsCount);
    }
  }

  public CharSequence readExternString(DataInputInputStream fis) throws IOException {
    int idx = readSize(this, fis);
    if (idx != 0) { // idx != 0 is the index of the extern string
      return stringsList.get(idx - 1);
    } else { // idx == 0 means it has a string value
      tagByte = fis.readByte();
      CharSequence str = readStr(this, fis, stringCache, false);
      if (str != null) str = str.toString();
      if (stringsList == null) stringsList = new ArrayList<>(16);
      stringsList.add(str);
      return str;
    }
  }

  public static void writeUTF8Str(JavaBinCodec javaBinCodec, Utf8CharSequence utf8) throws IOException {
    writeTag(javaBinCodec, STR, utf8.size());
    writeUtf8CharSeqToOS(javaBinCodec, utf8);
  }

  /**
   * Allows extension of {@link JavaBinCodec} to support serialization of arbitrary data types.
   *
   * <p>Implementors of this interface write a method to serialize a given object using an existing
   * {@link JavaBinCodec}
   */
  public interface ObjectResolver {
    /**
     * Examine and attempt to serialize the given object, using a {@link JavaBinCodec} to write it
     * to a stream.
     *
     * @param o the object that the caller wants serialized.
     * @param codec used to actually serialize {@code o}.
     * @return the object {@code o} itself if it could not be serialized, or {@code null} if the
     *     whole object was successfully serialized.
     * @see JavaBinCodec
     */
    Object resolve(Object o, JavaBinCodec codec) throws IOException;
  }

  public interface WritableDocFields {
    boolean isWritable(String name);

    boolean wantsAllFields();
  }

  public static class StringCache {
    private final Cache<StringBytes, String> cache;

    public StringCache(Cache<StringBytes, String> cache) {
      this.cache = cache;
    }

    public String get(StringBytes b) {
      String result = cache.get(b);
      if (result == null) {
        // make a copy because the buffer received may be changed later by the caller
        StringBytes copy =
            new StringBytes(
                Arrays.copyOfRange(b.bytes, b.offset, b.offset + b.length), 0, b.length);


        if (b.length < MAX_SZ_BEFORE_STRING_UTF8_ENCODE_OVER_SOLR) {
          CharArr arr = new CharArr();
          ByteUtils.UTF8toUTF16(b.bytes, b.offset, b.length, arr);
          result = arr.toString();
        } else {
          result = new String(b.bytes, b.offset, b.length, StandardCharsets.UTF_8);
        }

        cache.put(copy, result);
      }
      return result;
    }
  }

  @Override
  public void close() throws IOException {
    if (out != null) {
      flushBufferOS(this);
    }
  }

  /*
   We do a low level optimization here. Low level code that does IO, especially that writes
   bytes, is very impacted by any additional overhead. In the best case, things get inlined.
   However, the best case only goes so far. Inlining has cut off caps, limitations, reversals when assumptions get invalidated, etc.

   Via measurement and inspection, we find this and related existing classes have not been entirely compiler friendly.

   To further help the situation, we pull the OutputStream wrapper layer class into JavaBinCodec itself - allowing no additional layers
   of inheritance, nor the opportunity for the class to be co-opted for other tasks or uses, thus ensuring efficient code and monomorphic call sites.

   Looking at how the output methods should be dispatched to, the rule of thumb is that interfaces are slowest to dispatch on, abstract classes are faster,
   isolated classes obviously a decent case, but at the top is the simple jump of a static method call. JMH to confirm.
   The single byte methods show the most gratitude.
  */

  // passing null for the buffer writes straight through to the stream - if writing to something
  // like a ByteArrayOutputStream, intermediate buffering should not be used
  private void initOutStream(OutputStream sink, byte[] tempBuffer) {
    out = sink;
    buf = tempBuffer;
    pos = 0;
    if (sink instanceof FastOutputStream) {
      isFastOutputStream = true;
      if (tempBuffer != null) {
        throw new IllegalArgumentException(
            "FastInputStream cannot pass a buffer to JavaBinInputStream as it will already buffer - pass null to write to the stream directly");
      }
    } else {
      isFastOutputStream = false;
    }
  }

  private void initOutStream(OutputStream w) {
    // match jetty output buffer
    initOutStream(w, new byte[8192]);
  }

  private static void writeToOS(JavaBinCodec javaBinCodec, byte[] b) throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write(b, 0, b.length);
      return;
    }
    writeToOS(javaBinCodec, b, 0, b.length);
  }

  private static void writeToOS(JavaBinCodec javaBinCodec, byte b) throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write(b);
      return;
    }

    if (javaBinCodec.pos >= javaBinCodec.buf.length) {
      flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.buf.length);
      javaBinCodec.pos = 0;
    }
    javaBinCodec.buf[javaBinCodec.pos++] = b;
  }

  private static void writeToOS(JavaBinCodec javaBinCodec, byte[] arr, int off, int len)
      throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write(arr, off, len);
      return;
    }

    for (; ; ) {
      int space = javaBinCodec.buf.length - javaBinCodec.pos;

      if (len <= space) {
        System.arraycopy(arr, off, javaBinCodec.buf, javaBinCodec.pos, len);
        javaBinCodec.pos += len;
        return;
      } else if (len > javaBinCodec.buf.length) {
        if (javaBinCodec.pos > 0) {
          flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.pos); // flush
          javaBinCodec.pos = 0;
        }
        // don't buffer, just write to sink
        flushOS(javaBinCodec, arr, off, len);
        return;
      }

      // buffer is too big to fit in the free space, but
      // not big enough to warrant writing on its own.
      // write whatever we can fit, then flush and iterate.

      System.arraycopy(arr, off, javaBinCodec.buf, javaBinCodec.pos, space);
      flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.buf.length);
      javaBinCodec.pos = 0;
      off += space;
      len -= space;
    }
  }

  protected static void writeByteToOS(JavaBinCodec javaBinCodec, int b) throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write((byte) b);
      return;
    }

    if (javaBinCodec.pos >= javaBinCodec.buf.length) {
      flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.buf.length);
      javaBinCodec.pos = 0;
    }
    javaBinCodec.buf[javaBinCodec.pos++] = (byte) b;
  }

  private static void writeShortToOS(JavaBinCodec javaBinCodec, int v) throws IOException {
    writeToOS(javaBinCodec, (byte) (v >>> 8));
    writeToOS(javaBinCodec, (byte) v);
  }

  private static void writeIntToOS(JavaBinCodec javaBinCodec, int v) throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write((byte) (v >>> 24));
      javaBinCodec.out.write((byte) (v >>> 16));
      javaBinCodec.out.write((byte) (v >>> 8));
      javaBinCodec.out.write((byte) (v));
      javaBinCodec.pos += 4;
      return;
    }

    if (4 > javaBinCodec.buf.length - javaBinCodec.pos && javaBinCodec.pos > 0) {
        flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.pos);
        javaBinCodec.pos = 0;
      }
    javaBinCodec.buf[javaBinCodec.pos] = (byte) (v >>> 24);
    javaBinCodec.buf[javaBinCodec.pos + 1] = (byte) (v >>> 16);
    javaBinCodec.buf[javaBinCodec.pos + 2] = (byte) (v >>> 8);
    javaBinCodec.buf[javaBinCodec.pos + 3] = (byte) (v);
    javaBinCodec.pos += 4;
  }

  protected static void writeLongToOS(JavaBinCodec javaBinCodec, long v) throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write((byte) (v >>> 56));
      javaBinCodec.out.write((byte) (v >>> 48));
      javaBinCodec.out.write((byte) (v >>> 40));
      javaBinCodec.out.write((byte) (v >>> 32));
      javaBinCodec.out.write((byte) (v >>> 24));
      javaBinCodec.out.write((byte) (v >>> 16));
      javaBinCodec.out.write((byte) (v >>> 8));
      javaBinCodec.out.write((byte) (v));
      javaBinCodec.pos += 8;
      return;
    }

    if (8 > (javaBinCodec.buf.length - javaBinCodec.pos) && javaBinCodec.pos > 0) {
      flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.pos);
      javaBinCodec.pos = 0;
    }
    javaBinCodec.buf[javaBinCodec.pos] = (byte) (v >>> 56);
    javaBinCodec.buf[javaBinCodec.pos + 1] = (byte) (v >>> 48);
    javaBinCodec.buf[javaBinCodec.pos + 2] = (byte) (v >>> 40);
    javaBinCodec.buf[javaBinCodec.pos + 3] = (byte) (v >>> 32);
    javaBinCodec.buf[javaBinCodec.pos + 4] = (byte) (v >>> 24);
    javaBinCodec.buf[javaBinCodec.pos + 5] = (byte) (v >>> 16);
    javaBinCodec.buf[javaBinCodec.pos + 6] = (byte) (v >>> 8);
    javaBinCodec.buf[javaBinCodec.pos + 7] = (byte) (v);
    javaBinCodec.pos += 8;
  }

  private static void writeFloatToOS(JavaBinCodec javaBinCodec, float v) throws IOException {
    writeIntToOS(javaBinCodec, Float.floatToRawIntBits(v));
  }

  private static void writeDoubleToOS(JavaBinCodec javaBinCodec, double v) throws IOException {
    writeLongToOS(javaBinCodec, Double.doubleToRawLongBits(v));
  }

  /** Only flushes the buffer of the FastOutputStream, not that of the underlying stream. */
  private static void flushBufferOS(JavaBinCodec javaBinCodec) throws IOException {
    if (javaBinCodec.buf == null) {
      if (javaBinCodec.isFastOutputStream) {
        ((FastOutputStream) javaBinCodec.out).flushBuffer();
      }
      return;
    }

    if (javaBinCodec.pos > 0) {
      flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.pos);
      javaBinCodec.pos = 0;
    }
  }

  /** All writes to the sink will go through this method */
  private static void flushOS(JavaBinCodec javaBinCodec, byte[] buf, int offset, int len) throws IOException {
    javaBinCodec.out.write(buf, offset, len);
  }

  /** Copies a {@link Utf8CharSequence} without making extra copies */
  private static void writeUtf8CharSeqToOS(JavaBinCodec javaBinCodec, Utf8CharSequence utf8)
      throws IOException {
    if (javaBinCodec.buf == null) {
      utf8.write(javaBinCodec.out);
      return;
    }

    int start = 0;
    int totalWritten = 0;
    while (true) {
      int size = utf8.size();
      if (totalWritten >= size) break;
      if (javaBinCodec.pos >= javaBinCodec.buf.length) flushBufferOS(javaBinCodec);
      int sz = utf8.write(start, javaBinCodec.buf, javaBinCodec.pos);
      javaBinCodec.pos += sz;
      totalWritten += sz;
      start += sz;
    }
  }

  private static class ByteArrayUtf8CharSequenceStringFunction
      implements Function<ByteArrayUtf8CharSequence, String> {

    @Override
    public String apply(ByteArrayUtf8CharSequence butf8cs) {
      return new String(butf8cs.buf, butf8cs.offset, butf8cs.length, StandardCharsets.UTF_8);
    }
  }

  public static class InvalidEncodingException extends IOException {
    public InvalidEncodingException(String s) {
      super(s);
    }
  }

  private class ObjectItemWriter implements ItemWriter {

    @Override
    public ItemWriter add(Object o) throws IOException {
      writeVal(o);
      return this;
    }

    @Override
    public ItemWriter add(int v) throws IOException {
      writeInt(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public ItemWriter add(long v) throws IOException {
      writeLong(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public ItemWriter add(float v) throws IOException {
      writeFloat(v);
      return this;
    }

    @Override
    public ItemWriter add(double v) throws IOException {
      writeDouble(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public ItemWriter add(boolean v) throws IOException {
      writeBoolean(JavaBinCodec.this, v);
      return this;
    }
  }
}
