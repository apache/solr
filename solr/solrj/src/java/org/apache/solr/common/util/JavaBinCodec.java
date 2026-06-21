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


import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.ObjectListIterator;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.io.DirectBufferInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.solr.common.*;
import org.apache.solr.common.IteratorWriter.ItemWriter;
import org.apache.solr.common.params.CommonParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Defines a space-efficient serialization/deserialization format for transferring data.
 * <p>
 * JavaBinCodec has built in support many commonly used types.  This includes primitive types (boolean, byte,
 * short, double, int, long, float), common Java containers/utilities (Date, Map, Collection, Iterator, String,
 * Object[], byte[]), and frequently used Solr types ({@link NamedList}, {@link SolrDocument},
 * {@link SolrDocumentList}). Each of the above types has a pair of associated methods which read and write
 * that type to a stream.
 * <p>
 * Classes that aren't supported natively can still be serialized/deserialized by providing
 * an {@link JavaBinCodec.ObjectResolver} object that knows how to work with the unsupported class.
 * This allows {@link JavaBinCodec} to be used to marshall/unmarshall arbitrary content.
 * <p>
 * NOTE -- {@link JavaBinCodec} instances cannot be reused for more than one marshall or unmarshall operation.
 */
@SuppressWarnings("unchecked")
public class JavaBinCodec implements PushWriter {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    public static final byte
            NULL = 0,
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

    /**
     * this is a special tag signals an end. No value is associated with it
     */
    END = 15,

    SOLRINPUTDOC = 16,
            MAP_ENTRY_ITER = 17,
            ENUM_FIELD_VALUE = 18,
            MAP_ENTRY = 19,
        STR = 20,
        EXTERN_STRING =21,
            VINT = 22,

    UUID = 23,// This is reserved to be used only in LogCodec

    // types that combine tag + length (or other info) in a single byte
    TAG_AND_LEN = (byte) (1 << 5),
            SINT = (byte) (2 << 5),
            SLONG = (byte) (3 << 5),
            ARR = (byte) (4 << 5), //
            ORDERED_MAP = (byte) (5 << 5), // SimpleOrderedMap (a NamedList subclass, and more common)
            NAMED_LST = (byte) (6 << 5), // NamedList
        STD_EXTERN_STRING= (byte) (7 << 5);

    public static final byte VERSION = 3;
    private final ObjectResolver resolver;
    protected JavaBinOutputStream daos;

    private WritableDocFields writableDocFields;
    private boolean alreadyMarshalled;
    private boolean alreadyUnmarshalled;
    protected final AtomicBoolean readStringAsCharSeq = new AtomicBoolean();
    protected boolean stdStrings = true;
    byte[] bytes;
    private UnsafeBuffer buffer;
    // shared buffer for ByteArrayUtf8CharSequence reads; strings that fit share the same byte[]
    private static final int CHARSEQ_BUFSIZE = 4096;
    private byte[] charseqBuf;
    private int charseqBufPos;

    public JavaBinCodec() {
        resolver = null;
        writableDocFields = null;
    }

    public JavaBinCodec(FastOutputStream os) throws IOException {
        this.resolver = null;
        initWrite(os);
    }

    private void initWrite(FastOutputStream os) throws IOException {

        this.daos = new JavaBinOutputStream() {
            @Override
            public void writeInt(int b) throws IOException {
                os.writeInt(b);
            }

            @Override
            public void writeLong(long b) throws IOException {
                os.writeLong(b);
            }

            @Override
            public void writeFloat(float val) throws IOException {
                os.writeFloat(val);
            }

            @Override
            public void writeShort(short val) throws IOException {
                os.writeShort(val);
            }

            @Override
            public void writeByte(int b) throws IOException {
                os.write((byte) b);
            }

            @Override
            public void write(int b) throws IOException {
                os.write((byte) b);
            }


        };
        log.trace("write version={}", VERSION);
        daos.writeByte(VERSION);
    }

    public JavaBinCodec setReadStringAsCharSeq(boolean flag) {
        readStringAsCharSeq.set(flag);
        return this;
    }

    /**
     * Use this to use this as a PushWriter. ensure that close() is called explicitly after use
     *
     * @param os The output stream
     */
//  public JavaBinCodec(FastOutputStream os, ObjectResolver resolver) throws IOException {
//    this.resolver = resolver;
//    initWrite(os);
//  }
    public JavaBinCodec(ExpandableDirectBufferOutputStream os, ObjectResolver resolver) throws IOException {
        this.resolver = resolver;
        initWrite(os);
    }

    public JavaBinCodec(ObjectResolver resolver) {
        this.resolver = resolver;
    }

    public JavaBinCodec setWritableDocFields(WritableDocFields writableDocFields) {
        this.writableDocFields = writableDocFields;
        return this;

    }

//  public JavaBinCodec(ObjectResolver resolver) {
//    this.resolver = resolver;
//  }

    public ObjectResolver getResolver() {
        return resolver;
    }

    public void marshal(Object nl, BufferedChannel os) throws IOException {
        try {
            initWrite(os);
            writeVal(nl);
        } finally {
            alreadyMarshalled = true;
            daos.flush();
            readStringAsCharSeq.set(false);
        }
    }

    public void marshal(Object nl, SolrDirectBufferOutputStream os) throws IOException {
        try {
            initWrite(os);
            writeVal(nl);
        } finally {
            alreadyMarshalled = true;
            daos.flush();
            readStringAsCharSeq.set(false);
        }
    }

    public void marshal(Object nl, ExpandableDirectBufferOutputStream os) throws IOException {
        try {
            initWrite(os);
            writeVal(nl);
        } finally {
            alreadyMarshalled = true;
            daos.flush();
            readStringAsCharSeq.set(false);
        }
    }

    public void marshal(Object nl, FastOutputStream os) throws IOException {
        try {
            initWrite(os);
            writeVal(nl);
        } finally {
            alreadyMarshalled = true;
            daos.flush();
            readStringAsCharSeq.set(false);
        }
    }

    protected void initWrite(ExpandableDirectBufferOutputStream os) throws IOException {
        assert !alreadyMarshalled;
        this.daos = new JavaBinOutputStream() {
            @Override
            public void writeInt(int b) {
                os.putInt(b);
            }

            @Override
            public void writeLong(long b) {
                os.putLong(b);
            }


            @Override
            public void writeByte(int b) {
                os.write((byte) b);
            }

            @Override
            public void writeFloat(float val) {
                os.putFloat(val);
            }

            @Override
            public void writeShort(short val) {
                os.putShort(val);
            }

            @Override
            public void write(int b) {
                os.write((byte) b);
            }
        };
        log.trace("write version={}", VERSION);
        daos.writeByte(VERSION);
    }

    protected void initWrite(SolrDirectBufferOutputStream os) throws IOException {
        assert !alreadyMarshalled;
        this.daos = new JavaBinOutputStream() {

            @Override
            public void writeInt(int b) {
                os.putInt(b);
            }

            @Override
            public void writeLong(long b) {
                os.putLong(b);
            }

            @Override
            public void writeFloat(float val) {
                os.putFloat(val);
            }

            @Override
            public void writeShort(short val) {
                os.putShort(val);
            }

            @Override
            public void writeByte(int b) {
                os.write((byte) b);
            }

            @Override
            public void write(int b) {
                os.write((byte) b);
            }
        };
        log.trace("write version={}", VERSION);
        daos.writeByte(VERSION);
    }

    protected void initWrite(BufferedChannel os) throws IOException {
        assert !alreadyMarshalled;
        this.daos = new JavaBinOutputStream() {

            @Override
            public void writeInt(int b) {
                os.putInt(b);
            }

            @Override
            public void writeLong(long b) {
                os.putLong(b);
            }

            @Override
            public void writeFloat(float val) {
                os.putFloat(val);
            }

            @Override
            public void writeShort(short val) {
                os.putShort(val);
            }

            @Override
            public void write(int b) throws IOException {
                os.write(b);
            }
        };
        log.trace("write version={}", VERSION);
        daos.writeByte(VERSION);
    }

    byte version;

    public Object unmarshal(byte[] buf) throws IOException {
        JavaBinInputStream dis = initRead(buf);
        return readVal(dis);
    }

    public Object unmarshal(JavaBinInputStream is) throws IOException {
        JavaBinInputStream dis = initRead(is);
        return readVal(dis);
    }

    protected JavaBinInputStream initRead(InputStream is) throws IOException {
        assert !alreadyUnmarshalled;
        if (!(is instanceof JavaBinInputStream)) {
            is = new FastInputStream(is);
            return _init((JavaBinInputStream) is);
        }
        return _init((JavaBinInputStream) is);
    }

    protected JavaBinInputStream initRead(byte[] buf) throws IOException {
        assert !alreadyUnmarshalled;

        buffer = new UnsafeBuffer(buf);
        JavaBinInputStream dis = new FastInputStream(new DirectBufferInputStream(buffer), 0);
        return _init(dis);
    }

    protected JavaBinInputStream _init(JavaBinInputStream dis) throws IOException {
        version = dis.readByte();
        if (version != VERSION) {
            throw new JavaBinFormatException("Invalid version (expected " + VERSION +
                    ", but " + version + ") or the data in not in 'javabin' format asString=" + IOUtils.toString(dis, UTF_8));
        }

        alreadyUnmarshalled = true;
        return dis;
    }

    public void marshal(Object payload, OutputStream os) throws IOException {
        if (os instanceof BufferedChannel) {
            marshal(payload, (BufferedChannel) os);
        } else if (os instanceof ExpandableDirectBufferOutputStream) {
            marshal(payload, (ExpandableDirectBufferOutputStream) os);
        } else if (os instanceof SolrDirectBufferOutputStream) {
            marshal(payload, (SolrDirectBufferOutputStream) os);
        } else {
            marshal(payload, new FastOutputStream(os));
            //throw new IllegalArgumentException("unsupported output stream " + os.getClass().getName());
        }
    }

    public void init(ExpandableDirectBufferOutputStream os) throws IOException {
        this.daos = new JavaBinOutputStream() {
            @Override
            public void writeInt(int b) {
                os.putInt(b);
            }

            @Override
            public void writeLong(long b) {
                os.putLong(b);
            }

            @Override
            public void writeFloat(float val) {
                os.putFloat(val);
            }

            @Override
            public void writeShort(short val) {
                os.putShort(val);
            }

            @Override
            public void writeByte(int b) {
                os.write((byte) b);
            }

            @Override
            public void write(int b) {
                os.write((byte) b);
            }
        };
        // NOTE: unlike initWrite(...)/marshal(...), this init(...) is used by the transaction log
        // to write raw records that are read back with a bare readVal(...) (no version framing).
        // Writing a VERSION byte here would prepend a stray 0x03 that readVal decodes as a Byte.
    }

    public void init(DirectMemBufferOutputStream os) throws IOException {
        this.daos = new JavaBinOutputStream() {
            @Override
            public void writeInt(int b) {
                os.putInt(b);
            }

            @Override
            public void writeLong(long b) {
                os.putLong(b);
            }

            @Override
            public void writeFloat(float val) {
                os.putFloat(val);
            }

            @Override
            public void writeShort(short val) {
                os.putShort(val);
            }

            @Override
            public void writeByte(int b) {
                os.write((byte) b);
            }

            @Override
            public void write(int b) {
                os.write(b);
            }
        };
        // See init(ExpandableDirectBufferOutputStream): tlog records are read with a bare
        // readVal(...), so do NOT prepend a VERSION byte here (only initWrite/marshal does).
    }

    public Object unmarshal(DirectBufferInputStream is) throws IOException {
        JavaBinInputStream dis = initRead(is);
        return readVal(dis);
    }

    public Object unmarshal(DirectMemBufferedInputStream is) throws IOException {
        JavaBinInputStream dis = initRead(is);
        return readVal(dis);
    }


    public Object unmarshal(ByteArrayInputStream byteArrayInputStream) throws IOException {
        return unmarshal(FastInputStream.wrap(byteArrayInputStream));
    }


    public static class JavaBinFormatException extends RuntimeException {

        public JavaBinFormatException(String msg) {
            super(msg);
        }
    }

    public SimpleOrderedMap<Object> readOrderedMap(JavaBinInputStream dis, int sz) throws IOException {
        log.trace("read ordered map");

        SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>(sz);
        for (int i = 0; i < sz; i++) {

            // Preserve a genuinely-null name (e.g. the null group key of the "no value"
            // group in distributed grouping). String.valueOf(null) would produce the
            // 4-char string "null", which then gets reparsed as a field value downstream
            // (e.g. NumericFieldType "Invalid Number: null"). toString() on a non-null
            // CharSequence keeps Utf8CharSequence names working.
            Object nameObj = readVal(dis);
            String name = nameObj == null ? null : nameObj.toString();
            Object val = readVal(dis);
            nl.add(name, val);
        }
        return nl;
    }

    public NamedList<Object> readNamedList(JavaBinInputStream dis, int sz) throws IOException {
        log.trace("read namedlist");

        NamedList<Object> nl = new NamedList<>(sz);
        for (int i = 0; i < sz; i++) {

            // Preserve a genuinely-null name; String.valueOf(null) would produce the
            // string "null" and corrupt null keys (see readOrderedMap for details).
            Object nameObj = readVal(dis);
            String name = nameObj == null ? null : nameObj.toString();

            Object val = readVal(dis);
            nl.add( name, val);
        }
        return nl;
    }

    public void writeNamedList(NamedList<?> nl) throws IOException {
        log.trace("write namedlist {}", nl);
        int size = nl.size();
        writeTag(nl instanceof SimpleOrderedMap ? ORDERED_MAP : NAMED_LST, size);
        for (int i = 0; i < size; i++) {
            String name = nl.getName(i);
            writeExternString(name);
            Object val = nl.getVal(i);
            writeVal(val);
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

        writeVal(val.getClass().getName() + ':' + val);
        //throw new UnsupportedOperationException("Cannot serialize " + val.getClass().getName());
    }

    protected static final Object END_OBJ = new Object();

    protected byte tagByte;

    public Object readVal(JavaBinInputStream dis) throws IOException {
        tagByte = read(dis);
        return readObject(dis);
    }

    protected Object readObject(JavaBinInputStream dis) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("read object tag={} name={}", tagByte, name(tagByte));
        }

        // if ((tagByte & 0xe0) == 0) {
        // if top 3 bits are clear, this is a normal tag

        //  }

        //  log.trace("tag with size");

        // OK, try type + size in single byte


        switch (tagByte) {
            case NULL:
                return null;
            case STR:
                if (readStringAsCharSeq.get()) {
                    return readUtf8CharSeq(dis, dis.readInt());
                }
                return readStr(dis, dis.readInt());
            case EXTERN_STRING:
                return readExternString(dis);
            case DATE:
                return new Date(dis.readLong());
            case INT:
                int i = dis.readInt();
                log.trace("read {} INT TYPE {} dis={}", tagByte, i, dis.getClass().getName());
                return i;
            case BOOL_TRUE:
                return Boolean.TRUE;
            case BOOL_FALSE:
                return Boolean.FALSE;
            case FLOAT:
                return dis.readFloat();
            case DOUBLE:
                return dis.readDouble();
            case LONG:
                long l = dis.readLong();
                log.trace("read {} LONG TYPE {} dis={}", tagByte, l, dis.getClass().getName());
                return l;
            case BYTE:
                return dis.readByte();
            case SHORT:
                return dis.readShort();
            case MAP:
                return readMap(dis);
            case SOLRDOC:
                tagByte = read(dis);
                int size = readSize(dis);
                return readSolrDocument(dis, size);
            case SOLRDOCLST:
                return readSolrDocumentList(dis);
            case BYTEARR:
                return readByteArray(dis);
            case ITERATOR:
                return readIterator(dis);
            case END:
                return END_OBJ;
            case SOLRINPUTDOC:
              //  tagByte = read(dis);
                size = dis.readInt();//readSize(dis);
                return readSolrInputDocument(dis, size);
            case ENUM_FIELD_VALUE:
                return readEnumFieldValue(dis);
            case MAP_ENTRY:
                return readMapEntry(dis);
            case MAP_ENTRY_ITER:
                return readMapIter(dis);
            case VINT:
                return readVInt(dis);
            default:
     //           throw new UnsupportedOperationException("Unknown type " + tagByte + " string=" + readStr(dis));
        }
        switch (tagByte >>> 5) {
            case SINT >>> 5:
                return readSmallInt(dis);
            case SLONG >>> 5:
                return readSmallLong(dis);
            case ARR >>> 5:
                return readArray(dis, readSize(dis));
            case ORDERED_MAP >>> 5:
                return readOrderedMap(dis, readSize(dis));
            case NAMED_LST >>> 5:
                return readNamedList(dis, readSize(dis));
            case STD_EXTERN_STRING >>> 5:
                return readStdExternString(dis);
        }

        throw new UnsupportedOperationException("Unknown type " + tagByte);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean writeKnownType(Object val) throws IOException {
        while (true) {
            if (writePrimitive(val)) return true;
            if (val instanceof NamedList) {
                writeNamedList((NamedList<?>) val);
                return true;
            }
            if (val instanceof SolrDocumentList) { // SolrDocumentList is a List, so must come before List check
                writeSolrDocumentList((SolrDocumentList) val);
                return true;
            }
            if (val instanceof SolrDocument) {
                //this needs special treatment to know which fields are to be written
                writeSolrDocument((SolrDocument) val);
                return true;
            }
            if (val instanceof SolrInputDocument) {
                writeSolrInputDocument((SolrInputDocument) val);
                return true;
            }
            if (val instanceof SolrInputField) {
                val = ((SolrInputField) val).getValue();
                log.trace("write SolrInputField value={}", val);
                return true;
            }
            if (val instanceof IteratorWriter) {
                writeIterator((IteratorWriter) val);
                return true;
            }
            if (val instanceof MapWriter) {
                writeMap((MapWriter) val);
                return true;
            }
            if (val instanceof Map) {
                writeMap((Map) val);
                return true;
            }
            if (val instanceof Collection) {
                writeArray((Collection) val);
                return true;
            }
            if (val instanceof Object[]) {
                writeArray((Object[]) val);
                return true;
            }
            if (val instanceof Iterator) {
                writeIterator((Iterator) val);
                return true;
            }
            if (val instanceof Path) {
                writeStr(((Path) val).toAbsolutePath().toString());
                return true;
            }
            if (val instanceof Iterable) {
                writeIterator(((Iterable) val).iterator());
                return true;
            }
            if (val instanceof EnumFieldValue) {
                writeEnumFieldValue((EnumFieldValue) val);
                return true;
            }
            if (val instanceof Map.Entry) {
                writeMapEntry((Entry) val);
                return true;
            }
            if (val instanceof MapSerializable) {
                //todo find a better way to reuse the map more efficiently
                writeMap(((MapSerializable) val).toMap(new NamedList().asShallowMap()));
                return true;
            }
            if (val instanceof AtomicInteger) {
                writeSInt(((AtomicInteger) val).get());
                return true;
            }
            if (val instanceof AtomicLong) {
                writeTag(LONG);
                daos.writeLong(((AtomicLong) val).get());
                return true;
            }
            if (val instanceof AtomicBoolean) {
                writeBoolean(((AtomicBoolean) val).get());
                return true;
            }
            return false;
        }
    }

    private static class MyEntry implements Entry<Object, Object> {

        private final Object key;
        private final Object value;

        public MyEntry(Object key, Object value) {
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

        @Override
        public Object setValue(Object value) {
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
                return (this.key.equals(entry.getKey()) && this.value.equals(entry.getValue()));
            }
            return false;
        }
    }

    public static class BinEntryWriter implements MapWriter.EntryWriter {
        private final JavaBinCodec codec;

        BinEntryWriter(JavaBinCodec codec) {
            this.codec = codec;
        }


        @Override
        public MapWriter.EntryWriter put(CharSequence k, Object v) throws IOException {
            codec.writeExternString(k);
            codec.writeVal(v);
            return this;
        }

        @Override
        public MapWriter.EntryWriter put(CharSequence k, int v) throws IOException {
            codec.writeExternString(k);
            codec.writeSInt(v);
            return this;
        }

        @Override
        public MapWriter.EntryWriter put(CharSequence k, long v) throws IOException {
            codec.writeExternString(k);
            codec.writeTag(LONG);
            codec.daos.writeLong(v);
            return this;
        }

        @Override
        public MapWriter.EntryWriter put(CharSequence k, float v) throws IOException {
            codec.writeExternString(k);
            codec.writeFloat(v);
            return this;
        }

        @Override
        public MapWriter.EntryWriter put(CharSequence k, double v) throws IOException {
            codec.writeExternString(k);
            codec.writeDouble(v);
            return this;
        }

        @Override
        public MapWriter.EntryWriter put(CharSequence k, boolean v) throws IOException {
            codec.writeExternString(k);
            codec.writeBoolean(v);
            return this;
        }

        @Override
        public MapWriter.EntryWriter put(CharSequence k, CharSequence v) throws IOException {
            codec.writeExternString(k);
            codec.writeStr(v);
            return this;
        }

        private BiConsumer<CharSequence, Object> biConsumer;

        @Override
        public BiConsumer<CharSequence, Object> getBiConsumer() {
            if (biConsumer == null) biConsumer = MapWriter.EntryWriter.super.getBiConsumer();
            return biConsumer;
        }
    }

    public final BinEntryWriter ew = new BinEntryWriter(this);


    public void writeMap(MapWriter val) throws IOException {
        log.trace("write mapwriter {}", val.getClass().getName());

        writeTag(MAP_ENTRY_ITER);
        val.writeMap(ew);
        writeTag(END);
    }


    public void writeTag(byte tag) throws IOException {
        log.trace("write tag {}", name(tag));
        daos.writeByte(tag);
    }

    public void writeTag(byte tag, int size) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("write tag {} sz={}", name(tag), size);
        }

        if ((tag & 0xe0) != 0) {
            if (size < 0x1f) {
                daos.writeByte(tag | size);
            } else {
                daos.writeByte(tag | 0x1f);
                writeVInt(size - 0x1f, daos);
            }
        } else {
            daos.writeByte(tag);
            writeVInt(size, daos);
        }
    }

    public void writeByteArray(byte[] arr, int offset, int len) throws IOException {
        log.trace("write ByteArray");
        writeTag(BYTEARR, len);
        daos.write(arr, offset, len);
    }

    public byte[] readByteArray(InputStream dis) throws IOException {
        byte[] arr = new byte[readVInt(dis)];
        readFully(dis, arr);
        return arr;
    }

    public static void readFully(InputStream dis, byte[] b) throws IOException {
        readFully(dis, b, 0, b.length);
    }

    public static void readFully(InputStream dis, byte[] b, int off, int len) throws IOException {
        while (len > 0) {
            int ret = dis.read(b, off, len);
            if (ret == -1) {
                throw new EOFException();
            }
            off += ret;
            len -= ret;
        }
    }

    //use this to ignore the writable interface because , child docs will ignore the fl flag
    // is it a good design?
   private boolean ignoreWritable = false;
    private MapWriter.EntryWriter cew;

    public void writeSolrDocument(SolrDocument doc) throws IOException {
        log.trace("write SolrDocument");
        List<SolrDocument> children = doc.getChildDocuments();
        int fieldsCount = 0;
        if (writableDocFields == null || writableDocFields.wantsAllFields() || ignoreWritable) {
            fieldsCount = doc.size();
        } else {
            for (Entry<String, Object> e : doc) {
                if (toWrite(e.getKey())) fieldsCount++;
            }
        }
        int sz = fieldsCount + (children == null ? 0 : children.size());
        writeTag(SOLRDOC);
        writeTag(ORDERED_MAP, sz);
        if (cew == null) cew = new ConditionalKeyMapWriter.EntryWriterWrapper(ew, (k) -> toWrite(k.toString()));
        doc.writeMap(cew);
        if (children != null) {
            try {
                ignoreWritable = true;
                for (SolrDocument child : children) {
                    writeSolrDocument(child);
                }
            } finally {
                ignoreWritable = false;
            }
        }

    }

    protected boolean toWrite(String key) {
        return writableDocFields == null || writableDocFields.isWritable(key) || ignoreWritable;
    }

    public SolrDocument readSolrDocument(JavaBinInputStream dis, int sz) throws IOException {
        log.trace("read SolrDocument");

        SolrDocument doc = new SolrDocument(new LinkedHashMap<>(sz));
        for (int i = 0; i < sz; i++) {
            String fieldName;
            Object obj = readVal(dis); // could be a field name, or a child document
            if (obj instanceof SolrDocument) {
                doc.addChildDocument((SolrDocument) obj);
                continue;
            } else {
                fieldName = String.valueOf( obj);
            }
            Object fieldVal = readVal(dis);
            doc.setField(fieldName, fieldVal);
        }
        return doc;
    }

    public SolrDocumentList readSolrDocumentList(JavaBinInputStream dis) throws IOException {
        log.trace("read SolrDocumentList metadata");
        SolrDocumentList solrDocs = new SolrDocumentList();
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) readVal(dis);
        solrDocs.setNumFound((Long) list.get(0));
        solrDocs.setStart((Long) list.get(1));
        solrDocs.setMaxScore((Float) list.get(2));
        if (list.size() > 3) {
            solrDocs.setNumFoundExact((Boolean) list.get(3));
        }

        // The documents follow as a single array value (written by writeSolrDocumentList /
        // BinaryResponseWriter.writeResultsBody as writeTag(ARR, size) + writeSolrDocument...).
        // Read it with the normal value dispatch. The previous code read an int doc-count and
        // looped, but the wire actually carries an ARR; readInt() consumed the array header as a
        // bogus (often negative) count, so every document was dropped -> numFound>0 but size 0.
        @SuppressWarnings("unchecked")
        List<SolrDocument> documents = (List<SolrDocument>) readVal(dis);
        if (documents != null) {
            solrDocs.addAll(documents);
        }
        log.trace("read SolrDocuments {}", solrDocs);
        return solrDocs;
    }

    public void writeSolrDocumentList(SolrDocumentList docs)
            throws IOException {
        log.trace("write SolrDocumentList");
        writeTag(SOLRDOCLST);
        List<Object> l = new ArrayList<>(4);
        l.add(docs.getNumFound());
        l.add(docs.getStart());
        l.add(docs.getMaxScore());
        l.add(docs.getNumFoundExact());
        writeArray(l);

        // Write the documents as a single ARR value (matching BinaryResponseWriter.writeResultsBody
        // and what readSolrDocumentList expects). The previous writeInt(size)+loop emitted a raw
        // int that the reader could not frame back as the document array.
        writeTag(ARR, docs.size());
        for (SolrDocument doc : docs) {
            writeSolrDocument(doc);
        }
    }

    public SolrInputDocument readSolrInputDocument(JavaBinInputStream dis, int sz) throws IOException {
        log.trace("read SolrInputDocument");
    //    int sz = dis.readInt();//readVInt(dis);
//    float docBoost = (Float)readVal(dis);
//    if (docBoost != 1f) {
//      String message = "Ignoring document boost: " + docBoost + " as index-time boosts are not supported anymore";
//      if (WARNED_ABOUT_INDEX_TIME_BOOSTS.compareAndSet(false, true)) {
//        log.warn(message);
//      } else {
//        log.debug(message);
//      }
//    }
        SolrInputDocument sdoc = createSolrInputDocument(sz);
        for (int i = 0; i < sz; i++) {
            String fieldName;
            Object obj = readVal(dis); //  a field name, or a child document
            if (obj instanceof SolrInputDocument) {
                sdoc.addChildDocument((SolrInputDocument) obj);
                continue;
            } else {
                fieldName = String.valueOf(obj);
            }
            Object fieldVal = readVal(dis);
            log.trace("read SolrInputDocument field key={} value={}", fieldName, fieldVal);
            sdoc.setField(fieldName, fieldVal);
        }
        return sdoc;
    }

    protected SolrInputDocument createSolrInputDocument(int sz) {
        return new SolrInputDocument(new LinkedHashMap<>(sz));
    }

    static final Predicate<CharSequence> IGNORECHILDDOCS = it -> !CommonParams.CHILDDOC.equals(it.toString());

    /**
     * Writes the SOLRINPUTDOC tag and field-count header exactly as readObject()/readSolrInputDocument
     * expect to read it (a fixed 4-byte int). Callers that stream SolrInputDocument fields manually
     * (e.g. ExportTool's javabin sink) must use this to stay framing-compatible with the reader.
     */
    public void writeSolrInputDocumentHeader(int sz) throws IOException {
        writeTag(SOLRINPUTDOC);
        daos.writeInt(sz);
    }

    public void writeSolrInputDocument(SolrInputDocument sdoc) throws IOException {
        log.trace("write SolrDocument");
        List<SolrInputDocument> children = sdoc.getChildDocuments();
        int sz = sdoc.size() + (children == null ? 0 : children.size());
        writeTag(SOLRINPUTDOC);
        daos.writeInt(sz);
     //   writeTag(ORDERED_MAP, sz);
        sdoc.writeMap(new ConditionalKeyMapWriter.EntryWriterWrapper(ew, IGNORECHILDDOCS));
        if (children != null) {
            for (SolrInputDocument child : children) {
                writeSolrInputDocument(child);
            }
        }
    }


    public Map<Object, Object> readMapIter(JavaBinInputStream dis) throws IOException {
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
     * create a new Map object
     *
     * @param size expected size, -1 means unknown size
     */
    protected Map<Object, Object> newMap(int size) {
        return size < 0 ? new Object2ObjectLinkedOpenHashMap<>() : new Object2ObjectLinkedOpenHashMap<>(size);
    }

    public Map<Object, Object> readMap(JavaBinInputStream dis)
            throws IOException {
        log.trace("read Map");
        int sz = dis.readInt();
        return readMap(dis, sz);
    }

    protected Map<Object, Object> readMap(JavaBinInputStream dis, int sz) throws IOException {
        Map<Object, Object> m = newMap(sz);
        for (int i = 0; i < sz; i++) {
            Object key = readVal(dis);
            Object val = readVal(dis);
            m.put(key, val);

        }
        return m;
    }

    public final ItemWriter itemWriter = new ItemWriter() {
        @Override
        public ItemWriter add(Object o) throws IOException {
            writeVal(o);
            return this;
        }

        @Override
        public ItemWriter add(int v) throws IOException {
            writeSInt(v);
            return this;
        }

        @Override
        public ItemWriter add(long v) throws IOException {
            daos.writeByte(LONG);
            daos.writeLong(v);
            return this;
        }

        @Override
        public ItemWriter add(float v) throws IOException {
            writeFloat(v);
            return this;
        }

        @Override
        public ItemWriter add(double v) throws IOException {
            writeDouble(v);
            return this;
        }

        @Override
        public ItemWriter add(boolean v) throws IOException {
            writeBoolean(v);
            return this;
        }
    };

    @Override
    public void writeIterator(IteratorWriter val) throws IOException {
        log.trace("write IteratorWriter");
        writeTag(ITERATOR);
        val.writeIter(itemWriter);
        writeTag(END);
    }

    public void writeIterator(@SuppressWarnings({"rawtypes"}) Iterator iter) throws IOException {
        log.trace("write Iterator");
        writeTag(ITERATOR);
        while (iter.hasNext()) {
            writeVal(iter.next());
        }
        writeTag(END);
    }

    public List<Object> readIterator(JavaBinInputStream fis) throws IOException {
        log.trace("read Iterator");

        ArrayList<Object> l = new ArrayList<>();
        while (true) {
            Object o = readVal(fis);
            if (o == END_OBJ) break;
            l.add(o);
        }
        return l;
    }

    public void writeArray(@SuppressWarnings({"rawtypes"}) List l) throws IOException {
        log.trace("write Array List {}", l);
        final int size = l.size();
        writeTag(ARR, size);
        for (Object o : l) {
            writeVal(o);
        }
    }

    public void writeArray(@SuppressWarnings({"rawtypes"}) Collection coll) throws IOException {
        log.trace("write Array Collection {}", coll);
        writeTag(ARR, coll.size());
        for (Object o : coll) {
            writeVal(o);
        }

    }

    public void writeArray(Object[] arr) throws IOException {
        log.trace("write Array Primitive {}", Arrays.asList(arr));
        int sz = arr.length;
        writeTag(ARR, sz);
        for (Object o : arr) {
            writeVal(o);
        }
    }

  @SuppressWarnings({"unchecked"})
  public List<Object> readArray(JavaBinInputStream dis) throws IOException {
    int sz = readSize(dis);
    return readArray(dis, sz);
  }

    @SuppressWarnings({"rawtypes"})
    protected List readArray(JavaBinInputStream dis, int sz) throws IOException {
        log.trace("read array of size {}", sz);
        ObjectList<Object> l = new ObjectArrayList<>(sz);
        for (int i = 0; i < sz; i++) {
            l.add(readVal(dis));
        }

        if (log.isTraceEnabled()) {
            log.trace("return list {}", l);
            for (ObjectListIterator<Object> it = l.iterator(); it.hasNext(); ) {
                Object item = it.next();
                log.trace("return list {} {}", item == null ? "null" : item.getClass().getName(), l);
            }
        }

        return l;
    }

    /**
     * write {@link EnumFieldValue} as tag+int value+string value
     *
     * @param enumFieldValue to write
     */
    public void writeEnumFieldValue(EnumFieldValue enumFieldValue) throws IOException {
        log.trace("write enum");
        writeTag(ENUM_FIELD_VALUE);
        writeSInt(enumFieldValue.toInt());
        writeStr(enumFieldValue.toString());
    }

    public void writeMapEntry(Map.Entry<?, ?> val) throws IOException {
        log.trace("write MapEntry");
        writeTag(MAP_ENTRY);
        writeVal(val.getKey());
        writeVal(val.getValue());
    }

    /**
     * read {@link EnumFieldValue} (int+string) from input stream
     *
     * @param dis data input stream
     * @return {@link EnumFieldValue}
     */
    public EnumFieldValue readEnumFieldValue(JavaBinInputStream dis) throws IOException {
        log.trace("read enum");
        Integer intValue = (Integer) readVal(dis);
        String stringValue = (String) readVal(dis);
        return new EnumFieldValue(intValue, stringValue);
    }


    public Map.Entry<Object, Object> readMapEntry(JavaBinInputStream dis) throws IOException {
        log.trace("read MapEntry");
        final Object key = readVal(dis);
        final Object value = readVal(dis);
        return new MyEntry(key, value);
    }

    /**
     * write the string as tag+length, with length being the number of UTF-8 bytes
     */
    public void writeStr(CharSequence s) throws IOException {

        if (s instanceof Utf8CharSequence) {
            writeUTF8Str((Utf8CharSequence) s);
            return;
        }

        byte[] bytes = ((String) s).getBytes(UTF_8);
        writeTag(STR);

        if (log.isTraceEnabled()) {
            log.trace("write str {} len={}", s, s.length());
        }
        daos.writeInt(bytes.length);
        daos.write(bytes, 0, bytes.length);

    }

    protected static String readStr(JavaBinInputStream dis, int sz) throws IOException {


        byte[] bytes = new byte[sz];
        dis.readFully(dis, bytes);
        return new String(bytes, UTF_8);

    }

//    protected static CharSequence readUtf8(JavaBinInputStream dis) throws IOException {
//
//        return readStr(dis);
//    }

    /**
     * Read a string as a {@link ByteArrayUtf8CharSequence} when {@link #readStringAsCharSeq} is enabled.
     * Strings up to CHARSEQ_BUFSIZE bytes share the same underlying byte[] per page; larger strings
     * get their own allocation. This matches the upstream buffer-sharing contract.
     */
    protected Object readUtf8CharSeq(JavaBinInputStream dis, int sz) throws IOException {
        byte[] tmp = new byte[sz];
        dis.readFully(dis, tmp);
        if (sz > CHARSEQ_BUFSIZE) {
            // too large for the shared buffer; give it its own allocation
            return new ByteArrayUtf8CharSequence(tmp, 0, sz);
        }
        if (charseqBuf == null || charseqBufPos + sz > charseqBuf.length) {
            charseqBuf = new byte[CHARSEQ_BUFSIZE];
            charseqBufPos = 0;
        }
        System.arraycopy(tmp, 0, charseqBuf, charseqBufPos, sz);
        ByteArrayUtf8CharSequence result = new ByteArrayUtf8CharSequence(charseqBuf, charseqBufPos, sz);
        charseqBufPos += sz;
        return result;
    }

    public void writeSInt(int v) throws IOException {
        log.trace("write sint {}", v);
        if (v > 0) {
            int b = SINT | (v & 0x0f);

            if (v >= 0x0f) {
                b |= 0x10;
                daos.writeByte(b);
                writeVInt(v >>> 4, daos);
                log.trace("writesint var {}", b);
            } else {
                log.trace("writesint single byte {}", b);
                daos.writeByte(b);
            }

        } else {
            log.trace("writesint data out");
            int b = SINT | (v & 0x0f);
            b |= 0x10;
            daos.writeByte(b);
            writeVInt(v >>> 4, daos);
        }
    }

    public int readSmallInt(InputStream dis) throws IOException {
        log.trace("read small int");
        int v = tagByte & 0x0F;
        if ((tagByte & 0x10) != 0)
            v = (readVInt(dis) << 4) | v;
        return v;
    }

    public void writeLong(long val) throws IOException {

        log.trace("write long");
        if ((val & 0xff00000000000000L) == 0) {
            int b = SLONG | ((int) val & 0x0f);
            if (val >= 0x0f) {
                b |= 0x10;
                daos.writeByte(b);
                writeVLong(val >>> 4, daos);
            } else {
                daos.writeByte(b);
            }
        } else {
            daos.writeByte(LONG);
            daos.writeLong(val);
        }
    }

    public long readSmallLong(InputStream dis) throws IOException {
        log.trace("read small long");
        long v = tagByte & 0x0F;
        if ((tagByte & 0x10) != 0)
            v = (readVLong(dis) << 4) | v;
        return v;
    }

    public void writeFloat(float val) throws IOException {
        log.trace("write float {}", val);
        daos.writeByte(FLOAT);
        daos.writeFloat(val);
    }

    public boolean writePrimitive(Object val) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("write primitive val={} class={}", val, val == null ? null : val.getClass().getName());
        }

        if (val == null) {
            daos.writeByte(NULL);
            return true;
        } else if (val instanceof Utf8CharSequence) {
            writeUTF8Str((Utf8CharSequence) val);
            return true;
        } else if (val instanceof CharSequence) {
            writeStr((CharSequence) val);
            return true;
        } else if (val instanceof Number) {
            if (val instanceof Integer) {
                log.trace("write prim INT TYPE");
                writeTag(INT);
                daos.writeInt((int) val);
                return true;
            } else if (val instanceof Long) {
                writeTag(LONG);
                daos.writeLong((Long) val);
                return true;
            } else if (val instanceof Float) {
                writeFloat((float) val);
                return true;
            } else if (val instanceof Double) {
                writeDouble((double) val);
                return true;
            } else if (val instanceof Byte) {
                log.trace("write BYTE");
                daos.writeByte(BYTE);
                daos.writeByte(((Byte) val).intValue());
                return true;
            } else if (val instanceof Short) {
                log.trace("write short to out {}", val);
                daos.writeByte(SHORT);
                daos.writeShort((short) val);
                return true;
            }
            return false;

        } else if (val instanceof Date) {
            daos.writeByte(DATE);
            daos.writeLong(((Date) val).getTime());
            return true;
        } else if (val instanceof Boolean) {
            writeBoolean((Boolean) val);
            return true;
        } else if (val instanceof byte[]) {
            writeByteArray((byte[]) val, 0, ((byte[]) val).length);
            return true;
        } else if (val instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) val;
            writeByteArray(buf.array(), buf.position(), buf.limit() - buf.position());
            return true;
        } else if (val == END_OBJ) {
            writeTag(END);
            return true;
        }
        return false;
    }

    protected void writeBoolean(boolean val) throws IOException {
        log.trace("write bool");
        if (val) daos.writeByte(BOOL_TRUE);
        else daos.writeByte(BOOL_FALSE);
    }

    protected void writeDouble(double val) throws IOException {
        log.trace("write double");
        writeDoubleToOut(val);
    }


    public void writeMap(Map<?, ?> val) throws IOException {
        //log.trace("write map");

        if (val instanceof MapWriter) {
            writeMap((MapWriter) val);
            return;
        }

        writeTag(MAP);
        daos.writeInt(val.size());
        for (Map.Entry<?, ?> entry : val.entrySet()) {
            Object key = entry.getKey();
            if (key instanceof String) {
                writeStr((String) key);
            } else {
                writeVal(key);
            }
            writeVal(entry.getValue());
        }
    }


    public int readSize(InputStream in) throws IOException {
        int sz = tagByte & 0x1f;
        if (sz == 0x1f) sz += readVInt(in);
        log.trace("read sz {}", sz);
        return sz;
    }


    /**
     * Special method for variable length int (copied from lucene). Usually used for writing the length of a
     * collection/array/map In most of the cases the length can be represented in one byte (length &lt; 127) so it saves 3
     * bytes/object
     *
     * @throws IOException If there is a low-level I/O error.
     */
    public static void writeVInt(int i, JavaBinOutputStream out) throws IOException {
        i = encodeZigZag32(i);
        while ((i & ~0x7F) != 0) {
            out.writeByte((i & 0x7f) | 0x80);
            i >>>= 7;
        }
        out.writeByte(i);
    }

    public static int encodeZigZag32(final int n) {
        // Note:  the right-shift must be arithmetic
        return n;
        //return (n << 1) ^ (n >> 31);
    }

    public static int decodeZigZag32(final int n) {
        return n;
        //return (n >>> 1) ^ -(n & 1);
    }

    public static long encodeZigZag64(final long n) {
        return n;
        // Note:  the right-shift must be arithmetic
        // return (n << 1) ^ (n >> 63);
    }

    public static long decodeZigZag64(final long n) {
        return n;
        // return (n >>> 1) ^ -(n & 1);
    }

    /**
     * The counterpart for #writeVInt(int)
     *
     * @throws IOException If there is a low-level I/O error.
     */
    public static int readVInt(InputStream in) throws IOException {
        byte b = read(in);
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = read(in);
            i |= (b & 0x7F) << shift;
        }
        return decodeZigZag32(i);
    }


    protected static byte read(InputStream in) throws IOException {
        int r = in.read();
        if (r == -1) {
            throw new EOFException();
        }
        return (byte) r;
    }


    public void writeVLong(long i, OutputStream out) throws IOException {
        log.trace("write vlong");
        i = encodeZigZag64(i);
        while ((i & ~0x7F) != 0) {
            daos.writeByte((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        daos.writeByte((byte) i);
    }

    public static float readFloat(JavaBinInputStream is) throws IOException {
        return is.readFloat();
    }

    public static double readDouble(JavaBinInputStream is) throws IOException {
        log.trace("read double");
        return is.readDouble();
    }



    public static int readUnsignedByte(InputStream in) throws IOException {
        return in.read() & 0xff;
    }

    public static long readVLong(InputStream in) throws IOException {
        log.trace("read vlong");
        byte b = read(in);
        long i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = read(in);
            i |= (long) (b & 0x7F) << shift;
        }
        return decodeZigZag64(i);
    }

    private static final String[] stdStringsList;

    private static final Object2IntMap<String> stdStringsMap = new Object2IntLinkedOpenHashMap<>(128, 0.25f);

    private static int stdSstringsCount = 0;
    private int stringsCount = 0;
    private Object2IntLinkedOpenHashMap<String> stringsMap;
    private List<CharSequence> stringsList;

    static {
        stdStringsMap.defaultReturnValue(-1);
        stdStringsMap.put("update.distrib", stdSstringsCount++);
        stdStringsMap.put("docsMap", stdSstringsCount++);
        stdStringsMap.put("_version_", stdSstringsCount++);
        stdStringsMap.put("delByQ", stdSstringsCount++);
        stdStringsMap.put("distrib.from", stdSstringsCount++);
        stdStringsMap.put("id", stdSstringsCount++);
        stdStringsMap.put("params", stdSstringsCount++);
        stdStringsMap.put("df", stdSstringsCount++);
        stdStringsMap.put("zkConnected", stdSstringsCount++);
        stdStringsMap.put("QTime", stdSstringsCount++);
        stdStringsMap.put("_stateVer_", stdSstringsCount++);
        stdStringsMap.put("echoParams", stdSstringsCount++);
        stdStringsMap.put("_root_", stdSstringsCount++);
        stdStringsMap.put("responseHeader", stdSstringsCount++);
        stdStringsMap.put("collection", stdSstringsCount++);
        stdStringsMap.put("failure", stdSstringsCount++);
        stdStringsMap.put("text", stdSstringsCount++);
        stdStringsMap.put("NOW", stdSstringsCount++);

        stdStringsMap.put("single", stdSstringsCount++);
        stdStringsMap.put("multi", stdSstringsCount++);
        stdStringsMap.put("state", stdSstringsCount++);
        stdStringsMap.put("warning", stdSstringsCount++);
        stdStringsMap.put("requestid", stdSstringsCount++);

        stdStringsMap.put("metrics", stdSstringsCount++);
        stdStringsMap.put("delByIdMap", stdSstringsCount++);
        stdStringsMap.put("solr.node", stdSstringsCount++);

        stdStringsMap.put("exception", stdSstringsCount++);
        stdStringsMap.put("rspCode", stdSstringsCount++);


        stdStringsMap.put("error", stdSstringsCount++);
        stdStringsMap.put("error-class", stdSstringsCount++);
        stdStringsMap.put("metadata", stdSstringsCount++);
        stdStringsMap.put("code", stdSstringsCount++);
        stdStringsMap.put("core", stdSstringsCount++);
        stdStringsMap.put("sort_values", stdSstringsCount++);
        stdStringsMap.put("versions", stdSstringsCount++);
        stdStringsMap.put("docs", stdSstringsCount++);

        stdStringsMap.put("stats", stdSstringsCount++);
        stdStringsMap.put("stats_fields", stdSstringsCount++);

        stdStringsMap.put("missing", stdSstringsCount++);
        stdStringsMap.put("count", stdSstringsCount++);
        stdStringsMap.put("max", stdSstringsCount++);

        stdStringsMap.put("debug", stdSstringsCount++);
        stdStringsMap.put("rawquerystring", stdSstringsCount++);
        stdStringsMap.put("parsedquery", stdSstringsCount++);
        stdStringsMap.put("explain", stdSstringsCount++);
        stdStringsMap.put("QParser", stdSstringsCount++);
        stdStringsMap.put("filter_queries", stdSstringsCount++);

        stdStringsMap.put("querystring", stdSstringsCount++);
        stdStringsMap.put("Response", stdSstringsCount++);
        stdStringsMap.put("NumFound", stdSstringsCount++);

        stdStringsMap.put("RequestPurpose", stdSstringsCount++);
        stdStringsMap.put("ElapsedTime", stdSstringsCount++);
        stdStringsMap.put("EXECUTE_QUERY", stdSstringsCount++);

        stdStringsMap.put("track", stdSstringsCount++);
        stdStringsMap.put("cat", stdSstringsCount++);

        stdStringsMap.put("timing", stdSstringsCount++);
        stdStringsMap.put("time", stdSstringsCount++);
        stdStringsMap.put("facet_module", stdSstringsCount++);

        stdStringsMap.put("prepare", stdSstringsCount++);
        stdStringsMap.put("query", stdSstringsCount++);
        stdStringsMap.put("facet", stdSstringsCount++);

        stdStringsMap.put("mlt", stdSstringsCount++);
        stdStringsMap.put("highlight", stdSstringsCount++);
        stdStringsMap.put("expand", stdSstringsCount++);

        stdStringsMap.put("terms", stdSstringsCount++);
        stdStringsMap.put("process", stdSstringsCount++);

        stdStringsMap.put("indexversion", stdSstringsCount++);
        stdStringsMap.put("generation", stdSstringsCount++);
        stdStringsMap.put("indent", stdSstringsCount++);
        stdStringsMap.put("fingerprint", stdSstringsCount++);
        stdStringsMap.put("trace", stdSstringsCount++);
        stdStringsMap.put("omitHeader", stdSstringsCount++);
        stdStringsMap.put("rf", stdSstringsCount++);
        stdStringsMap.put("start", stdSstringsCount++);
        stdStringsMap.put("isShard", stdSstringsCount++);
        stdStringsMap.put("fsv", stdSstringsCount++);
        stdStringsMap.put("content_t", stdSstringsCount++);
        stdStringsMap.put("STATUS", stdSstringsCount++);
        stdStringsMap.put("rows", stdSstringsCount++);
        stdStringsMap.put("version", stdSstringsCount++);
        stdStringsMap.put("q", stdSstringsCount++);
        stdStringsMap.put("response", stdSstringsCount++);
        stdStringsMap.put("json", stdSstringsCount++);
        stdStringsMap.put("wt", stdSstringsCount++);
        stdStringsMap.put("status", stdSstringsCount++);
        stdStringsMap.put("success", stdSstringsCount++);
        stdStringsMap.put("msg", stdSstringsCount++);
        stdStringsMap.put("score", stdSstringsCount++);
        stdStringsMap.put("distrib", stdSstringsCount++);
        stdStringsMap.put("shards.purpose", stdSstringsCount++);

        stdStringsList = stdStringsMap.keySet().toArray(new String[0]);
    }


    public void writeExternString(CharSequence s) throws IOException {
        if (s == null) {
            writeTag(NULL);
            return;
        }
        if (s.length() > 15) {
            writeStr(s);
            return;
        }

        log.trace("writeExternString {}", s);

        int idx = 0;

        if (stdStrings) {
            idx = stdStringsMap.getInt(s);
        }

        if (idx > 0) {
            writeTag(STD_EXTERN_STRING, idx);
            return;
        } else {
            idx = stringsMap == null ? 0 : stringsMap.getInt(s);
        }

        writeTag(EXTERN_STRING);
        daos.writeInt(idx);
        log.trace("write string sz {}", idx);

        if (idx == 0) {
            writeStr(s);
            if (stringsMap == null) {
                stringsMap = new Object2IntLinkedOpenHashMap<>(32, 0.5f);
            }
            stringsMap.put(s.toString(), ++stringsCount);
        }

        log.trace("write ext str idx={} cnt={}", idx, stringsCount);
    }

    public CharSequence readExternString(JavaBinInputStream fis) throws IOException {
        int idx = fis.readInt();
        log.trace("read extern idx={}", idx);


        if (idx > 0) {// idx != 0 is the index of the extern string

            if (stringsList == null) {
                throw new NullPointerException("stringsList should not be null idx=" + idx);
            }
            CharSequence s = stringsList.get(idx - 1);
            log.trace("read extern string idx={} s={}", idx, s);
            return s;
        }
        // idx == 0 means it has a string value
        tagByte = read(fis);

        String s = readStr(fis, fis.readInt());

        if (stringsList == null) stringsList = new ArrayList<>();
        stringsList.add(s);
        //log.info("read extern string idx={} s={}", idx, s);
        return s;

    }

    public CharSequence readStdExternString(JavaBinInputStream fis) throws IOException {
        int idx = readSize(fis);
        log.trace("read std extern idx={}", idx);
        CharSequence s = stdStringsList[idx];
        log.trace("read std extern string idx={} s={}", idx, s);
        return s;
    }


    public void writeUTF8Str(Utf8CharSequence utf8) throws IOException {
        log.trace("write utf8str {} len={}", utf8, utf8.size());


        byte[] buf = ((ByteArrayUtf8CharSequence) utf8).getBuf();
        int offset = ((ByteArrayUtf8CharSequence) utf8).offset();
        int sz = utf8.size();
        writeTag(STR);
        daos.writeInt(sz);

        daos.write(buf, offset, sz);


        //  writeTag(STR, utf8.size());
        // writeUtf8CharSeq(utf8);
    }

    public void writeDoubleToOut(double v) throws IOException {
        log.trace("write double to out {}", v);
        daos.write(DOUBLE);
        daos.writeLong(Double.doubleToRawLongBits(v));
    }

    /**
     * Allows extension of {@link JavaBinCodec} to support serialization of arbitrary data types.
     * <p>
     * Implementors of this interface write a method to serialize a given object using an existing {@link JavaBinCodec}
     */
    public interface ObjectResolver {
        /**
         * Examine and attempt to serialize the given object, using a {@link JavaBinCodec} to write it to a stream.
         *
         * @param o     the object that the caller wants serialized.
         * @param codec used to actually serialize {@code o}.
         * @return the object {@code o} itself if it could not be serialized, or {@code null} if the whole object was successfully serialized.
         * @see JavaBinCodec
         */
        Object resolve(Object o, JavaBinCodec codec) throws IOException;
    }

    public interface WritableDocFields {
        boolean isWritable(String name);

        boolean wantsAllFields();
    }

    @Override
    public void close() throws IOException {
        try {
            if (daos != null) {
                daos.flush();
            }
//            if (stringsMap != null) {
//                log.error("Extern String Stats {}", stringsMap);
//            }
        } finally {
            if (buffer != null) {
                BufferUtil.free(buffer);
            }
        }
    }

    private static String name(byte tagByte) {
        switch (tagByte >>> 5) {

            case SINT >>> 5:
                return "SINT";
            case SLONG >>> 5:
                return "SLONG";
            case ARR >>> 5:
                return "ARR";
            case ORDERED_MAP >>> 5:
                return "ORDERED_MAP";
            case NAMED_LST >>> 5:
                return "NAMED_LST";
            case STD_EXTERN_STRING >>> 5:
                return "STD_EXTERN_STRING";
        }

       // log.trace("norm tag");
        switch (tagByte) {
            case NULL:
                return "NULL";
            case STR:
                return "STR";
            case DATE:
                return "DATE";
            case INT:
                return "INT";
            case BOOL_TRUE:
                return "BOOL_TRUE";
            case BOOL_FALSE:
                return "BOOL_FALSE";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case LONG:
                return "LONG";
            case BYTE:
                return "BYTE";
            case SHORT:
                return "SHORT";
            case MAP:
                return "MAP";
            case SOLRDOC:
                return "SOLRDOC";
            case SOLRDOCLST:
                return "SOLRDOCLST";
            case BYTEARR:
                return "BYTEARR";
            case ITERATOR:
                return "ITERATOR";
            case END:
                return "END";
            case SOLRINPUTDOC:
                return "SOLRINPUTDOC";
            case ENUM_FIELD_VALUE:
                return "ENUM_FIELD_VALUE";
            case MAP_ENTRY:
                return "MAP_ENTRY";
            case MAP_ENTRY_ITER:
                return "MAP_ENTRY_ITER";
            case VINT:
                return "VINT";
            case EXTERN_STRING:
                return "EXTERN_STRING";
        }
        throw new IllegalArgumentException("Unknown tag=" + tagByte);
    }

}
