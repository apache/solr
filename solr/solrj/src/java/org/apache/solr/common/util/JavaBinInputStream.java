package org.apache.solr.common.util;

import java.io.IOException;
import java.io.InputStream;

public abstract class JavaBinInputStream extends InputStream {

    public abstract int readInt() throws IOException;

    public abstract long readLong() throws IOException;

    public abstract float readFloat() throws IOException;

    public abstract double readDouble() throws IOException;

    public abstract byte readByte() throws IOException;

    public abstract short readShort() throws IOException;

    public abstract void readFully(JavaBinInputStream dis, byte[] bytes) throws IOException;
}
