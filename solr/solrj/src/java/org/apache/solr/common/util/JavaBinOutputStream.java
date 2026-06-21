package org.apache.solr.common.util;

import java.io.IOException;
import java.io.OutputStream;

public abstract class JavaBinOutputStream  extends OutputStream {

    public abstract void writeInt(int b) throws IOException;

    public abstract void writeLong(long v) throws IOException;

    public void writeByte(int b) throws IOException {
        write((byte)b);
    }

    public abstract void writeFloat(float val) throws IOException;

    public abstract void writeShort(short val) throws IOException;
}
