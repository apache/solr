package org.apache.solr.update;

import it.unimi.dsi.fastutil.io.MeasurableStream;
import it.unimi.dsi.fastutil.io.RepositionableStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

public class GetChannelInputStream extends InputStream implements MeasurableStream, RepositionableStream {
  private final InputStream inputStream;
  private final FileChannel ch;

  public GetChannelInputStream(FileChannel ch) {
    this.inputStream = Channels.newInputStream((FileChannel) ch);
    this.ch = ch;
  }
  
  public Channel getChannel() {
    return ch;
  }

  @Override
  public int read() throws IOException {
    return inputStream.read();
  }

  public void close() throws IOException {
    inputStream.close();
  }

  @Override
  public long length() throws IOException {
    return ch.size();
  }

  @Override
  public void position(long newPosition) throws IOException {
    ch.position(newPosition);
  }

  @Override
  public long position() throws IOException {
    return ch.position();
  }
}
