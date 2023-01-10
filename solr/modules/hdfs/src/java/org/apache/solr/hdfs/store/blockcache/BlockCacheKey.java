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
package org.apache.solr.hdfs.store.blockcache;

import java.util.Objects;

/**
 * @lucene.experimental
 */
public class BlockCacheKey implements Cloneable {

  private long block;
  private int file;
  private String path;

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public long getBlock() {
    return block;
  }

  public int getFile() {
    return file;
  }

  public void setBlock(long block) {
    this.block = block;
  }

  public void setFile(int file) {
    this.file = file;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (block ^ (block >>> 32));
    result = prime * result + file;
    result = prime * result + ((path == null) ? 0 : path.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof BlockCacheKey)) return false;
    BlockCacheKey other = (BlockCacheKey) obj;
    return block == other.block && file == other.file && Objects.equals(path, other.path);
  }

  @Override
  public BlockCacheKey clone() {
    try {
      return (BlockCacheKey) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}
