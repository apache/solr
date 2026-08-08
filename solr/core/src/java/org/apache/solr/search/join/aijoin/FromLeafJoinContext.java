package org.apache.solr.search.join.aijoin;

class FromLeafJoinContext {
  final AIJoinUtil.CacheAndCount matches;
  final ForeignKeyColumn fkColumn;

  public FromLeafJoinContext(AIJoinUtil.CacheAndCount matches, ForeignKeyColumn fkColumn) {
    this.matches = matches;
    this.fkColumn = fkColumn;
  }
}
