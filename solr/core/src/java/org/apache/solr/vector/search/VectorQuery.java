package org.apache.solr.vector.search;


import java.util.Objects;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;


public class VectorQuery extends Query {

  private final SearchVector searchVector;
  
  public VectorQuery(SearchVector searchVector) {
    this.searchVector = Objects.requireNonNull(searchVector);
  }

  public SearchVector getSearchVector() {
    return this.searchVector;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!this.searchVector.field().equals(field)) {
      buffer.append(this.searchVector.field());
      buffer.append(":");
    }
    buffer.append(this.searchVector.text());
    return buffer.toString();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && this.searchVector.equals(((VectorQuery) other).searchVector);
  }

  @Override
  public int hashCode() {
    return classHash() ^ this.searchVector.hashCode();
  }


  @Override
  public void visit(QueryVisitor visitor) {
  }
}
