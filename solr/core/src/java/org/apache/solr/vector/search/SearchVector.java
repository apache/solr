package org.apache.solr.vector.search;


import java.util.Arrays;

/**
 * A SearchVector composed of two elements, 
 * the embeddings as a float array, and the name of the field that the embeddings occurred in.
 */

public final class SearchVector{

  private String field;
  private float[] vector;
  
  public SearchVector(String field, float[] vector) {
    this.field = field;
    this.vector = vector;
  }

  public SearchVector(String field) {
    this(field, null);
  }

  /**
   * Returns the field of this SearchVector. The field indicates the part of a document which this term came
   * from.
   */
  public final String field() {
    return this.field;
  }
  public final String text() {
    return Arrays.toString(this.vector);
  }

  public final float[] getVector() {
    return this.vector;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    SearchVector other = (SearchVector) obj;
    if (field == null) {
      if (other.field != null) return false;
    } else if (!field.equals(other.field)) return false;
    if (vector == null) {
      if (other.vector != null) return false;
    } else if (!Arrays.equals(vector, other.vector)) return false;
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((field == null) ? 0 : field.hashCode());
    result = prime * result + ((vector == null) ? 0 : vector.hashCode());
    return result;
  }

  /**
   * Resets the field and embeddings of a SearchVector.
   */
  final void set(String field, float[] vector) {
    this.field = field;
    this.vector = vector;
  }

  @Override
  public final String toString() {
    return this.field + ":" + Arrays.toString(this.vector);
  }


}
