package org.apache.solr.schema;

class FloatDenseVectorParser extends DenseVectorParser {
  private float[] vector;
  private int curPosition;

  public FloatDenseVectorParser(int dimension, Object inputValue, BuilderPhase builderPhase) {
    this.dimension = dimension;
    this.inputValue = inputValue;
    this.curPosition = 0;
    this.builderPhase = builderPhase;
  }

  @Override
  public float[] getFloatVector() {
    if (vector == null) {
      vector = new float[dimension];
      parseVector();
    }
    return vector;
  }

  @Override
  protected void addNumberElement(Number element) {
    vector[curPosition++] = element.floatValue();
  }

  @Override
  protected void addStringElement(String element) {
    vector[curPosition++] = Float.parseFloat(element);
  }

  @Override
  protected String errorMessage() {
    return "The expected format is:'[f1,f2..f3]' where each element f is a float";
  }
}
