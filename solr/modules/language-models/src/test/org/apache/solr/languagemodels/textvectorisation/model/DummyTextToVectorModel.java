package org.apache.solr.languagemodels.textvectorisation.model;

public class DummyTextToVectorModel implements TextToVectorModel{
  private final float[] vector;

  public DummyTextToVectorModel(float[] vector) {
    this.vector = vector;
  }

  @Override
  public float[] vectorise(String text) {
    return vector;
  }
}
