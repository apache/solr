package com.flipkart.neo.solr.ltr.banner.scorer;


import com.flipkart.neo.content.ranking.automation.StaticFileBasedModelClient;

public class StaticFileBasedModelClientHelper {

  private static final String STATIC_MODEL_BASE_PATH = "/usr/share/fk-neo-solr/offlinemodels/";
  private static final String L1_PCTR_MODEL_PATH = "NEO_neo_pctr_l1_2020_03_18_11_13_03_34.tsv";
  private static final String PCVR_MODEL_PATH = "NEO_neo_pcvr_hp_random_2020_03_18_00_55_56_105.tsv";
  private static final String PCTR_MODEL_PATH = "NEO_neo_pctr_2020_03_18_09_10_59_95.tsv";

  private StaticFileBasedModelClientHelper(){}

  public static StaticFileBasedModelClient getModelClient(){
    return new StaticFileBasedModelClient(STATIC_MODEL_BASE_PATH, L1_PCTR_MODEL_PATH, PCVR_MODEL_PATH, PCTR_MODEL_PATH);
  }

}