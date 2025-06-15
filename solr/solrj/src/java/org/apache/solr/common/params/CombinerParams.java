package org.apache.solr.common.params;

public class CombinerParams {

    private CombinerParams() {}
    public static final String COMBINER = "combiner";
    public static final String COMBINER_ALGORITHM = COMBINER + ".algorithm";
    public static final String COMBINER_QUERY = COMBINER + ".query";
    public static final String RECIPROCAl_RANK_FUSION = "rrf";
    public static final String COMBINER_UP_TO = COMBINER + ".upTo";
    public static final String COMBINER_RRF_K = COMBINER + "." + RECIPROCAl_RANK_FUSION + ".k";
    public static final int COMBINER_UP_TO_DEFAULT = 100;
    public static final int COMBINER_RRF_K_DEFAULT = 60;
}
