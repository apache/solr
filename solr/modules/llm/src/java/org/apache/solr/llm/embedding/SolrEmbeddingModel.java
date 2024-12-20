package org.apache.solr.llm.embedding;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.DimensionAwareEmbeddingModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.llm.store.EmbeddingModelException;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;


public class SolrEmbeddingModel implements Accountable {
    private static final long BASE_RAM_BYTES =
            RamUsageEstimator.shallowSizeOfInstance(SolrEmbeddingModel.class);
    public static final String TIMEOUT_PARAM = "timeout";
    public static final String LOG_REQUESTS_PARAM = "logRequests";
    public static final String LOG_RESPONSES_PARAM = "logResponses";
    public static final String MAX_SEGMENTS_PER_BATCH_PARAM = "maxSegmentsPerBatch";
    public static final String MAX_RETRIES_PARAM = "maxRetries";

    protected final String name;
    private final Map<String, Object> params;
    private EmbeddingModel embedder;
    private Integer hashCode;

    public static SolrEmbeddingModel getInstance(
            String className,
            String name,
            Map<String, Object> params)
            throws EmbeddingModelException {
        try {
            EmbeddingModel embedder;
            Class<?> modelClass = Class.forName(className);
            var builder = modelClass.getMethod("builder").invoke(null);
            for (String paramName : params.keySet()) {
                switch (paramName) {
                    case TIMEOUT_PARAM:
                        Duration timeOut = Duration.ofSeconds((Long) params.get(paramName));
                        builder.getClass().getMethod(paramName, Duration.class).invoke(builder, timeOut);
                        break;
                    case LOG_REQUESTS_PARAM:
                        builder.getClass().getMethod(paramName, Boolean.class).invoke(builder, params.get(paramName));
                        break;
                    case LOG_RESPONSES_PARAM:
                        builder.getClass().getMethod(paramName, Boolean.class).invoke(builder, params.get(paramName));
                        break;
                    case MAX_SEGMENTS_PER_BATCH_PARAM:
                        builder.getClass().getMethod(paramName, Integer.class).invoke(builder, ((Long)params.get(paramName)).intValue());
                        break;
                    case MAX_RETRIES_PARAM:
                        builder.getClass().getMethod(paramName, Integer.class).invoke(builder, ((Long)params.get(paramName)).intValue());
                        break;
                    default:
                        builder.getClass().getMethod(paramName, String.class).invoke(builder, params.get(paramName));
                }
            }
            embedder = (EmbeddingModel) builder.getClass().getMethod("build").invoke(builder);
            return new SolrEmbeddingModel(name, embedder, params);
        } catch (final Exception e) {
             throw new EmbeddingModelException("Model loading failed for " + className, e);
        }
    }
    
    public SolrEmbeddingModel(String name, EmbeddingModel embedder, Map<String, Object> params) {
        this.name = name;
        this.embedder = embedder;
        this.params = params;
    }

    public float[] floatVectorise(String text){
        Embedding vector = embedder.embed(text).content();
        return vector.vector();
    }

    public byte[] byteVectorise(String text){
        return new byte[0];
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(name=" + getName() + ")";
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES
                + RamUsageEstimator.sizeOfObject(name)
                + RamUsageEstimator.sizeOfObject(embedder);
    }
    @Override
    public int hashCode() {
        if (hashCode == null) {
            hashCode = calculateHashCode();
        }
        return hashCode;
    }

    private int calculateHashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + Objects.hashCode(name);
        result = (prime * result) + Objects.hashCode(embedder);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof SolrEmbeddingModel)) return false;
        final SolrEmbeddingModel other = (SolrEmbeddingModel) obj;
        return Objects.equals(embedder, other.embedder)
                && Objects.equals(name, other.name);
    }

    public String getName() {
        return name;
    }

    public EmbeddingModel getEmbedder() {
        return embedder;
    }

    public void setEmbedder(DimensionAwareEmbeddingModel embedder) {
        this.embedder = embedder;
    }

    public Map<String, Object> getParams() {
        return params;
    }
}
