package org.apache.solr.llm.embedding;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.cohere.CohereEmbeddingModel;
import dev.langchain4j.model.embedding.DimensionAwareEmbeddingModel;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.llm.store.EmbeddingModelException;
import org.apache.solr.util.SolrPluginUtils;
import java.util.Map;
import java.util.Objects;


public class EmbeddingModel implements Accountable {
    private static final long BASE_RAM_BYTES =
            RamUsageEstimator.shallowSizeOfInstance(EmbeddingModel.class);
    
    protected final String name;
    private final Map<String, Object> params;
    private DimensionAwareEmbeddingModel embedder;
    private Integer hashCode;

    public static EmbeddingModel getInstance(
            SolrResourceLoader solrResourceLoader,
            String className,
            String name,
            Map<String, Object> params)
            throws EmbeddingModelException {
        final DimensionAwareEmbeddingModel embedder;
        try {
//            // create an instance of the model
//            embedder =
//                    solrResourceLoader.newInstance(
//                            className,
//                            DimensionAwareEmbeddingModel.class,
//                            new String[0], // no sub packages
//                            new Class<?>[] {
//                                    String.class, Map.class
//                            },
//                            new Object[] {name, params});
//            if (params != null) {
//                SolrPluginUtils.invokeSetters(embedder, params.entrySet());
//            }
            
            //how to instantiate embedder?
            embedder = CohereEmbeddingModel.builder()
                    .baseUrl(System.getenv("OPENAI_BASE_URL"))
                    .apiKey(System.getenv("OPENAI_API_KEY"))
                    .modelName("")
                    .logRequests(true)
                    .logResponses(true)
                    .build(); //this need to happen through inversion of control, where for each json param a method of the builder is called with the parameter the json value
        } catch (final Exception e) {
            throw new EmbeddingModelException("Model loading failed for " + className, e);
        }
        return new EmbeddingModel(name, embedder, params);
    }
    
    public EmbeddingModel(String name, DimensionAwareEmbeddingModel embedder, Map<String, Object> params) {
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
        if (!(obj instanceof EmbeddingModel)) return false;
        final EmbeddingModel other = (EmbeddingModel) obj;
        return Objects.equals(embedder, other.embedder)
                && Objects.equals(name, other.name);
    }

    public String getName() {
        return name;
    }

    public DimensionAwareEmbeddingModel getEmbedder() {
        return embedder;
    }

    public void setEmbedder(DimensionAwareEmbeddingModel embedder) {
        this.embedder = embedder;
    }

    public Map<String, Object> getParams() {
        return params;
    }
}
