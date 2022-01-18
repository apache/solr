package org.apache.solr.schema;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.apache.solr.vector.search.SearchVector;
import org.apache.solr.vector.search.VectorQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VectorFieldType extends FieldType {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String vectorDimension;
  private static final String PARAM_VECTOR_DIMENSION = "vectorDimension";

  @Override
  public Type getUninversionType(SchemaField sf) {
    return Type.VECTOR_FIELD;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeArray(name, parse(f.getCharSequenceValue()));
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    //TODO throw exception
    return null;
  }
  
  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    this.vectorDimension = args.remove(PARAM_VECTOR_DIMENSION);
  }
  
  private List<Float> parse(CharSequence value)throws IOException {
    if(value.charAt(0) == '[' || value.charAt(0) == '{') {
      ObjectMapper mapper = new ObjectMapper();
      // 1. convert JSON array to Array objects
      List<Float> pp1 = mapper.readValue(value.toString(), new TypeReference<List<Float>>() {});
      return pp1;
    } else {
      String str = value.toString();
      String[] tokens = str.split(" ");
      List<Float> pp1 = new ArrayList<Float>();
      for(int i=0;i<tokens.length;i++) {
        pp1.add(Float.parseFloat(tokens[i]));
      }
      return pp1;
    }
  }
  
  public IndexableField createField(SchemaField field, Object value) {
    if (!field.indexed() && !field.stored()) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: {}", field);
      return null;
    }
      
      float[] vector = null;
      if(value instanceof SolrInputField) {
          SolrInputField inValue = (SolrInputField) value;
          if(inValue.getValue() instanceof ArrayList) {
            ArrayList<?> vList = (ArrayList<?>) inValue.getValue();
            if(vList != null && vList.size() > 0) {
              vector = new float[vList.size()];
              for(int i=0; i<vList.size(); ++i) {
                vector[i] = ((Double)vList.get(i)).floatValue();
              }
            } 
          }            
      }
      
      
      return new KnnVectorField(field.getName(), vector, KnnVectorField.createFieldType(vector.length, VectorSimilarityFunction.DOT_PRODUCT));      
  }

  /**
   * Returns a Query instance for doing searches against a field.
   * @param parser The {@link org.apache.solr.search.QParser} calling the method
   * @param field The {@link org.apache.solr.schema.SchemaField} of the field to search
   * @param externalVal The String representation of the value to search
   * @return The {@link org.apache.lucene.search.Query} instance.  This implementation returns a {@link org.apache.solr.vector.search.VectorQuery} but overriding queries may not
   */
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {

    if (field.hasDocValues() && !field.indexed()) {
      // match-only
      return getRangeQuery(parser, field, externalVal, externalVal, true, true);
    } else {
      float vector[] = null;
      String arrOfStr[] = externalVal.split(",");
      vector = new float[arrOfStr.length];
      for (int i = 0; i < arrOfStr.length; i++) { 
            vector[i] = Float.parseFloat(arrOfStr[i]);
      }
      return new VectorQuery(new SearchVector(field.getName(), vector));
    }
  }

}
