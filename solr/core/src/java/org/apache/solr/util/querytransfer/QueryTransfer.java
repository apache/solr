package org.apache.solr.util.querytransfer;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryTransfer {
    static ObjectMapper mapper;
    static {
        SmileFactory smileFactory = new SmileFactory();
        mapper = new ObjectMapper(smileFactory);
        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(BoostQuery.class, new StdSerializer<>(BoostQuery.class) {

            @Override
            public void serialize(BoostQuery value, JsonGenerator gen, SerializerProvider provider) throws IOException {
                gen.writeStartArray(value, 3);
                gen.writeString("boost1");
                gen.writeNumber(value.getBoost());
                gen.writeObject(value.getQuery());
                gen.writeEndArray();
            }
        });

        simpleModule.addDeserializer(Query.class, new JsonDeserializer<>() {
            @Override
            public Query deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                final ObjectCodec codec = p.getCodec();
                final List<?> arr = codec.readValue(p, List.class);
                return interpret(p, arr);
            }
            @SuppressWarnings("unchecked")
            private Query interpret(JsonParser p, List<?> arr) throws JacksonException {
                final String format = (String) arr.get(0);
                switch (format) {
                    case "term1":
                        final String field = (String) arr.get(1);
                        Object txt = arr.get(2);
                        byte[] decode = txt instanceof String ? Base64.getDecoder().decode((String)txt) : (byte[])txt;
                        Term t = new Term(field, new BytesRef(decode));
                        return new TermQuery(t);
                    case "boost1":
                        final Number boost = (Number) arr.get(1);
                        final Query query = interpret(p, (List<?>) arr.get(2));
                        return new BoostQuery(query, boost.floatValue());
                    case "bool1":
                        final Map<String, ?> obj = (Map<String, ?>) arr.get(1);
                        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
                        for(Map.Entry<String ,?> prop: obj.entrySet()) {
                            if ("mm".equals(prop.getKey())) {
                                builder.setMinimumNumberShouldMatch(((Number) prop.getValue()).intValue());
                            } else {
                                final BooleanClause.Occur occur = BooleanClause.Occur.valueOf(prop.getKey());
                                for(Object q : (List<?>) prop.getValue()){
                                    builder.add(interpret(p, (List<?>) q), occur);
                                }
                            }
                        }
                        return builder.build();
                }
                throw new InvalidFormatException(p, "Unable to interpret", arr, Query.class);
            }
        });
        simpleModule.addSerializer(TermQuery.class, new StdSerializer<>(TermQuery.class) {
            @Override
            public void serialize(TermQuery value, JsonGenerator gen, SerializerProvider provider) throws IOException {
                gen.writeStartArray(value, 3);
                gen.writeString("term1");
                gen.writeString(value.getTerm().field());
                final BytesRef bytes = value.getTerm().bytes();
                gen.writeBinary(bytes.bytes, bytes.offset, bytes.length);
                gen.writeEndArray();
            }
        });
        simpleModule.addSerializer(BooleanQuery.class, new StdSerializer<>(BooleanQuery.class) {
            @Override
            public void serialize(BooleanQuery value, JsonGenerator gen, SerializerProvider provider) throws IOException {
                gen.writeStartArray(value, 2);
                gen.writeString("bool1");

                final Map<BooleanClause.Occur, List<Query>> byOccur = value.clauses().stream()
                        .collect(Collectors.groupingBy(BooleanClause::getOccur,
                                Collectors.mapping(BooleanClause::getQuery, Collectors.toList())));
                gen.writeStartObject(value, byOccur.size() + (value.getMinimumNumberShouldMatch()==0?0:1));//cnt
                for (Map.Entry<BooleanClause.Occur, List<Query>> occur : byOccur.entrySet()) {
                    gen.writeArrayFieldStart(occur.getKey().name()); //TODO scalar
                    for (Query q : occur.getValue()) {
                        gen.writeObject(q);
                    }
                    gen.writeEndArray();
                }
                if (value.getMinimumNumberShouldMatch()>0) {
                    gen.writeNumberField("mm", value.getMinimumNumberShouldMatch());
                }
                gen.writeEndObject();
                gen.writeEndArray();
            }
        });
        mapper.registerModule(simpleModule);
    }

    private QueryTransfer () {
    }

    public static byte[] transfer(Query query) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        mapper.writeValue(os, query);
        return os.toByteArray();
    }

    public static Query receive(byte[] unmarshal) throws IOException {
        return mapper.readValue(unmarshal, Query.class);
    }

}
