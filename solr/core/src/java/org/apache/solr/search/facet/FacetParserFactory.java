package org.apache.solr.search.facet;

import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SyntaxError;

import java.util.Map;

public class FacetParserFactory implements OneFacetParser {

    public FacetRequest parseRequest(SolrQueryRequest req, Map<String, Object> jsonFacet) {
        FacetParser<?> parser = createTopParser(req);
        try {
          return parser.parse(jsonFacet);
        } catch (SyntaxError syntaxError) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
        }
    }

    private FacetParser.FacetTopParser createTopParser(SolrQueryRequest req) {
        return new FacetParser.FacetTopParser(req) {
            @Override
            public Object parseFacetOrStat(String key, String type, Object args) throws SyntaxError {
                return FacetParserFactory.this.parseFacetOrStat(this, key, type, args);
            }
        };
    }

    @Override
    public FacetRequest parseOneFacetReq(SolrQueryRequest req, Map<String, Object> params) {
        @SuppressWarnings("rawtypes")
        FacetParser parser = createTopParser(req);
        try {
            return (FacetRequest) parser.parseFacetOrStat("", params);
        } catch (SyntaxError syntaxError) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
        }
    }

    protected Object parseFacetOrStat(FacetParser.FacetTopParser facetTopParser, String key, String type, Object args) throws SyntaxError {
        // TODO: a place to register all these facet types?
        switch (type) {
            case "field":
            case "terms":
                FacetParser.FacetFieldParser facetFieldParser = new FacetParser.FacetFieldParser(facetTopParser, key){
                    public Object parseFacetOrStat(String key, String type, Object args) throws SyntaxError {
                        return FacetParserFactory.this.parseFacetOrStat(facetTopParser, key, type, args);
                    }
                };
                return facetFieldParser.parse(args);
            case "query":
                FacetParser.FacetQueryParser facetQueryParser = new FacetParser.FacetQueryParser(facetTopParser, key) {
                    public Object parseFacetOrStat(String key, String type, Object args) throws SyntaxError {
                        return FacetParserFactory.this.parseFacetOrStat(facetTopParser, key, type, args);
                    }
                };
                return facetQueryParser.parse(args);
            case "range":
                FacetRangeParser facetRangeParser = new FacetRangeParser(facetTopParser, key){
                    @Override
                    public Object parseFacetOrStat(String key, String type, Object args) throws SyntaxError {
                        return FacetParserFactory.this.parseFacetOrStat(facetTopParser, key, type, args);
                    }
                };
                return facetRangeParser.parse(args);
            case "heatmap":
                FacetHeatmap.Parser heatmapParser = new FacetHeatmap.Parser(facetTopParser, key) {
                    @Override
                    public Object parseFacetOrStat(String key, String type, Object args) throws SyntaxError {
                        return FacetParserFactory.this.parseFacetOrStat(facetTopParser, key, type, args);
                    }
                };
                return heatmapParser.parse(args);
            case "func":
                return facetTopParser.parseStat(key, args);
        }
        throw facetTopParser.err("Unknown facet or stat. key=" + key + " type=" + type + " args=" + args);
    }
}
