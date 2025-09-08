#!/bin/bash
set -e

SOLR_TARBALL="solr-10.0.0-SNAPSHOT.tgz"
SOLR_DIR="solr-10.0.0-SNAPSHOT"
COLLECTION_NAME="vectors"
CUVS_LIB_PATH="/home/ishan/code/cuvs/cpp/build"

echo "Testing Solr cuVS-Lucene module..."

# Check prerequisites
[ ! -f "$SOLR_TARBALL" ] && echo "ERROR: $SOLR_TARBALL not found" && exit 1
[ ! -d "$CUVS_LIB_PATH" ] && echo "ERROR: cuVS library path not found: $CUVS_LIB_PATH" && exit 1

# Extract and setup
echo "Extracting Solr..."
[ -d "$SOLR_DIR" ] && rm -rf "$SOLR_DIR"
tar -xzf "$SOLR_TARBALL"
cd "$SOLR_DIR"

# Configure
export LD_LIBRARY_PATH="$CUVS_LIB_PATH:$LD_LIBRARY_PATH"
echo "SOLR_MODULES=cuvs-lucene" >> bin/solr.in.sh

# Copy JARs for SPI loading
echo "Setting up cuVS module..."
cp modules/cuvs-lucene/lib/*.jar server/solr-webapp/webapp/WEB-INF/lib/

# Create configset
echo "Creating configset..."
mkdir -p /tmp/cuvs_configset/conf

cat > /tmp/cuvs_configset/conf/solrconfig.xml << 'EOF'
<?xml version="1.0" ?>
<config>
    <luceneMatchVersion>10.0.0</luceneMatchVersion>
    <dataDir>${solr.data.dir:}</dataDir>
    <directoryFactory name="DirectoryFactory" class="${solr.directoryFactory:solr.NRTCachingDirectoryFactory}"/>
    
    <updateHandler class="solr.DirectUpdateHandler2">
        <updateLog>
            <str name="dir">${solr.ulog.dir:}</str>
        </updateLog>
        <autoCommit>
            <maxTime>${solr.autoCommit.maxTime:15000}</maxTime>
            <openSearcher>false</openSearcher>
        </autoCommit>
        <autoSoftCommit>
            <maxTime>${solr.autoSoftCommit.maxTime:1000}</maxTime>
        </autoSoftCommit>
    </updateHandler>

    <codecFactory name="CuvsCodecFactory" class="org.apache.solr.cuvs_lucene.CuvsCodecFactory">
        <str name="cuvsWriterThreads">32</str>
        <str name="intGraphDegree">128</str>
        <str name="graphDegree">64</str>
        <str name="hnswLayers">1</str>
    </codecFactory>

    <requestHandler name="/select" class="solr.SearchHandler">
        <lst name="defaults">
            <str name="echoParams">explicit</str>
            <int name="rows">10</int>
        </lst>
    </requestHandler>
    
    <requestHandler name="/update" class="solr.UpdateRequestHandler" />
</config>
EOF

cat > /tmp/cuvs_configset/conf/managed-schema << 'EOF'
<?xml version="1.0" ?>
<schema name="schema-densevector" version="1.7">
    <fieldType name="string" class="solr.StrField" multiValued="true"/>
    <fieldType name="knn_vector" class="solr.DenseVectorField" 
               vectorDimension="8" 
               knnAlgorithm="cagra_hnsw" 
               similarityFunction="cosine" />
    <fieldType name="plong" class="solr.LongPointField" useDocValuesAsStored="false"/>

    <field name="id" type="string" indexed="true" stored="true" multiValued="false" required="false"/>
    <field name="article_vector" type="knn_vector" indexed="true" stored="true"/>
    <field name="_version_" type="plong" indexed="true" stored="true" multiValued="false" />
    
    <uniqueKey>id</uniqueKey>
</schema>
EOF

# Start Solr
echo "Starting Solr..."
./bin/solr start

# Check if running
curl -s "http://localhost:8983/solr/admin/info/system" > /dev/null || (echo "ERROR: Solr not responding" && exit 1)

# Upload configset and create collection
echo "Setting up collection..."
./bin/solr zk upconfig -n cuvs_vectors -d /tmp/cuvs_configset/conf
./bin/solr create -c "$COLLECTION_NAME" -n cuvs_vectors

# Index documents
echo "Indexing documents..."
curl -s -X POST "http://localhost:8983/solr/$COLLECTION_NAME/update?commit=true" \
     -H "Content-Type: application/json" \
     -d '[
       {"id": "doc1", "article_vector": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]},
       {"id": "doc2", "article_vector": [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]},
       {"id": "doc3", "article_vector": [0.2, 0.4, 0.6, 0.8, 0.1, 0.3, 0.5, 0.7]},
       {"id": "doc4", "article_vector": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]},
       {"id": "doc5", "article_vector": [0.9, 0.1, 0.8, 0.2, 0.7, 0.3, 0.6, 0.4]}
     ]'

# Test vector search
echo "Testing vector search..."
RESULT=$(curl -s "http://localhost:8983/solr/$COLLECTION_NAME/select?q=%7B!knn%20f=article_vector%20topK=3%7D%5B0.1,0.15,0.3,0.35,0.5,0.55,0.7,0.75%5D&fl=id,score&rows=3")
QTIME=$(echo "$RESULT" | grep -o '"QTime":[0-9]*' | cut -d: -f2)
NUM_FOUND=$(echo "$RESULT" | grep -o '"numFound":[0-9]*' | cut -d: -f2)

echo "Query time: ${QTIME}ms, Results: ${NUM_FOUND}"
echo "$RESULT" | python3 -m json.tool | grep -A1 -E '"id"|"score"'

# Check cuVS usage
if grep -q "Intermediate graph degree cannot be larger than dataset size" server/logs/solr-8983-console.log 2>/dev/null; then
    echo -e "\033[32mcuVS was definitely used (found cuVS log message)\033[0m"
fi

# Shutdown
echo "Shutting down Solr..."
./bin/solr stop --all
rm -rf /tmp/cuvs_configset

echo "Test completed successfully!"
