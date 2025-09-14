<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Welcome to Apache Solr's cuVS module!
========

> [!CAUTION]
> This feature is currently experimental.

# Introduction

Building HNSW graphs, especially with high dimensions and cardinality, is usually slow. If you have NVIDIA GPUs then building HNSW based indexes can be sped up manifold. This is powered by the [cuVS-Lucene](https://github.com/rapidsai/cuvs-lucene) library, a pluggable vectors format for Apache Lucene. It uses the state of the art [CAGRA algorithm](https://arxiv.org/abs/2308.15136) for quickly building a fixed degree connected graph, which is then serialized into a HNSW graph. [CUDA 13.0+](https://developer.nvidia.com/cuda-downloads) and [JDK 22](https://jdk.java.net/archive/) are required to use this feature.


To try this out, first copy the module jar files (found in the regular Solr tarball, not the slim one) before starting Solr.

```sh
cp modules/cuvs/lib/*.jar server/solr-webapp/webapp/WEB-INF/lib/
```

Define the `fieldType` in the schema, with knnAlgorithm set to `cagra_hnsw`:

```xml
<fieldType name="knn_vector" class="solr.DenseVectorField" vectorDimension="8" knnAlgorithm="cagra_hnsw" similarityFunction="cosine" />
```

Define the `codecFactory` in `solrconfig-xml`

```xml
<codecFactory name="CuvsCodecFactory" class="org.apache.solr.cuvs.CuvsCodecFactory">
    <str name="cuvsWriterThreads">8</str>
    <str name="intGraphDegree">128</str>
    <str name="graphDegree">64</str>
    <str name="hnswLayers">1</str>
</codecFactory>
```

Where:

* `cuvsWriterThreads` - number of threads to use

* `intGraphDegree` - Intermediate graph degree for building the CAGRA index

* `graphDegree` - Graph degree for building the CAGRA index

* `hnswLayers` - Number of HNSW graph layers to construct while building the HNSW index

# Example

Following is a complete example of setting up a collection with cuVS.

1. Install CUDA 13.0 and cuVS native library

   ```sh
     #TODO: Add the script.
   ```

2. Copy the `cuvs` module jar files (before starting Solr).

   ```sh
    cp modules/cuvs/lib/*.jar server/solr-webapp/webapp/WEB-INF/lib/
   ```

3. Create a configset

   ```sh
    mkdir -p cuvs_configset/conf
   ```
   ```sh
    cat > cuvs_configset/conf/solrconfig.xml << 'EOF'
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

        <codecFactory name="CuvsCodecFactory" class="org.apache.solr.cuvs.CuvsCodecFactory">
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
   ```

   ```sh
    cat > cuvs_configset/conf/managed-schema << 'EOF'
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
   ```

4. Start Solr
   ```sh
    ./bin/solr start
   ```

5. Upload the configset and create a collection

   ```sh
    ./bin/solr zk upconfig -n cuvs_vectors -d cuvs_configset/conf && ./bin/solr create -c vectors -n cuvs_vectors
   ```

6. Index documents

   ```sh
    curl -s -X POST "http://localhost:8983/solr/vectors/update?commit=true" \
        -H "Content-Type: application/json" \
        -d '[
        {"id": "doc1", "article_vector": [0.35648, 0.11664, 0.85660, 0.25043, 0.80778, 0.08031, 0.48444, 0.39083]},
        {"id": "doc2", "article_vector": [0.86821, 0.24947, 0.38601, 0.22615, 0.31498, 0.74612, 0.69403, 0.19691]},
        {"id": "doc3", "article_vector": [0.34098, 0.49236, 0.35950, 0.17840, 0.49470, 0.97242, 0.28249, 0.72526]},
        {"id": "doc4", "article_vector": [0.44979, 0.49473, 0.47197, 0.02869, 0.05262, 0.60855, 0.67370, 0.78656]},
        {"id": "doc5", "article_vector": [0.23235, 0.70062, 0.95036, 0.36251, 0.41233, 0.53170, 0.25459, 0.81606]}
        ]'
   ```

7. Query the index

   ```sh
    curl -s 'http://localhost:8983/solr/vectors/select?q=%7B!knn%20f=article_vector%20topK=1%7D%5B0.84393,0.50073,0.57059,0.89899,-0.08722,0.26803,0.00807,0.09877%5D&fl=id,score&rows=3&omitHeader=true'
   ```

   Should return the following

   ```json
    {
    "response":{
        "numFound":1,
        "start":0,
        "maxScore":0.8377289,
        "numFoundExact":true,
        "docs":[{
        "id":"doc2",
        "score":0.8377289
        }]
    }
    }
   ```