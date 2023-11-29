
# ./gradlew dev

# following the https://github.com/apache/solr/blob/main/solr/example/films/README.md steps except for
# starting with `-cloud -noprompt` arguments to get cloud mode
# creating with `-s 2` argument to get two shards

solr/packaging/build/dev/bin/solr start -cloud -noprompt

solr/packaging/build/dev/bin/solr create -c films -s 2

curl --silent http://localhost:8983/solr/films/schema -X POST -H 'Content-type:application/json' --data-binary '{
  "add-field-type" : {
    "name":"knn_vector_10",
    "class":"solr.DenseVectorField",
    "vectorDimension":10,
    "similarityFunction":"cosine",
    "knnAlgorithm":"hnsw"
  },
  "add-field" : [
    {
      "name":"name",
      "type":"text_general",
      "multiValued":false,
      "stored":true
    },
    {
      "name":"initial_release_date",
      "type":"pdate",
      "stored":true
    },
    {
      "name":"film_vector",
      "type":"knn_vector_10",
      "indexed":true,
      "stored":true
    }
  ]
}'

solr/packaging/build/dev/bin/post -c films solr/packaging/build/dev/example/films/films.json

curl --silent "http://localhost:8983/solr/films/select?q=name:batman&rows=10"

curl --silent --data-urlencode 'expr=
  search(films,
         q="name:batman",
         fl="id,name,genre,film_vector",
         sort="id asc",
         rows="10")
' "http://localhost:8983/solr/films/stream"

echo
echo
echo "TODO: solr/packaging/build/dev/bin/solr stop -all"
echo
echo

