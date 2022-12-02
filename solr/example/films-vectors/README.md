This example demonstrates how to create a collection, index, and query data with Solr's Dense Vector feature. 
 
We use the same dataset from the `films` example, enriched by adding a new field `film_vector` for the movies.

The `film_vector` is an embedding vector created to represent the movie with 10 dimensions. The vector is created by combining the first 5 dimensions of a pre-trained BERT sentence model applied on the name of the movies plus the name of the genres, followed by an item2vec 5-dimensions model of itemset co-ocurrence of genres in the movies, totalling 10 dimensions. This model is a "toy example", created only to serve as an example for the Dense Vector feature, so it's not guaranteed to be a good representation for the movies. 

The following steps shows you how to start Solr, setup the collection with the Dense Vector field, and then load data. Then we also show some basic queries with the KNN feature, followed by other more advanced queries combining scores.

 Steps:
   * Start Solr:
     ```
     bin/solr start
     ```

   * Create a "films-vectors" core:

     ```
     bin/solr create -c films-vectors
     ```

   * Specify the field type that will store the films vectors:

      ```
      curl --request POST \
        --url http://localhost:8983/solr/films-vectors/schema \
        --header 'Content-type:application/json' \
        --data-binary '{
          "add-field-type" : {
            "name":"knn_vector_10",
            "class":"solr.DenseVectorField",
            "vectorDimension": 10,
            "similarityFunction":"cosine",
            "knnAlgorithm":"hnsw"
          }
        }'
      ```

   * Set the schema on the same fields as the `films` example, but adding the extra `film_vector` field:

      ```
      curl --request POST \
        --url http://localhost:8983/api/collections/films-vectors/schema \
        --header 'Content-Type:application/json' \
        --data '{
          "add-field": [
            {"name": "name", "type": "text_general", "multiValued": false, "stored":true},
            {"name": "initial_release_date", "type": "pdate", "stored":true},
            {"name": "film_vector", "type": "knn_vector_10", "indexed":true, "stored":true}
          ]
        }'
      ```

   * Now let's index the data from the JSON dataset

     ```
     bin/post -c films-vectors example/films-vectors/films-vectors.json
     ```

   * Before making the queries, we define an example target vector, simulating a person that watched 3 movies: _Finding Nemo_, _Bee Movie_, and _Harry Potter and the Chamber of Secrets_. We get the 3 vectors of each movie, ten calculate the resulting average vector, which will be used as input vector for all the following example queries.

     ```
     [0.0133, 0.0010, 0.0107, -0.0081, 0.0360, 1.0028 ,0.6504, 1.3453, -1.3509, -0.9636]
     ```

   * Now let's search with vectors!
     - Search for the top 10 movies most similar to the target vector (KNN Query for recommendation):

       http://localhost:8983/solr/films-vectors/query?q={!knn%20f=film_vector%20topK=10}[0.0133,0.0010,0.0107,-0.0081,0.0360,1.0028,0.6504,1.3453,-1.3509,-0.9636]

       * Notice that among the results, there are some animation family movies, such as _Curious George_ and _Finding Nemo_, which makes sense, since the target vector was created with two other animation family movies (_Finding Nemo_ and _Bee Movie_).
       * We also notice that among the results there are two movies that the person already watched. In the next example we will filter then out.

     - Search for the top 10 movies most similar to the resulting vector, excluding the movies already watched (KNN query with Filter Query):

       http://localhost:8983/solr/films-vectors/query?q={!knn%20f=film_vector%20topK=10}[0.0133,0.0010,0.0107,-0.0081,0.0360,1.0028,0.6504,1.3453,-1.3509,-0.9636]&fq=-id:("%2Fen%2Ffinding_nemo"%20"%2Fen%2Fbee_movie"%20"%2Fen%2Fharry_potter_and_the_chamber_of_secrets_2002")

     - Search for movies with "cinderella" in the name among the top 50 movies most similar to the target vector (KNN with Filter Query):

       http://localhost:8983/solr/films-vectors/query?q=name:cinderella&fq={!knn%20f=film_vector%20topK=50}[0.0133,0.0010,0.0107,-0.0081,0.0360,1.0028,0.6504,1.3453,-1.3509,-0.9636]

       * There are 3 "cinderella" movies in the index, but only 1 is among the top 50 most similar to the target vector (_Cinderella III: A Twist in Time_).

     - Search for movies with "animation" in the genre, and rerank the top 5 documents by combining (sum) the original query score with twice (2x) the similarity to the target vector (KNN with ReRanking):

       http://localhost:8983/solr/films-vectors/query?q=genre:animation&rqq={!knn%20f=film_vector%20topK=10000}[0.0133,0.0010,0.0107,-0.0081,0.0360,1.0028,0.6504,1.3453,-1.3509,-0.9636]&rq={!rerank%20reRankQuery=$rqq%20reRankDocs=5%20reRankWeight=2}

       * To guarantee we calculate the vector similarity score for all the movies, we set `topK=10000`, a number higher than the total number of documents (`1100`).

   * It's possible to combine the vector similarity scores with other scores, by using Sub-query, Function Queries and Parameter Dereferencing Solr features:

     - Search for "harry potter" movies, ranking the results by the similarity to the target vector instead of by the lexical query score. Beside the `q` parameter, we define a "sub-query" named `q_vector`, that will calculate the similarity score between all the movies (since we set `topK=10000`). Then we use the sub-query parameter name as input for the `sort`, specifying that we want to rank descending according to the vector similarity score (`sort=$q_vector desc`):

       http://localhost:8983/solr/films-vectors/query?q=name:"harry%20potter"&q_vector={!knn%20f=film_vector%20topK=10000}[0.0133,0.0010,0.0107,-0.0081,0.0360,1.0028,0.6504,1.3453,-1.3509,-0.9636]&sort=$q_vector%20desc

     - Search for movies with "the" in the name, keeping the original lexical query ranking, but keeping only movies with similarity to the target vector of 0.8 or higher. Like previously, we define the sub-query `q_vector`, but this time we use it as input for the `frange` filter, specifying that we want documents with at least 0.8 of vector similarity score:

       http://localhost:8983/solr/films-vectors/query?q=name:the&q_vector={!knn%20f=film_vector%20topK=10000}[0.0133,0.0010,0.0107,-0.0081,0.0360,1.0028,0.6504,1.3453,-1.3509,-0.9636]&fq={!frange%20l=0.8}$q_vector

     - Search for "batman" movies, ranking the results by combining 70% of the original lexical query score and 30% of the similarity to the target vector. Besides the `q` main query and the `q_vector` sub-query, we also specify the `q_lexical` query, which will hold the lexical score of the main `q` query. Then we specify a parameter variable called `score_combined`, which scales the lexical and similarity scores, applies the 0.7 and 0.3 weights, then sum the result. We set the `sort` parameter to order according the combined score, and also set the `fl` parameter so that we can view the intermediary and the combined score values in the response:

       http://localhost:8983/solr/films-vectors/query?q=name:batman&q_lexical={!edismax%20v=$q}&q_vector={!knn%20f=film_vector%20topK=10000}[0.0133,0.0010,0.0107,-0.0081,0.0360,1.0028,0.6504,1.3453,-1.3509,-0.9636]&score_combined=sum(mul(scale($q_lexical,0,1),0.7),mul(scale($q_vector,0,1),0.3))&sort=$score_combined%20desc&fl=name,score,$q_lexical,$q_vector,$score_combined

     - Query for movies with "love" in the name, ranking the results by combining 10% of the original lexical query score, and 30% for the similarity of each of the 3 vectors of the watched movies (_Finding Nemo_, _Bee Movie_, and _Harry Potter and the Chamber of Secrets_). In this example instead of using a single sub-query with the combined target vector, we use each one of the 3 vectors of the watched movies separately, creating 3 KNN sub-queries (`similarity_beemovie`, `similarity_nemo`, `similarity_harrypotter`). We then define the combined score and the other parameters (`sort`, `fl`) similarly like the previous example:
  
       http://localhost:8983/solr/films-vectors/query?q=name:love&q_lexical={!edismax%20v=$q}&similarity_beemovie={!knn%20f=film_vector%20topK=10000}[0.0209,-0.0407,0.0250,-0.0424,0.0406,0.9472,0.8459,1.4537,-1.2848,-0.9490]&similarity_nemo={!knn%20f=film_vector%20topK=10000}[0.0181,0.0282,0.0014,-0.0231,0.0817,0.9472,0.8459,1.4537,-1.2848,-0.9490]&similarity_harrypotter={!knn%20f=film_vector%20topK=10000}[0.0009,0.0155,0.0058,0.0413,-0.0144,1.1141,0.2593,1.1286,-1.4831,-0.9927]&score_combined=sum(mul(scale($q_lexical,0,1),0.1),mul(scale($similarity_beemovie,0,1),0.3),mul(scale($similarity_nemo,0,1),0.3),mul(scale($similarity_harrypotter,0,1),0.3))&sort=$score_combined%20desc&fl=name,score,$q_lexical,$similarity_beemovie,$similarity_nemo,$similarity_harrypotter,$score_combined
