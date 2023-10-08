We have a movie data set in JSON, Solr XML, and CSV formats.  All 3 formats contain the same data.  You can use any one format to index documents to Solr.

This example uses the `_default` configset that ships with Solr plus some custom fields added via Schema API.  It demonstrates the use of ParamSets in conjunction with the [Request Parameters API](https://solr.apache.org/guide/solr/latest/configuration-guide/request-parameters-api.html).

The original data was fetched from Freebase and the data license is present in the films-LICENSE.txt file.  Freebase was shutdown in 2016 by Google.

This data consists of the following fields:
 * `id` - unique identifier for the movie
 * `name` - Name of the movie
 * `directed_by` - The person(s) who directed the making of the film
 * `initial_release_date` - The earliest official initial film screening date in any country
 * `genre` - The genre(s) that the movie belongs to
 * `film_vector` - The 10 dimensional vector representing the film, according to a toy example embedding model

 The `name` and `initial_release_date` are created via the Schema API, and the `genre` and `direct_by` fields
 are created by the use of an Update Request Processor Chain called `add-unknown-fields-to-the-schema`.

 The `film_vector` is an embedding vector created to represent the movie with 10 dimensions. The vector is created from a BERT pre-trained model, followed by a dimension reduction technique to reduce the embeddings from 768 to 10 dimensions. Even though it is expected that similar movies will be close to each other, this model is just a "toy example", so it's not guaranteed to be a good representation for the movies. The Python scripts utilized to create the model and calculate the films vectors are in the [vectors directory](./vectors).

 The below steps walk you through learning how to start up Solr, setup the films collection yourself, and then load data.  We'll then create ParamSets to organize our query parameters. Finally, we also show some example of KNN queries exploring the Dense Vectors feature.

 You can also run `bin/solr start -example films` or `bin/solr start -c -example films` for SolrCloud version which does all the below steps for you.

 Steps:
   * Start Solr:
     ```
     bin/solr start
     ```

   * Create a "films" core:

     ```
     bin/solr create -c films
     ```

   * Set the schema on a couple of fields that Solr would otherwise guess differently (than we'd like) about:

      ```
      curl http://localhost:8983/solr/films/schema -X POST -H 'Content-type:application/json' --data-binary '{
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
      ```

   * Now let's index the data, using one of these three commands:

     - JSON: `bin/post -c films example/films/films.json`
     - XML: `bin/post -c films example/films/films.xml`
     - CSV:
     ```
         bin/post \
                  -c films \
                  example/films/films.csv \
                  -params "f.genre.split=true&f.directed_by.split=true&f.film_vector.split=true&f.genre.separator=|&f.directed_by.separator=|&f.film_vector.separator=|"
     ```


   * Let's get searching!
     - Search for 'Batman':

       http://localhost:8983/solr/films/query?q=name:batman

       * If you get an error about the name field not existing, you haven't yet indexed the data
       * If you don't get an error, but zero results, chances are that the _name_ field schema type override wasn't set
         before indexing the data the first time (it ended up as a "string" type, requiring exact matching by case even).
         It's easiest to simply reset the environment and try again, ensuring that each step successfully executes.

     - Show me all 'Super hero' movies:

       http://localhost:8983/solr/films/query?q=*:*&fq=genre:%22Superhero%20movie%22

     - Let's see the distribution of genres across all the movies. See the facet section of the response for the counts:

       http://localhost:8983/solr/films/query?q=*:*&facet=true&facet.field=genre

   * Time for relevancy tuning with ParamSets :

     - Search for 'harry potter':

       http://localhost:8983/solr/films/query?q=name:harry%20potter

       * Notice the very first result is the move _Dumb &amp; Dumberer: When Harry Met Lloyd_?
       That is clearly not what we are looking for.  

     - Let's set up two relevancy algorithms, and then compare the quality of the results.
         Algorithm *A* will use the `dismax` and a `qf` parameters, and then Algorithm *B*
         will use `dismax`, `qf` and a must match `mm` of 100%.

         ```
         curl http://localhost:8983/solr/films/config/params -X POST -H 'Content-type:application/json' --data-binary '{
           "set": {
              "algo_a":{
                "defType":"dismax",
                "qf":"name"
              }
            },
            "set": {
              "algo_b":{
                "defType":"dismax",
                "qf":"name",
                "mm":"100%"
              }
             }            
         }'
         ```

     - Search for 'harry potter' with Algorithm *A*:

       http://localhost:8983/solr/films/query?q=harry%20potter&useParams=algo_a

       * Now we are returning the five results, including the Harry Potter movies, however notice that we still have the
         _Dumb &amp; Dumberer: When Harry Met Lloyd_ movie coming back?   

     - Search for 'harry potter' with Algorithm *B*:

       http://localhost:8983/solr/films/query?q=harry%20potter&useParams=algo_b

       * We are now returning only the four Harry Potter movies, leading to more precise results!
           We can say that we believe Algorithm *B* is better then Algorithm *A*.  You can extend
           this to online A/B testing very easily to confirm with real users.

   * Now let's search with Dense Vectors and KNN!

     - Before making the queries, we define an example target vector, simulating a person that 
       watched 3 movies: _Finding Nemo_, _Bee Movie_, and _Harry Potter and the Chamber of Secrets_. 
       We get the 3 vectors of each movie, then calculate the resulting average vector, which will 
       be used as input vector for all the following example queries.

     ```
     [-0.1784, 0.0096, -0.1455, 0.4167, -0.1148, -0.0053, -0.0651, -0.0415, 0.0859, -0.1789]
     ```
     - Search for the top 10 movies most similar to the target vector (KNN Query for recommendation):

       http://localhost:8983/solr/films/query?q={!knn%20f=film_vector%20topK=10}[-0.1784,0.0096,-0.1455,0.4167,-0.1148,-0.0053,-0.0651,-0.0415,0.0859,-0.1789]

       * Notice that among the results, there are some animation family movies, such as _Curious George_ and _Bambi_, which makes sense, since the target vector was created with two other animation family movies (_Finding Nemo_ and _Bee Movie_).
       * We also notice that among the results there are two movies that the person already watched. In the next example we will filter then out.

     - Search for the top 10 movies most similar to the resulting vector, excluding the movies already watched (KNN query with Filter Query):

       http://localhost:8983/solr/films/query?q={!knn%20f=film_vector%20topK=10}[-0.1784,0.0096,-0.1455,0.4167,-0.1148,-0.0053,-0.0651,-0.0415,0.0859,-0.1789]&fq=-id:("%2Fen%2Ffinding_nemo"%20"%2Fen%2Fbee_movie"%20"%2Fen%2Fharry_potter_and_the_chamber_of_secrets_2002")

     - Search for movies with "cinderella" in the name among the top 50 movies most similar to the target vector (KNN as Filter Query):

       http://localhost:8983/solr/films/query?q=name:cinderella&fq={!knn%20f=film_vector%20topK=50}[-0.1784,0.0096,-0.1455,0.4167,-0.1148,-0.0053,-0.0651,-0.0415,0.0859,-0.1789]

       * There are 3 "cinderella" movies in the index, but only 1 is among the top 50 most similar to the target vector (_Cinderella III: A Twist in Time_).

     - Search for movies with "animation" in the genre, and rerank the top 5 documents by combining (sum) the original query score with twice (2x) the similarity to the target vector (KNN with ReRanking):

       http://localhost:8983/solr/films/query?q=genre:animation&rqq={!knn%20f=film_vector%20topK=10000}[-0.1784,0.0096,-0.1455,0.4167,-0.1148,-0.0053,-0.0651,-0.0415,0.0859,-0.1789]&rq={!rerank%20reRankQuery=$rqq%20reRankDocs=5%20reRankWeight=2}

       * To guarantee we calculate the vector similarity score for all the movies, we set `topK=10000`, a number higher than the total number of documents (`1100`).

   * It's possible to combine the vector similarity scores with other scores, by using Sub-query, 
     Function Queries and Parameter Dereferencing Solr features:

     - Search for "harry potter" movies, ranking the results by the similarity to the target vector instead of the lexical query score. Beside the `q` parameter, we define a "sub-query" named `q_vector`, that will calculate the similarity score between all the movies (since we set `topK=10000`). Then we use the sub-query parameter name as input for the `sort`, specifying that we want to rank descending according to the vector similarity score (`sort=$q_vector desc`):

       http://localhost:8983/solr/films/query?q=name:"harry%20potter"&q_vector={!knn%20f=film_vector%20topK=10000}[-0.1784,0.0096,-0.1455,0.4167,-0.1148,-0.0053,-0.0651,-0.0415,0.0859,-0.1789]&sort=$q_vector%20desc

     - Search for movies with "the" in the name, keeping the original lexical query ranking, but returning only movies with similarity to the target vector of 0.8 or higher. Like previously, we define the sub-query `q_vector`, but this time we use it as input for the `frange` filter, specifying that we want documents with at least 0.8 of vector similarity score:

       http://localhost:8983/solr/films/query?q=name:the&q_vector={!knn%20f=film_vector%20topK=10000}[-0.1784,0.0096,-0.1455,0.4167,-0.1148,-0.0053,-0.0651,-0.0415,0.0859,-0.1789]&fq={!frange%20l=0.8}$q_vector

     - Search for "batman" movies, ranking the results by combining 70% of the original lexical query score and 30% of the similarity to the target vector. Besides the `q` main query and the `q_vector` sub-query, we also specify the `q_lexical` query, which will hold the lexical score of the main `q` query. Then we specify a parameter variable called `score_combined`, which scales the lexical and similarity scores, applies the 0.7 and 0.3 weights, then sum the result. We set the `sort` parameter to order according the combined score, and also set the `fl` parameter so that we can view the intermediary and the combined score values in the response:

       http://localhost:8983/solr/films/query?q=name:batman&q_lexical={!edismax%20v=$q}&q_vector={!knn%20f=film_vector%20topK=10000}[-0.1784,0.0096,-0.1455,0.4167,-0.1148,-0.0053,-0.0651,-0.0415,0.0859,-0.1789]&score_combined=sum(mul(scale($q_lexical,0,1),0.7),mul(scale($q_vector,0,1),0.3))&sort=$score_combined%20desc&fl=name,score,$q_lexical,$q_vector,$score_combined


FAQ:
  Why override the schema of the _name_ and _initial_release_date_ fields?

     Without overriding those field types, the _name_ field would have been guessed as a multi-valued string field type
     and _initial_release_date_ would have been guessed as a multi-valued `pdate` type.  It makes more sense with this
     particular data set domain to have the movie name be a single valued general full-text searchable field,
     and for the release date also to be single valued.
