We have a movie data set in JSON, Solr XML, and CSV formats.  All 3 formats contain the same data.  You can use any one format to index documents to Solr.

This example uses the `_default` configset that ships with Solr plus some custom fields added via Schema API.  It demonstrates the use of ParamSets in conjunction with the [Request Parameters API](https://solr.apache.org/guide/solr/latest/configuration-guide/request-parameters-api.html).

The original data was fetched from Freebase and the data license is present in the films-LICENSE.txt file.  Freebase was shutdown in 2016 by Google.

This data consists of the following fields:
 * `id` - unique identifier for the movie
 * `name` - Name of the movie
 * `directed_by` - The person(s) who directed the making of the film
 * `initial_release_date` - The earliest official initial film screening date in any country
 * `genre` - The genre(s) that the movie belongs to

 The `name` and `initial_release_date` are created via the Schema API, and the `genre` and `direct_by` fields
 are created by the use of an Update Request Processor Chain called `add-unknown-fields-to-the-schema`.

 The below steps walk you through learning how to start up Solr, setup the films collection yourself, and then load data.  We'll then create ParamSets to organize our query parameters.

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
        "add-field" : {
          "name":"name",
          "type":"text_general",
          "multiValued":false,
          "stored":true
        },
        "add-field" : {
          "name":"initial_release_date",
          "type":"pdate",
          "stored":true
        }
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
                  -params "f.genre.split=true&f.directed_by.split=true&f.genre.separator=|&f.directed_by.separator=|"
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





FAQ:
  Why override the schema of the _name_ and _initial_release_date_ fields?

     Without overriding those field types, the _name_ field would have been guessed as a multi-valued string field type
     and _initial_release_date_ would have been guessed as a multi-valued `pdate` type.  It makes more sense with this
     particular data set domain to have the movie name be a single valued general full-text searchable field,
     and for the release date also to be single valued.
