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
