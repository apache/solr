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

We present in this directory the Python scripts that were used to create the `film_vector` field for the films dataset.

 - [films.py](./films.py): define some general purpose functions to read, save and process the films dataset.
 - [create_model.py](./create_model.py): creates an embedding model to represent the films.
 - [create_dataset.py](./create_dataset.py): uses the embedding model to calculate the vectors of the films and create the new dataset with the extra `film_vector` field.

To replicate the example you have to run the `create_model.py` script first, followed by `create_dataset.py`. We will describe and discuss each of these scripts below.

## Creating the Model (`create_model.py`)

There are several approaches that one could use to create vectors (embeddings) to represent documents. In the case of our example we decided to use a _textual_ approach, where we use the text of the document as input for calculating its vector.

To create the "sentence" that will serve as textual input for the movies we get its title followed by the genres separated in comma. For example, the "8 Mile" movie will have this sentence:
```
8 Mile

Musical, Hip hop film, Drama, Musical Drama
```

We use a pretrained model from [SentenceTransformers](https://www.sbert.net/) framework (`all-mpnet-base-v2`) as base for creating a new tailored reduced model. We calculate the 768-dimensions vectors for the sentences of all the movies in the dataset, then run a PCA to extract the 10 most important dimensions. With the PCA result we create a new model that will create vectors of size 10. The number of dimensions is a compromise between performance and quality, and we choose 10 here just to serve as a small and compact example. Generally the higher the number of dimensions, the higher the quality, while also increasing the memory consumption and the computational time to manipulate the vectors.

This model is created to serve as a small example to demonstrate the vectors features of Solr, so it is just one among many possible ways to create vectors for documents. For example, it is possible to _fine-tune_ a pre-trained model using textual data from our context. Another possibility is to train a model that does not even rely on text, but uses coocurrence of documents or items, like item2vec.

## Calculating Vectors (`create_dataset.py`)

Once we have the model created and stored we can use it to calculate the vectors of the documents.

First we load the model (reading it from disk to RAM). Then we read the films dataset and creates the sentences (as previously described in the previous section). Finally, for each sentence we use the model to calculate and encode the film vector according to its "sentence". After having the `film_vector` field added to the dataset, we export and store it in the 3 formats (JSON, XML and CSV).

So, if we have new movies to be indexed in the collection we have just to replicate the above steps: (1) load the model, (2) create the film sentence, (3) calculate the film vector from its sentence.
