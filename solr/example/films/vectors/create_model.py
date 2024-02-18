#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# In this example, we reduce the dimensionality of the embeddings of
# the SBERT pre-trained model 'all-mpnet-base-v2' from 768 to 10 dimensions. 
#
# The code is derived from the SBERT documentation and corresponding example code:
#  - https://www.sbert.net/examples/training/distillation/README.html
#  - https://github.com/UKPLab/sentence-transformers/tree/master/examples/training/distillation/dimensionality_reduction.py

from sklearn.decomposition import PCA
from sentence_transformers import SentenceTransformer, LoggingHandler, util, evaluation, models, InputExample
import logging
import os
import pathlib
import gzip
import csv
import random
import numpy as np
import torch

import films

#### Just some code to print debug information to stdout
logging.basicConfig(format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO, handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)

#### Create folders structure
pathlib.Path("./data/").mkdir(parents=True, exist_ok=True)
pathlib.Path("./models/").mkdir(parents=True, exist_ok=True)


######## Load full model ########

# Model for which we apply dimensionality reduction
model = SentenceTransformer("all-mpnet-base-v2")

# New size for the embeddings
new_dimension = 10


######## Evaluate performance of full model ########

# We use the STS benchmark dataset to see how much performance we loose by using the dimensionality reduction
sts_dataset_path = "./data/stsbenchmark.tsv.gz"
if not os.path.exists(sts_dataset_path):
    util.http_get("https://sbert.net/datasets/stsbenchmark.tsv.gz", sts_dataset_path)

# We measure the performance of the original model
# and later we will measure the performance with the reduces dimension size
logger.info("Read STSbenchmark test dataset")
eval_examples = []
with gzip.open(sts_dataset_path, "rt", encoding="utf8") as fIn:
    reader = csv.DictReader(fIn, delimiter="\t", quoting=csv.QUOTE_NONE)
    for row in reader:
        if row["split"] == "test":
            score = float(row["score"]) / 5.0 #Normalize score to range 0 ... 1
            eval_examples.append(InputExample(texts=[row["sentence1"], row["sentence2"]], label=score))

# Evaluate the original model on the STS benchmark dataset
stsb_evaluator = evaluation.EmbeddingSimilarityEvaluator.from_input_examples(eval_examples, name="sts-benchmark-test")

logger.info("Original model performance:")
stsb_evaluator(model)


######## Reduce the embedding dimensions ########

# We load the films dataset and creates a list of unique sentences utilizing the movie title and the genres
films_dataset = films.load_films_dataset()
films_sentences = list(set(films.get_films_sentences(films_dataset)))
random.shuffle(films_sentences)

# To determine the PCA matrix, we need some example sentence embeddings.
# Here, we compute the embeddings for all the movies in the films dataset. 
pca_train_sentences = films_sentences
train_embeddings = model.encode(pca_train_sentences, convert_to_numpy=True)

# Compute PCA on the train embeddings matrix
pca = PCA(n_components=new_dimension)
pca.fit(train_embeddings)
pca_comp = np.asarray(pca.components_)

# We add a dense layer to the model, so that it will produce directly embeddings with the new size
dense = models.Dense(in_features=model.get_sentence_embedding_dimension(), out_features=new_dimension, bias=False, activation_function=torch.nn.Identity())
dense.linear.weight = torch.nn.Parameter(torch.tensor(pca_comp))
model.add_module("dense", dense)


######## Evaluate the model with the reduce embedding size
logger.info("Model with {} dimensions:".format(new_dimension))
stsb_evaluator(model)


######## Store the reduced model on disc
model.save(films.PATH_FILMS_MODEL)
