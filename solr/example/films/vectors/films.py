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

import json
from sentence_transformers import SentenceTransformer

FILEPATH_FILMS_DATASET         = "./data/films.json"
FILEPATH_FILMS_MODEL           = "./models/films-model-size_10"
FILEPATH_FILMS_VECTORS_DATASET = "./data/films-vectors.json"

def load_films_dataset():
    with open(FILEPATH_FILMS_DATASET, "r") as infile:
        films = json.load(infile)
    return films

def get_film_sentence(film):
    return f"{film['name']}\n\n{', '.join(film['genre'])}"

def get_films_sentences(films):
    return [get_film_sentence(film) for film in films]

def load_films_embedding_model():
    return SentenceTransformer(FILEPATH_FILMS_MODEL)

def calculate_film_vector(model, film):
    film_sentence = get_film_sentence(film)
    return model.encode(film_sentence)

def calculate_films_vectors(model, films):
    films_sentences = get_films_sentences(films)
    return model.encode(films_sentences)
