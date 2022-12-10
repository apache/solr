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
import csv
from lxml import etree
from sentence_transformers import SentenceTransformer

PATH_FILMS_DATASET      = "./data/films.json"
PATH_FILMS_MODEL        = "./models/films-model-size_10"
PATH_FILMS_VECTORS_JSON = "./data/films-vectors.json"
PATH_FILMS_VECTORS_XML  = "./data/films-vectors.xml"
PATH_FILMS_VECTORS_CSV  = "./data/films-vectors.csv"

def load_films_dataset():
    with open(PATH_FILMS_DATASET, "r") as infile:
        films_dataset = json.load(infile)
    return films_dataset

def get_film_sentence(film):
    return f"{film['name']}\n\n{', '.join(film['genre'])}"

def get_films_sentences(films_dataset):
    return [get_film_sentence(film) for film in films_dataset]

def load_films_embedding_model():
    return SentenceTransformer(PATH_FILMS_MODEL)

def calculate_film_vector(model, film):
    film_sentence = get_film_sentence(film)
    return model.encode(film_sentence)

def calculate_films_vectors(model, films_dataset):
    films_sentences = get_films_sentences(films_dataset)
    return model.encode(films_sentences)

def export_films_json(films_dataset):
    with open(PATH_FILMS_VECTORS_JSON, "w") as outfile:
        json.dump(films_dataset, outfile, indent=2)


def export_films_xml(films_dataset):

    films_xml = etree.Element("add")
    for film in films_dataset:

        film_xml = etree.Element("doc")

        for field_name, field_value in film.items():

            field_value = film[field_name]
            if not isinstance(field_value, list):
                field_value = [field_value]
            
            for value in field_value:
                child = etree.Element("field", attrib={"name": field_name})
                child.text = str(value)
                film_xml.append(child)

        films_xml.append(film_xml)

    etree.ElementTree(films_xml).write(
        PATH_FILMS_VECTORS_XML,
        pretty_print=True,
        xml_declaration=True,
        encoding="utf-8"
    )


def export_films_csv(films_dataset):
    with open(PATH_FILMS_VECTORS_CSV, "w") as outfile:
        csvw = csv.DictWriter(outfile, ["name","directed_by","genre","type","id","initial_release_date","film_vector"])
        csvw.writeheader()
        for film in films_dataset:
            film["directed_by"] = "|".join(film["directed_by"])
            film["genre"] = "|".join(film["genre"])
            film["film_vector"] = "|".join(map(str, film["film_vector"]))
            csvw.writerow(film)
