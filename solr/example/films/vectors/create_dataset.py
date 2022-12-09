from sentence_transformers import SentenceTransformer, util
import torch

from films import *

#### Load the 10-dimensions model
model = SentenceTransformer(FILEPATH_FILMS_MODEL)

#### Load the original films dataset
films = load_films_dataset()

#### Use the embedding model to calculate vectors for all movies
vectors = calculate_films_vectors(model, films)

#### Visual evaluation of some specific movies

def most_similar_movie(target_idx, top_k=5):
    film = films[target_idx]
    query_embedding = vectors[target_idx]
    
    cos_scores = util.cos_sim(query_embedding, vectors)[0]
    top_results = torch.topk(cos_scores, k=top_k)
    
    print("\n======================\n")
    print("Film:", get_film_sentence(film).replace("\n", " - "))
    print("\nTop 5 most similar films in corpus:")

    for score, idx in zip(top_results[0], top_results[1]):
        movie_str = get_film_sentence(films[idx]).replace("\n", " - ")
        print(f"  - [{idx}] {movie_str} (Score: {score:.4f})")
        
most_similar_movie(200)
most_similar_movie(100)
most_similar_movie(500)
most_similar_movie(911)


#### Export the new films dataset by creating a new field with the embedding vector

films_vectors = [
    film | {
        "film_vector" : list(vectors[idx].astype("float64"))
    }
    for idx, film in enumerate(films)
]

with open(FILEPATH_FILMS_VECTORS_DATASET, "w") as outfile:
    json.dump(films_vectors, outfile, indent=2)
