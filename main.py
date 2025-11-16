from typing import Union
from fastapi import FastAPI
import csv

app = FastAPI()

class Movie:
    def __init__(self, id: int, title: str, genres: str):
        self.id = id
        self.title = title
        self.genres = genres

class Rating:
    def __init__(self, userId: int, movieId: int, rating: float, timestamp: int):
        self.userId = userId
        self.movieId = movieId
        self.rating = rating
        self.timestamp = timestamp

class Link:
    def __init__(self, movieId: int, imdbId: int, tmdbId: int):
        self.movieId = movieId
        self.imdbId = imdbId
        self.tmdbId = tmdbId

class Tag:
    def __init__(self, userId: int, movieId: int, tag: str, timestamp: int):
        self.userId = userId
        self.movieId = movieId
        self.tag = tag
        self.timestamp = timestamp
def load_movies_from_file_movies(filepath: str = "movies.csv"):
    movies = []
    with open(filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")

        for row in reader:
            movie = Movie(
                id=int(row["movieId"]),
                title=row["title"],
                genres=row["genres"],
            )
            movies.append(movie.__dict__)

    return movies

def load_movies_from_file_ratings(filepath: str = "ratings.csv"):
    ratings = []
    with open(filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rating = Rating(
                userId=row["userId"],
                movieId=(row["movieId"]),
                rating=row["rating"],
                timestamp=row["timestamp"]
            )
            ratings.append(rating.__dict__)
    return ratings

def load_movies_from_file_links(filepath: str = "links.csv"):
    links = []
    with open(filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")

        for row in reader:
            link = Link(
                movieId=row["movieId"],
                imdbId=row["imdbId"],
                tmdbId=row["tmdbId"],
            )
            links.append(link.__dict__)

    return links

def load_movies_from_file_tags(filepath: str = "tags.csv"):
    print("ŁADUJĘ TAGI...")
    tags = []
    with open(filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for row in reader:
            tag = Tag(
                userId=int(row["userId"]),
                movieId=int(row["movieId"]),
                tag=row["tag"],
                timestamp=int(row["timestamp"])
            )
            tags.append(tag.__dict__)
    return tags
print("ŁADUJĘ TEN MAIN:", __file__)
@app.get("/")
def read_root():
    return {"hello": "world"}
@app.get("/movies")
def get_movies():
    return load_movies_from_file_movies()
@app.get("/ratings")
def get_ratings():
    return load_movies_from_file_ratings()
@app.get("/links")
def get_links():
    return load_movies_from_file_links()
@app.get("/tags")
def get_tags():
    return load_movies_from_file_tags()