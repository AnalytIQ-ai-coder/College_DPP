# Imports
from typing import List, Dict, Any
from fastapi import FastAPI, Depends
import csv
from pathlib import Path

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
)
from sqlalchemy.orm import (
    sessionmaker,
    declarative_base,
    relationship,
    Session,
)

# FastAPI
app = FastAPI()

# Database configuration
DATABASE_URL = "sqlite:///./movies.db"
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    future=True,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

MOVIES_CSV = Path("movies.csv")
RATINGS_CSV = Path("ratings.csv")
LINKS_CSV = Path("links.csv")
TAGS_CSV = Path("tags.csv")

# ORM Models
class Movie(Base):
    __tablename__ = "movies"

    movieId = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    genres = Column(String, nullable=False)

    ratings = relationship("Rating", back_populates="movie")
    tags = relationship("Tag", back_populates="movie")
    link = relationship("Link", back_populates="movie", uselist=False)


class Link(Base):
    __tablename__ = "links"

    movieId = Column(Integer, ForeignKey("movies.movieId"), primary_key=True)
    imdbId = Column(String, nullable=True)
    tmdbId = Column(String, nullable=True)

    movie = relationship("Movie", back_populates="link")


class Rating(Base):
    __tablename__ = "ratings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    userId = Column(Integer, nullable=False)
    movieId = Column(Integer, ForeignKey("movies.movieId"), nullable=False)
    rating = Column(Float, nullable=False)
    timestamp = Column(Integer, nullable=False)

    movie = relationship("Movie", back_populates="ratings")


class Tag(Base):
    __tablename__ = "tags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    userId = Column(Integer, nullable=False)
    movieId = Column(Integer, ForeignKey("movies.movieId"), nullable=False)
    tag = Column(String, nullable=False)
    timestamp = Column(Integer, nullable=False)

    movie = relationship("Movie", back_populates="tags")

# Dependency for FASTAPI
def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Loading csv to database
def load_movies_from_csv(db: Session) -> None:
    if not MOVIES_CSV.exists():
        print(f"No file {MOVIES_CSV}, skip import movies.")
        return

    with MOVIES_CSV.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        objs = [
            Movie(
                movieId=int(row["movieId"]),
                title=row["title"],
                genres=row["genres"],
            )
            for row in reader
        ]

    db.add_all(objs)
    db.commit()
def load_links_from_csv(db: Session) -> None:
    if not LINKS_CSV.exists():
        print(f"No file {LINKS_CSV}, skip import links.")
        return

    with LINKS_CSV.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        objs = [
            Link(
                movieId=int(row["movieId"]),
                imdbId=row["imdbId"] or None,
                tmdbId=row["tmdbId"] or None,
            )
            for row in reader
        ]

    db.add_all(objs)
    db.commit()
def load_ratings_from_csv(db: Session) -> None:
    if not RATINGS_CSV.exists():
        print(f"No file {RATINGS_CSV}, skip import ratings.")
        return

    with RATINGS_CSV.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        objs = [
            Rating(
                userId=int(row["userId"]),
                movieId=int(row["movieId"]),
                rating=float(row["rating"]),
                timestamp=int(row["timestamp"]),
            )
            for row in reader
        ]

    db.add_all(objs)
    db.commit()
def load_tags_from_csv(db: Session) -> None:
    if not TAGS_CSV.exists():
        print(f"No file {TAGS_CSV}, skip import tags.")
        return

    with TAGS_CSV.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        objs = [
            Tag(
                userId=int(row["userId"]),
                movieId=int(row["movieId"]),
                tag=row["tag"],
                timestamp=int(row["timestamp"]),
            )
            for row in reader
        ]

    db.add_all(objs)
    db.commit()
def load_all_data_if_needed(db: Session) -> None:

    if db.query(Movie).first() is None:
        load_movies_from_csv(db)

    if db.query(Link).first() is None:
        load_links_from_csv(db)

    if db.query(Rating).first() is None:
        load_ratings_from_csv(db)

    if db.query(Tag).first() is None:
        load_tags_from_csv(db)

# Initialization database on startup
@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        load_all_data_if_needed(db)
# Endpoints API
@app.get("/")
def read_root():
    return {"hello": "world"}


@app.get("/movies")
def get_movies(
    limit: int = 100,
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    movies = db.query(Movie).limit(limit).all()
    return [
        {
            "movieId": m.movieId,
            "title": m.title,
            "genres": m.genres,
        }
        for m in movies
    ]


@app.get("/ratings")
def get_ratings(
    limit: int = 1000,
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    ratings = db.query(Rating).limit(limit).all()
    return [
        {
            "id": r.id,
            "userId": r.userId,
            "movieId": r.movieId,
            "rating": r.rating,
            "timestamp": r.timestamp,
        }
        for r in ratings
    ]


@app.get("/links")
def get_links(
    limit: int = 100,
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    links = db.query(Link).limit(limit).all()
    return [
        {
            "movieId": l.movieId,
            "imdbId": l.imdbId,
            "tmdbId": l.tmdbId,
        }
        for l in links
    ]


@app.get("/tags")
def get_tags(
    limit: int = 1000,
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    tags = db.query(Tag).limit(limit).all()
    return [
        {
            "id": t.id,
            "userId": t.userId,
            "movieId": t.movieId,
            "tag": t.tag,
            "timestamp": t.timestamp,
        }
        for t in tags
    ]