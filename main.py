from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict
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

# ======================
#   DB config
# ======================
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

# ======================
#   ORM Models
# ======================
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

# ======================
#   Pydantic Schemas
# ======================

# Movies
class MovieBase(BaseModel):
    title: str
    genres: str

class MovieCreate(MovieBase):
    movieId: int

class MovieUpdate(MovieBase):
    pass

class MovieOut(MovieBase):
    movieId: int

    model_config = ConfigDict(from_attributes=True)

# Links
class LinkBase(BaseModel):
    imdbId: Optional[str] = None
    tmdbId: Optional[str] = None

class LinkCreate(LinkBase):
    movieId: int

class LinkUpdate(LinkBase):
    pass

class LinkOut(LinkBase):
    movieId: int

    model_config = ConfigDict(from_attributes=True)

# Ratings
class RatingBase(BaseModel):
    userId: int
    movieId: int
    rating: float
    timestamp: int

class RatingCreate(RatingBase):
    pass

class RatingUpdate(BaseModel):
    rating: float
    timestamp: int

class RatingOut(RatingBase):
    id: int

    model_config = ConfigDict(from_attributes=True)

# Tags
class TagBase(BaseModel):
    userId: int
    movieId: int
    tag: str
    timestamp: int

class TagCreate(TagBase):
    pass

class TagUpdate(BaseModel):
    tag: str
    timestamp: int

class TagOut(TagBase):
    id: int

    model_config = ConfigDict(from_attributes=True)

# ======================
#   DB Dependency
# ======================
def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ======================
#   CSV Loaders
# ======================
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

# ======================
#   Lifespan
# ======================
@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        load_all_data_if_needed(db)
    yield

app = FastAPI(lifespan=lifespan)

# ======================
#   Endpoints
# ======================
@app.get("/")
def read_root():
    return {"hello": "world"}

# LIST endpoints
@app.get("/movies", response_model=List[MovieOut])
def get_movies(
    limit: int = 100,
    db: Session = Depends(get_db),
):
    movies = db.query(Movie).limit(limit).all()
    return movies

@app.get("/ratings", response_model=List[RatingOut])
def get_ratings(
    limit: int = 1000,
    db: Session = Depends(get_db),
):
    ratings = db.query(Rating).limit(limit).all()
    return ratings

@app.get("/links", response_model=List[LinkOut])
def get_links(
    limit: int = 100,
    db: Session = Depends(get_db),
):
    links = db.query(Link).limit(limit).all()
    return links

@app.get("/tags", response_model=List[TagOut])
def get_tags(
    limit: int = 1000,
    db: Session = Depends(get_db),
):
    tags = db.query(Tag).limit(limit).all()
    return tags

# ==================
#   CRUD: MOVIES
# ==================
@app.post(
    "/movies",
    response_model=MovieOut,
    status_code=status.HTTP_201_CREATED,
)
def create_movie(
    movie_in: MovieCreate,
    db: Session = Depends(get_db),
):
    existing = db.query(Movie).filter(Movie.movieId == movie_in.movieId).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Movie with this ID already exists",
        )

    movie = Movie(
        movieId=movie_in.movieId,
        title=movie_in.title,
        genres=movie_in.genres,
    )
    db.add(movie)
    db.commit()
    db.refresh(movie)
    return movie

@app.get(
    "/movies/{movie_id}",
    response_model=MovieOut,
)
def get_movie(
    movie_id: int,
    db: Session = Depends(get_db),
):
    movie = db.query(Movie).filter(Movie.movieId == movie_id).first()
    if movie is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Movie not found",
        )
    return movie

@app.put(
    "/movies/{movie_id}",
    response_model=MovieOut,
)
def update_movie(
    movie_id: int,
    movie_in: MovieUpdate,
    db: Session = Depends(get_db),
):
    movie = db.query(Movie).filter(Movie.movieId == movie_id).first()
    if movie is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Movie not found",
        )

    movie.title = movie_in.title
    movie.genres = movie_in.genres
    db.commit()
    db.refresh(movie)
    return movie

@app.delete(
    "/movies/{movie_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_movie(
    movie_id: int,
    db: Session = Depends(get_db),
):
    movie = db.query(Movie).filter(Movie.movieId == movie_id).first()
    if movie is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Movie not found",
        )

    db.query(Link).filter(Link.movieId == movie_id).delete()
    db.query(Rating).filter(Rating.movieId == movie_id).delete()
    db.query(Tag).filter(Tag.movieId == movie_id).delete()

    db.delete(movie)
    db.commit()
    return None

# ==================
#   CRUD: LINKS
# ==================
@app.post(
    "/links",
    response_model=LinkOut,
    status_code=status.HTTP_201_CREATED,
)
def create_link(
    link_in: LinkCreate,
    db: Session = Depends(get_db),
):
    existing = db.query(Link).filter(Link.movieId == link_in.movieId).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Link for this movie already exists",
        )

    link = Link(
        movieId=link_in.movieId,
        imdbId=link_in.imdbId,
        tmdbId=link_in.tmdbId,
    )
    db.add(link)
    db.commit()
    db.refresh(link)
    return link

@app.get(
    "/links/{movie_id}",
    response_model=LinkOut,
)
def get_link(
    movie_id: int,
    db: Session = Depends(get_db),
):
    link = db.query(Link).filter(Link.movieId == movie_id).first()
    if link is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Link not found",
        )
    return link

@app.put(
    "/links/{movie_id}",
    response_model=LinkOut,
)
def update_link(
    movie_id: int,
    link_in: LinkUpdate,
    db: Session = Depends(get_db),
):
    link = db.query(Link).filter(Link.movieId == movie_id).first()
    if link is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Link not found",
        )

    link.imdbId = link_in.imdbId
    link.tmdbId = link_in.tmdbId
    db.commit()
    db.refresh(link)
    return link

@app.delete(
    "/links/{movie_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_link(
    movie_id: int,
    db: Session = Depends(get_db),
):
    link = db.query(Link).filter(Link.movieId == movie_id).first()
    if link is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Link not found",
        )

    db.delete(link)
    db.commit()
    return None

# ==================
#   CRUD: RATINGS
# ==================
@app.post(
    "/ratings",
    response_model=RatingOut,
    status_code=status.HTTP_201_CREATED,
)
def create_rating(
    rating_in: RatingCreate,
    db: Session = Depends(get_db),
):
    rating = Rating(
        userId=rating_in.userId,
        movieId=rating_in.movieId,
        rating=rating_in.rating,
        timestamp=rating_in.timestamp,
    )
    db.add(rating)
    db.commit()
    db.refresh(rating)
    return rating


@app.get(
    "/ratings/{rating_id}",
    response_model=RatingOut,
)
def get_rating(
    rating_id: int,
    db: Session = Depends(get_db),
):
    rating = db.query(Rating).filter(Rating.id == rating_id).first()
    if rating is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Rating not found",
        )
    return rating

@app.put(
    "/ratings/{rating_id}",
    response_model=RatingOut,
)
def update_rating(
    rating_id: int,
    rating_in: RatingUpdate,
    db: Session = Depends(get_db),
):
    rating = db.query(Rating).filter(Rating.id == rating_id).first()
    if rating is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Rating not found",
        )

    rating.rating = rating_in.rating
    rating.timestamp = rating_in.timestamp
    db.commit()
    db.refresh(rating)
    return rating

@app.delete(
    "/ratings/{rating_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_rating(
    rating_id: int,
    db: Session = Depends(get_db),
):
    rating = db.query(Rating).filter(Rating.id == rating_id).first()
    if rating is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Rating not found",
        )

    db.delete(rating)
    db.commit()
    return None

# ==================
#   CRUD: TAGS
# ==================
@app.post(
    "/tags",
    response_model=TagOut,
    status_code=status.HTTP_201_CREATED,
)
def create_tag(
    tag_in: TagCreate,
    db: Session = Depends(get_db),
):
    tag = Tag(
        userId=tag_in.userId,
        movieId=tag_in.movieId,
        tag=tag_in.tag,
        timestamp=tag_in.timestamp,
    )
    db.add(tag)
    db.commit()
    db.refresh(tag)
    return tag

@app.get(
    "/tags/{tag_id}",
    response_model=TagOut,
)
def get_tag(
    tag_id: int,
    db: Session = Depends(get_db),
):
    tag = db.query(Tag).filter(Tag.id == tag_id).first()
    if tag is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tag not found",
        )
    return tag

@app.put(
    "/tags/{tag_id}",
    response_model=TagOut,
)
def update_tag(
    tag_id: int,
    tag_in: TagUpdate,
    db: Session = Depends(get_db),
):
    tag = db.query(Tag).filter(Tag.id == tag_id).first()
    if tag is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tag not found",
        )

    tag.tag = tag_in.tag
    tag.timestamp = tag_in.timestamp
    db.commit()
    db.refresh(tag)
    return tag

@app.delete(
    "/tags/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_tag(
    tag_id: int,
    db: Session = Depends(get_db),
):
    tag = db.query(Tag).filter(Tag.id == tag_id).first()
    if tag is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tag not found",
        )

    db.delete(tag)
    db.commit()
    return None
