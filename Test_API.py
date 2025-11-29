import pytest
from fastapi.testclient import TestClient

from main import (
    app,
    SessionLocal,
    Movie,
    Link,
    Rating,
    Tag,
)

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

@pytest.fixture
def sample_data(client):
    db = SessionLocal()
    try:
        db.query(Tag).delete()
        db.query(Rating).delete()
        db.query(Link).delete()
        db.query(Movie).delete()

        # Movies
        m1 = Movie(movieId=1, title="Movie 1", genres="Comedy")
        m2 = Movie(movieId=2, title="Movie 2", genres="Drama")
        db.add_all([m1, m2])

        # Links
        l1 = Link(movieId=1, imdbId="tt0000001", tmdbId="100")
        db.add(l1)

        # Ratings
        r1 = Rating(userId=1, movieId=1, rating=4.5, timestamp=1111111111)
        db.add(r1)

        # Tags
        t1 = Tag(userId=1, movieId=1, tag="funny", timestamp=1111111112)
        db.add(t1)

        db.commit()
    finally:
        db.close()

# ===========================
#   MOVIES – CRUD tests
# ===========================

def test_create_movie(client, sample_data):
    payload = {
        "movieId": 10,
        "title": "New Movie",
        "genres": "Action|Comedy",
    }
    response = client.post("/movies", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["movieId"] == 10
    assert data["title"] == "New Movie"
    assert data["genres"] == "Action|Comedy"

def test_get_movie_existing(client, sample_data):
    response = client.get("/movies/1")
    assert response.status_code == 200
    data = response.json()
    assert data["movieId"] == 1
    assert data["title"] == "Movie 1"

def test_get_movie_not_found(client, sample_data):
    response = client.get("/movies/9999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Movie not found"

def test_update_movie(client, sample_data):
    payload = {
        "title": "Updated Title",
        "genres": "Thriller",
    }
    response = client.put("/movies/1", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["movieId"] == 1
    assert data["title"] == "Updated Title"
    assert data["genres"] == "Thriller"

    get_resp = client.get("/movies/1")
    assert get_resp.status_code == 200
    assert get_resp.json()["title"] == "Updated Title"

def test_delete_movie(client, sample_data):
    assert client.get("/movies/1").status_code == 200

    delete_resp = client.delete("/movies/1")
    assert delete_resp.status_code == 204

    get_resp = client.get("/movies/1")
    assert get_resp.status_code == 404

def test_get_movies_list_returns_all(client, sample_data):
    response = client.get("/movies")
    assert response.status_code == 200
    data = response.json()

    assert len(data) == 2
    ids = {m["movieId"] for m in data}
    assert ids == {1, 2}

# ===========================
#   LINKS – CRUD tests
# ===========================
def test_create_link(client, sample_data):
    payload = {
        "movieId": 2,
        "imdbId": "tt0000002",
        "tmdbId": "200",
    }
    resp = client.post("/links", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["movieId"] == 2
    assert data["imdbId"] == "tt0000002"
    assert data["tmdbId"] == "200"

def test_get_link_existing(client, sample_data):
    resp = client.get("/links/1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["movieId"] == 1
    assert data["imdbId"] == "tt0000001"

def test_get_link_not_found(client, sample_data):
    resp = client.get("/links/9999")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Link not found"

def test_update_link(client, sample_data):
    payload = {"imdbId": "tt9999999", "tmdbId": "999"}
    resp = client.put("/links/1", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["imdbId"] == "tt9999999"
    assert data["tmdbId"] == "999"

def test_delete_link(client, sample_data):
    assert client.get("/links/1").status_code == 200
    resp = client.delete("/links/1")
    assert resp.status_code == 204
    assert client.get("/links/1").status_code == 404

# ===========================
#   RATINGS – CRUD tests
# ===========================

def test_create_rating(client, sample_data):
    payload = {
        "userId": 2,
        "movieId": 1,
        "rating": 3.5,
        "timestamp": 2222222222,
    }
    resp = client.post("/ratings", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["userId"] == 2
    assert data["movieId"] == 1
    assert data["rating"] == 3.5
    assert "id" in data

def test_get_rating_existing(client, sample_data):
    ratings = client.get("/ratings").json()
    assert len(ratings) >= 1
    rating_id = ratings[0]["id"]

    resp = client.get(f"/ratings/{rating_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == rating_id

def test_get_rating_not_found(client, sample_data):
    resp = client.get("/ratings/9999")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Rating not found"

def test_update_rating(client, sample_data):
    ratings = client.get("/ratings").json()
    rating_id = ratings[0]["id"]

    payload = {"rating": 1.0, "timestamp": 3333333333}
    resp = client.put(f"/ratings/{rating_id}", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["rating"] == 1.0
    assert data["timestamp"] == 3333333333

def test_delete_rating(client, sample_data):
    ratings = client.get("/ratings").json()
    rating_id = ratings[0]["id"]

    resp = client.delete(f"/ratings/{rating_id}")
    assert resp.status_code == 204

    resp2 = client.get(f"/ratings/{rating_id}")
    assert resp2.status_code == 404

# ===========================
#   TAGS – CRUD tests
# ===========================

def test_create_tag(client, sample_data):
    payload = {
        "userId": 2,
        "movieId": 1,
        "tag": "boring",
        "timestamp": 4444444444,
    }
    resp = client.post("/tags", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["tag"] == "boring"
    assert data["userId"] == 2
    assert data["movieId"] == 1
    assert "id" in data

def test_get_tag_existing(client, sample_data):
    tags = client.get("/tags").json()
    assert len(tags) >= 1
    tag_id = tags[0]["id"]

    resp = client.get(f"/tags/{tag_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == tag_id

def test_get_tag_not_found(client, sample_data):
    resp = client.get("/tags/9999")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Tag not found"

def test_update_tag(client, sample_data):
    tags = client.get("/tags").json()
    tag_id = tags[0]["id"]

    payload = {"tag": "updated-tag", "timestamp": 5555555555}
    resp = client.put(f"/tags/{tag_id}", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["tag"] == "updated-tag"

def test_delete_tag(client, sample_data):
    tags = client.get("/tags").json()
    tag_id = tags[0]["id"]

    resp = client.delete(f"/tags/{tag_id}")
    assert resp.status_code == 204
    resp2 = client.get(f"/tags/{tag_id}")
    assert resp2.status_code == 404
