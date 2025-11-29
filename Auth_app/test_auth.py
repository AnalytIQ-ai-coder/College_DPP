import pytest
from fastapi.testclient import TestClient

from main import app, SessionLocal, User, hash_password, Base, engine

client = TestClient(app)

@pytest.fixture(scope="session", autouse=True)
def setup_db():
    Base.metadata.create_all(bind=engine)
@pytest.fixture(autouse=True)
def reset_db(setup_db):
    db = SessionLocal()
    try:
        db.query(User).delete()

        admin = User(
            username="admin",
            password_hash=hash_password("admin123"),
            roles="ROLE_ADMIN",
        )
        user = User(
            username="user",
            password_hash=hash_password("user123"),
            roles="ROLE_USER",
        )
        db.add_all([admin, user])
        db.commit()
    finally:
        db.close()

def get_token(username: str, password: str) -> str:
    resp = client.post(
        "/login",
        json={"username": username, "password": password},
    )
    assert resp.status_code == 200
    data = resp.json()
    return data["access_token"]

# ===========================
#   /login
# ===========================
def test_login_success():
    resp = client.post(
        "/login",
        json={"username": "admin", "password": "admin123"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"

def test_login_invalid_password():
    resp = client.post(
        "/login",
        json={"username": "admin", "password": "wrongpass"},
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid credentials"

def test_login_nonexistent_user():
    resp = client.post(
        "/login",
        json={"username": "no_such_user", "password": "whatever"},
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid credentials"

# ===========================
#   /users
# ===========================
def test_create_user_as_admin_success():
    token = get_token("admin", "admin123")

    resp = client.post(
        "/users",
        json={
            "username": "new_user",
            "password": "newpass",
            "roles": ["ROLE_USER"],
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["username"] == "new_user"
    assert "ROLE_USER" in data["roles"]

def test_create_user_without_admin_forbidden():
    token = get_token("user", "user123")

    resp = client.post(
        "/users",
        json={
            "username": "another_user",
            "password": "pass",
            "roles": ["ROLE_USER"],
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403
    assert resp.json()["detail"] == "Insufficient permissions"

def test_create_user_without_token():
    resp = client.post(
        "/users",
        json={
            "username": "no_token_user",
            "password": "pass",
            "roles": ["ROLE_USER"],
        },
    )
    assert resp.status_code == 403

# ===========================
#   /user_details
# ===========================
def test_user_details_with_valid_token():
    token = get_token("user", "user123")

    resp = client.get(
        "/user_details",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["username"] == "user"
    assert "ROLE_USER" in data["roles"]

def test_user_details_without_token():
    resp = client.get("/user_details")
    assert resp.status_code == 403
