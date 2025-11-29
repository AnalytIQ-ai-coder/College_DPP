from typing import Optional, List
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
from fastapi import (
    FastAPI,
    Depends,
    HTTPException,
    status,
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, ConfigDict

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
)
from sqlalchemy.orm import (
    sessionmaker,
    declarative_base,
    Session,
)

from users_db import USERS_DB

# ======================
#   Configuration DB
# ======================
DATABASE_URL = "sqlite:///./auth.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    future=True,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ======================
#   Modal User
# ======================
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String, nullable=False)

    roles = Column(String, nullable=False, default="ROLE_USER")

# ======================
#   Schemy Pydantic
# ======================
class LoginData(BaseModel):
    username: str
    password: str

class UserCreate(BaseModel):
    username: str
    password: str
    roles: Optional[List[str]] = None

class UserOut(BaseModel):
    id: int
    username: str
    roles: List[str]

    model_config = ConfigDict(from_attributes=False)

class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"

# ======================
#   JWT
# ======================
SECRET_KEY = "af99ee3b61d8da12c71e8266189449852f6fb2b15fb8cc5a71647c9a7f34cc70"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 1

security = HTTPBearer()

def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def hash_password(password: str) -> str:
    pw_bytes = password.encode("utf-8")
    hashed = bcrypt.hashpw(pw_bytes, bcrypt.gensalt())
    return hashed.decode("utf-8")

def verify_password(plain_password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(
        plain_password.encode("utf-8"),
        password_hash.encode("utf-8"),
    )

def create_access_token(
    subject: str,
    roles: List[str],
    expires_delta: Optional[timedelta] = None,
) -> str:
    now = datetime.now(timezone.utc)
    if expires_delta is None:
        expires_delta = timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    expire = now + expires_delta

    payload = {
        "sub": subject,
        "roles": roles,
        "iat": now,
        "exp": expire,
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return token

def parse_roles(roles_str: str) -> List[str]:
    if not roles_str:
        return []
    return [r.strip() for r in roles_str.split(",") if r.strip()]

# ======================
#   Lifespan
# ======================
def create_default_admin(db: Session) -> None:
    existing = db.query(User).filter(User.username == "admin").first()
    if existing:
        return

    admin_hashed_pw_bytes = USERS_DB["admin"]

    admin = User(
        username="admin",
        password_hash=admin_hashed_pw_bytes.decode("utf-8"),
        roles="ROLE_ADMIN",
    )
    db.add(admin)
    db.commit()

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        create_default_admin(db)
    yield

app = FastAPI(lifespan=lifespan)

# ======================
#   Dependencies auth
# ======================
def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
) -> User:
    token = credentials.credentials
    credentials_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exc
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired",
        )
    except jwt.InvalidTokenError:
        raise credentials_exc

    user = db.query(User).filter(User.username == username).first()
    if user is None:
        raise credentials_exc
    return user

def get_current_admin_user(
    current_user: User = Depends(get_current_user),
) -> User:
    roles = parse_roles(current_user.roles)
    if "ROLE_ADMIN" not in roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions",
        )
    return current_user

# ======================
#   Endpoints
# ======================
@app.get("/")
def read_root():
    return {"status": "ok"}

@app.post("/login", response_model=TokenOut)
def login(
    data: LoginData,
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.username == data.username).first()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    if not verify_password(data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    roles = parse_roles(user.roles)
    token = create_access_token(subject=user.username, roles=roles)
    return TokenOut(access_token=token)

@app.post("/users", response_model=UserOut)
def create_user(
    user_in: UserCreate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_admin_user),
):

    existing = db.query(User).filter(User.username == user_in.username).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already taken",
        )

    roles_list = user_in.roles if user_in.roles is not None else ["ROLE_USER"]
    roles_str = ",".join(roles_list)

    user = User(
        username=user_in.username,
        password_hash=hash_password(user_in.password),
        roles=roles_str,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    return UserOut(
        id=user.id,
        username=user.username,
        roles=parse_roles(user.roles),
    )

@app.get("/user_details", response_model=UserOut)
def user_details(
    current_user: User = Depends(get_current_user),
):
    return UserOut(
        id=current_user.id,
        username=current_user.username,
        roles=parse_roles(current_user.roles),
    )
