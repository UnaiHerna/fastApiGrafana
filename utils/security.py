from fastapi import Depends, status
from fastapi.security import OAuth2PasswordBearer
from typing import Optional
from jose import jwt, JWTError
from pydantic import BaseModel
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# Dictionary to store request counts and timestamps by IP
ip_request_counts = defaultdict(lambda: {"count": 0, "timestamps": []})

# Dictionary to store request counts and timestamps by path
path_request_counts = defaultdict(lambda: {"count": 0, "timestamps": []})


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, max_requests_per_minute: int, max_requests_total: int, path_limit: int):
        super().__init__(app)
        self.max_requests_per_minute = max_requests_per_minute
        self.max_requests_total = max_requests_total
        self.path_limit = path_limit  # The limit for total requests to a path

    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host
        current_time = datetime.now()
        path = request.url.path

        # Handle IP-based rate limiting
        ip_data = ip_request_counts[client_ip]
        ip_data["timestamps"] = [
            timestamp for timestamp in ip_data["timestamps"]
            if current_time - timestamp < timedelta(minutes=1)
        ]

        if len(ip_data["timestamps"]) >= self.max_requests_per_minute:
            raise HTTPException(status_code=429, detail="Too many requests from this IP")

        # Handle path-based rate limiting
        path_data = path_request_counts[path]
        path_data["timestamps"] = [
            timestamp for timestamp in path_data["timestamps"]
            if current_time - timestamp < timedelta(minutes=1)
        ]

        if len(path_data["timestamps"]) >= self.path_limit:
            raise HTTPException(status_code=429, detail="This path has received too many requests")

        # Update IP-based request counts
        ip_data["timestamps"].append(current_time)
        ip_data["count"] += 1

        # Update path-based request counts
        path_data["timestamps"].append(current_time)
        path_data["count"] += 1

        response = await call_next(request)
        return response


# Secret key to encode and decode JWT tokens
SECRET_KEY = "your-secret-key"  # Use environment variable or secure storage in production
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class TokenData(BaseModel):
    username: str


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    return token_data
