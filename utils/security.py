from fastapi import Depends, status
from fastapi.security import OAuth2PasswordBearer
from typing import Optional
from jose import jwt, JWTError
from pydantic import BaseModel
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# Dictionary to store request counts and timestamps
request_counts = defaultdict(lambda: {"count": 0, "timestamps": []})


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, max_requests_per_minute: int, max_requests_total: int):
        super().__init__(app)
        self.max_requests_per_minute = max_requests_per_minute
        self.max_requests_total = max_requests_total

    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host
        current_time = datetime.now()

        # Clean up old timestamps
        request_counts[client_ip]["timestamps"] = [
            timestamp for timestamp in request_counts[client_ip]["timestamps"]
            if current_time - timestamp < timedelta(minutes=1)
        ]

        # Check if the request limit is exceeded
        if len(request_counts[client_ip]["timestamps"]) >= self.max_requests_per_minute or \
                request_counts[client_ip]["count"] >= self.max_requests_total:
            raise HTTPException(status_code=429, detail="Too many requests")

        # Update request counts and timestamps
        request_counts[client_ip]["timestamps"].append(current_time)
        request_counts[client_ip]["count"] += 1

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
