"""
JWT Token Management
Handles JWT token creation, verification, and decoding
"""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError

from app.config import settings
from app.utils.errors import AuthenticationError


def create_access_token(
    data: Dict[str, Any],
    expires_delta: Optional[timedelta] = None
) -> str:
    """
    Create a JWT access token

    Args:
        data: Payload data to encode in token
        expires_delta: Optional custom expiration time

    Returns:
        Encoded JWT token string
    """
    to_encode = data.copy()

    # Set expiration time
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.JWT_EXPIRATION_MINUTES)

    to_encode.update({
        "exp": expire,
        "iat": datetime.utcnow(),
        "type": "access"
    })

    # Encode JWT
    encoded_jwt = jwt.encode(
        to_encode,
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM
    )

    return encoded_jwt


def decode_access_token(token: str) -> Dict[str, Any]:
    """
    Decode and verify JWT access token

    Args:
        token: JWT token string

    Returns:
        Decoded token payload

    Raises:
        AuthenticationError: If token is invalid or expired
    """
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM]
        )

        # Verify token type
        if payload.get("type") != "access":
            raise AuthenticationError("Invalid token type")

        return payload

    except ExpiredSignatureError:
        raise AuthenticationError("Token has expired")

    except JWTError as e:
        raise AuthenticationError(f"Invalid token: {str(e)}")


def get_token_payload(token: str) -> Optional[Dict[str, Any]]:
    """
    Get token payload without verification (for logging/debugging)

    Args:
        token: JWT token string

    Returns:
        Decoded payload or None if invalid
    """
    try:
        return jwt.get_unverified_claims(token)
    except JWTError:
        return None


def create_user_token(
    username: str,
    email: str,
    display_name: str,
    groups: list,
    roles: list
) -> str:
    """
    Create JWT token for authenticated user

    Args:
        username: User's username
        email: User's email
        display_name: User's display name
        groups: User's groups
        roles: User's roles

    Returns:
        JWT token string
    """
    token_data = {
        "sub": username,  # Subject (standard JWT claim)
        "email": email,
        "display_name": display_name,
        "groups": groups,
        "roles": roles
    }

    return create_access_token(token_data)


def extract_username_from_token(token: str) -> str:
    """
    Extract username from token

    Args:
        token: JWT token string

    Returns:
        Username

    Raises:
        AuthenticationError: If token is invalid
    """
    payload = decode_access_token(token)
    username = payload.get("sub")

    if not username:
        raise AuthenticationError("Token missing subject claim")

    return username
