"""
FastAPI Dependencies
Dependency injection for authentication and common functionality
"""

from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.utils.jwt import decode_access_token, extract_username_from_token
from app.utils.errors import AuthenticationError, unauthorized_exception
from app.utils.logger import app_logger
from app.models.auth import UserInfo


# Security scheme for JWT Bearer tokens
security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> UserInfo:
    """
    Dependency to get current authenticated user from JWT token

    Args:
        credentials: HTTP Authorization credentials (Bearer token)

    Returns:
        UserInfo object with user details

    Raises:
        HTTPException: If token is invalid or expired
    """
    try:
        token = credentials.credentials

        # Decode and verify token
        payload = decode_access_token(token)

        # Extract user info from payload
        username = payload.get("sub")
        if not username:
            raise AuthenticationError("Token missing username")

        user_info = UserInfo(
            username=username,
            email=payload.get("email", ""),
            display_name=payload.get("display_name", username),
            groups=payload.get("groups", []),
            roles=payload.get("roles", [])
        )

        app_logger.info("user_authenticated", username=username)

        return user_info

    except AuthenticationError as e:
        app_logger.warning("authentication_failed", error=str(e))
        raise unauthorized_exception(str(e))

    except Exception as e:
        app_logger.error("authentication_error", error=str(e))
        raise unauthorized_exception("Authentication failed")


async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False))
) -> Optional[UserInfo]:
    """
    Dependency to get current user if authenticated, None otherwise

    Args:
        credentials: Optional HTTP Authorization credentials

    Returns:
        UserInfo if authenticated, None otherwise
    """
    if not credentials:
        return None

    try:
        return await get_current_user(credentials)
    except HTTPException:
        return None


def get_username(user: UserInfo = Depends(get_current_user)) -> str:
    """
    Dependency to extract just the username from authenticated user

    Args:
        user: Authenticated user info

    Returns:
        Username string
    """
    return user.username
