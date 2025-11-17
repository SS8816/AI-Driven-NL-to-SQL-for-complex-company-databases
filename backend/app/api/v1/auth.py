"""
Authentication API Endpoints
Handles user login and authentication
"""

from fastapi import APIRouter, Depends, HTTPException, status

from app.models.auth import LoginRequest, LoginResponse, UserInfo
from app.services.auth_service import auth_service
from app.utils.jwt import create_user_token
from app.utils.errors import AuthenticationError, bad_request_exception
from app.utils.logger import app_logger
from app.config import settings
from app.dependencies import get_current_user
from app.db.user_queries import user_queries_repo


router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    """
    Login with username and password

    Authenticates against HERE's authentication endpoint and returns JWT token

    Args:
        request: Login credentials

    Returns:
        LoginResponse with JWT token and user info

    Raises:
        HTTPException 401: If credentials are invalid
        HTTPException 500: If authentication service fails
    """
    try:
        # Authenticate with HERE endpoint
        user_data = auth_service.authenticate_user(
            request.username,
            request.password
        )

        # Create or update user record in database
        await user_queries_repo.create_or_update_user(
            username=user_data["username"],
            email=user_data["email"],
            display_name=user_data["display_name"]
        )

        # Generate JWT token
        token = create_user_token(
            username=user_data["username"],
            email=user_data["email"],
            display_name=user_data["display_name"],
            groups=user_data["groups"],
            roles=user_data["roles"]
        )

        # Build response
        user_info = UserInfo(
            username=user_data["username"],
            email=user_data["email"],
            display_name=user_data["display_name"],
            groups=user_data["groups"],
            roles=user_data["roles"]
        )

        app_logger.info(
            "user_logged_in",
            username=user_data["username"],
            email=user_data["email"]
        )

        return LoginResponse(
            access_token=token,
            token_type="bearer",
            expires_in=settings.JWT_EXPIRATION_MINUTES * 60,  # Convert to seconds
            user=user_info
        )

    except AuthenticationError as e:
        app_logger.warning("login_failed", username=request.username, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "message": str(e),
                "error_code": "AUTH_FAILED"
            }
        )

    except Exception as e:
        app_logger.error("login_error", username=request.username, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "message": "Authentication service error",
                "error_code": "INTERNAL_ERROR"
            }
        )


@router.get("/me", response_model=UserInfo)
async def get_current_user_info(user: UserInfo = Depends(get_current_user)):
    """
    Get current authenticated user information

    Requires: Valid JWT token in Authorization header

    Args:
        user: Authenticated user (injected by dependency)

    Returns:
        UserInfo with user details
    """
    app_logger.info("user_info_requested", username=user.username)
    return user


@router.post("/logout")
async def logout(user: UserInfo = Depends(get_current_user)):
    """
    Logout (placeholder endpoint)

    Since JWT is stateless, logout is handled client-side by discarding the token
    This endpoint is provided for consistency but doesn't perform server-side action

    Args:
        user: Authenticated user (injected by dependency)

    Returns:
        Success message
    """
    app_logger.info("user_logged_out", username=user.username)

    return {
        "success": True,
        "message": "Logged out successfully. Please discard your token."
    }
