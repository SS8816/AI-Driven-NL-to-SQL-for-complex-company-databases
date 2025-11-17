"""
Authentication Service
Handles user authentication against HERE's authentication endpoint
"""

import base64
from typing import Dict, Optional

import requests

from app.config import settings
from app.utils.logger import app_logger
from app.utils.errors import AuthenticationError


class AuthService:
    """Service for authenticating users against HERE's auth endpoint"""

    def __init__(self):
        self.auth_endpoint = settings.HERE_AUTH_ENDPOINT

    def authenticate_user(self, username: str, password: str) -> Optional[Dict]:
        """
        Authenticate user against HERE's auth endpoint

        Args:
            username: User's username
            password: User's password

        Returns:
            Dict with user info if successful, None if failed

        Raises:
            AuthenticationError: If authentication fails
        """
        try:
            # Create Basic Auth header
            credentials = f"{username}:{password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()

            headers = {
                "Authorization": f"Basic {encoded_credentials}",
                "Accept": "application/json",
            }

            app_logger.info(
                "authentication_attempt",
                username=username,
                endpoint=self.auth_endpoint
            )

            # Make request to HERE's auth endpoint
            response = requests.get(
                self.auth_endpoint,
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                user_info = response.json()

                app_logger.info(
                    "authentication_success",
                    username=username,
                    email=user_info.get("email")
                )

                return {
                    "username": user_info.get("username") or username,
                    "display_name": user_info.get("user-display-name") or username,
                    "email": user_info.get("email") or f"{username}@here.com",
                    "groups": user_info.get("groups", []),
                    "roles": user_info.get("roles", []),
                }

            elif response.status_code == 401:
                app_logger.warning(
                    "authentication_failed",
                    username=username,
                    reason="invalid_credentials",
                    status_code=response.status_code
                )
                raise AuthenticationError(
                    "Invalid username or password",
                    details={"username": username}
                )

            else:
                app_logger.error(
                    "authentication_error",
                    username=username,
                    status_code=response.status_code,
                    response_text=response.text[:200]
                )
                raise AuthenticationError(
                    f"Authentication service error: {response.status_code}",
                    details={"status_code": response.status_code}
                )

        except requests.exceptions.Timeout:
            app_logger.error(
                "authentication_timeout",
                username=username,
                endpoint=self.auth_endpoint
            )
            raise AuthenticationError(
                "Authentication service timeout",
                details={"username": username}
            )

        except requests.exceptions.ConnectionError as e:
            app_logger.error(
                "authentication_connection_error",
                username=username,
                endpoint=self.auth_endpoint,
                error=str(e)
            )
            raise AuthenticationError(
                "Cannot connect to authentication service",
                details={"username": username}
            )

        except AuthenticationError:
            # Re-raise authentication errors
            raise

        except Exception as e:
            app_logger.error(
                "authentication_unexpected_error",
                username=username,
                error=str(e),
                error_type=type(e).__name__
            )
            raise AuthenticationError(
                f"Unexpected authentication error: {str(e)}",
                details={"username": username}
            )

    def validate_credentials(self, username: str, password: str) -> bool:
        """
        Validate if credentials are correct

        Args:
            username: User's username
            password: User's password

        Returns:
            True if valid, False otherwise
        """
        try:
            self.authenticate_user(username, password)
            return True
        except AuthenticationError:
            return False


# Global instance
auth_service = AuthService()
