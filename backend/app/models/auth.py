"""
Authentication Models
Pydantic models for authentication requests and responses
"""

from typing import List, Optional
from pydantic import BaseModel, Field, EmailStr


class LoginRequest(BaseModel):
    """User login request"""
    username: str = Field(..., min_length=1, description="Username")
    password: str = Field(..., min_length=1, description="Password")

    model_config = {
        "json_schema_extra": {
            "example": {
                "username": "john.doe",
                "password": "your-password"
            }
        }
    }


class TokenResponse(BaseModel):
    """JWT token response"""
    access_token: str = Field(..., description="JWT access token")
    token_type: str = Field(default="bearer", description="Token type")
    expires_in: int = Field(..., description="Token expiration time in seconds")

    model_config = {
        "json_schema_extra": {
            "example": {
                "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "token_type": "bearer",
                "expires_in": 28800
            }
        }
    }


class UserInfo(BaseModel):
    """User information"""
    username: str = Field(..., description="Username")
    email: str = Field(..., description="Email address")
    display_name: str = Field(..., description="Display name")
    groups: List[str] = Field(default_factory=list, description="User groups")
    roles: List[str] = Field(default_factory=list, description="User roles")

    model_config = {
        "json_schema_extra": {
            "example": {
                "username": "john.doe",
                "email": "john.doe@here.com",
                "display_name": "John Doe",
                "groups": ["engineering", "data-team"],
                "roles": ["user", "analyst"]
            }
        }
    }


class LoginResponse(BaseModel):
    """Complete login response with token and user info"""
    access_token: str = Field(..., description="JWT access token")
    token_type: str = Field(default="bearer", description="Token type")
    expires_in: int = Field(..., description="Token expiration in seconds")
    user: UserInfo = Field(..., description="User information")

    model_config = {
        "json_schema_extra": {
            "example": {
                "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "token_type": "bearer",
                "expires_in": 28800,
                "user": {
                    "username": "john.doe",
                    "email": "john.doe@here.com",
                    "display_name": "John Doe",
                    "groups": ["engineering"],
                    "roles": ["user"]
                }
            }
        }
    }
