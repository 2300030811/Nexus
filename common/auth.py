"""
Enhanced authentication and authorization utilities for Nexus platform.

Provides JWT-based authentication, role-based access control, and API key management.
"""

import os
import jwt
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any
from fastapi import Security, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader

from .logging_utils import get_logger
from .secrets import get_secret, set_secret, rotate_secret
from .metrics import AUTH_TOKEN_VALIDATION, AUTH_API_KEY_USAGE

logger = get_logger("auth")

# Configuration - integrate with secrets manager
JWT_SECRET_KEY = get_secret("JWT_SECRET_KEY") or os.getenv("JWT_SECRET_KEY", secrets.token_urlsafe(32))
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = int(os.getenv("JWT_EXPIRATION_HOURS", "24"))
API_KEY = get_secret("API_KEY") or os.getenv("API_KEY", "")
ENV = os.getenv("ENV", "development").strip().lower()

# Security schemes
bearer_scheme = HTTPBearer(auto_error=False)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# Role definitions
ROLES = {
    "admin": ["read", "write", "delete", "manage_users", "system_config"],
    "analyst": ["read", "write"],
    "viewer": ["read"],
    "service": ["read", "write", "system_metrics"]  # For service-to-service communication
}

# In-memory user store (in production, use database)
USERS = {
    "admin": {
        "id": 1,
        "username": "admin",
        "role": "admin",
        "active": True,
        "created_at": datetime.now(timezone.utc)
    },
    "analyst": {
        "id": 2,
        "username": "analyst", 
        "role": "analyst",
        "active": True,
        "created_at": datetime.now(timezone.utc)
    },
    "viewer": {
        "id": 3,
        "username": "viewer",
        "role": "viewer", 
        "active": True,
        "created_at": datetime.now(timezone.utc)
    }
}

class TokenData:
    """Token data structure."""
    def __init__(self, user_id: int, username: str, role: str, permissions: List[str]):
        self.user_id = user_id
        self.username = username
        self.role = role
        self.permissions = permissions

def create_access_token(username: str, role: str = "viewer") -> str:
    """Create a JWT access token."""
    if username not in USERS:
        raise ValueError(f"User {username} not found")
    
    user = USERS[username]
    permissions = ROLES.get(role, ["read"])
    
    payload = {
        "sub": username,
        "user_id": user["id"],
        "role": role,
        "permissions": permissions,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRATION_HOURS),
        "type": "access"
    }
    
    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    logger.info("Created access token", extra={
        "username": username,
        "role": role,
        "expires_in_hours": JWT_EXPIRATION_HOURS
    })
    return token

def create_refresh_token(username: str) -> str:
    """Create a JWT refresh token with longer expiration."""
    if username not in USERS:
        raise ValueError(f"User {username} not found")
    
    payload = {
        "sub": username,
        "type": "refresh",
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(days=30)  # 30 days
    }
    
    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    logger.info("Created refresh token", extra={"username": username})
    return token

def verify_token(token: str) -> TokenData:
    """Verify and decode JWT token."""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        
        username = payload.get("sub")
        if not username or username not in USERS:
            AUTH_TOKEN_VALIDATION.labels(result="invalid").inc()
            raise HTTPException(status_code=401, detail="Invalid token: user not found")
        
        user = USERS[username]
        if not user["active"]:
            AUTH_TOKEN_VALIDATION.labels(result="invalid").inc()
            raise HTTPException(status_code=401, detail="User account is inactive")
        
        AUTH_TOKEN_VALIDATION.labels(result="success").inc()
        return TokenData(
            user_id=payload["user_id"],
            username=username,
            role=payload["role"],
            permissions=payload["permissions"]
        )
        
    except jwt.ExpiredSignatureError:
        AUTH_TOKEN_VALIDATION.labels(result="expired").inc()
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError as e:
        AUTH_TOKEN_VALIDATION.labels(result="invalid").inc()
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")

def verify_api_key(api_key: str) -> Optional[str]:
    """Verify API key for service authentication."""
    if not API_KEY:
        if ENV in {"development", "local", "test"}:
            return "service"  # Default service role in dev
        raise HTTPException(status_code=500, detail="API_KEY is not configured")
    
    if api_key != API_KEY:
        AUTH_API_KEY_USAGE.labels(result="failure").inc()
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    AUTH_API_KEY_USAGE.labels(result="success").inc()
    return "service"

async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
    api_key: Optional[str] = Security(api_key_header)
) -> TokenData:
    """
    Authenticate user via JWT token or API key.
    Returns TokenData with user information and permissions.
    """
    # Try JWT authentication first
    if credentials:
        try:
            return verify_token(credentials.credentials)
        except HTTPException:
            logger.warning("JWT authentication failed", extra={
                "token_preview": credentials.credentials[:20] + "..."
            })
    
    # Fallback to API key for service-to-service communication
    if api_key:
        role = verify_api_key(api_key)
        return TokenData(
            user_id=0,  # System user
            username="service",
            role=role,
            permissions=ROLES.get(role, ["read"])
        )
    
    # No authentication provided
    if ENV in {"development", "local", "test"}:
        # Allow unauthenticated access in development
        return TokenData(
            user_id=0,
            username="dev_user",
            role="admin",  # Full access in dev
            permissions=ROLES["admin"]
        )
    
    raise HTTPException(
        status_code=401,
        detail="Authentication required. Provide JWT token or API key.",
        headers={"WWW-Authenticate": "Bearer"},
    )

def require_permission(required_permission: str):
    """
    Decorator factory to require specific permission.
    Usage: @require_permission("delete")
    """
    def dependency(current_user: TokenData = Depends(get_current_user)) -> TokenData:
        if required_permission not in current_user.permissions:
            logger.warning("Permission denied", extra={
                "username": current_user.username,
                "role": current_user.role,
                "required_permission": required_permission,
                "user_permissions": current_user.permissions
            })
            raise HTTPException(
                status_code=403,
                detail=f"Permission '{required_permission}' required. Current role: {current_user.role}"
            )
        return current_user
    return dependency

def require_role(required_role: str):
    """
    Decorator factory to require specific role.
    Usage: @require_role("admin")
    """
    def dependency(current_user: TokenData = Depends(get_current_user)) -> TokenData:
        if current_user.role != required_role:
            logger.warning("Role access denied", extra={
                "username": current_user.username,
                "current_role": current_user.role,
                "required_role": required_role
            })
            raise HTTPException(
                status_code=403,
                detail=f"Role '{required_role}' required. Current role: {current_user.role}"
            )
        return current_user
    return dependency

# Permission dependencies for common use
RequireRead = require_permission("read")
RequireWrite = require_permission("write")
RequireDelete = require_permission("delete")
RequireManageUsers = require_permission("manage_users")
RequireSystemConfig = require_permission("system_config")
RequireAdmin = require_role("admin")

# Backward compatibility - maintain existing verify_api_key function
async def verify_api_key_legacy(api_key: str = Security(api_key_header)):
    """Legacy API key verification for backward compatibility."""
    if not API_KEY:
        if ENV in {"development", "local", "test"}:
            return
        raise HTTPException(status_code=500, detail="API_KEY is not configured")
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

class TokenManager:
    """Manages token creation and validation."""
    
    @staticmethod
    def generate_token_pair(username: str, role: str = "viewer") -> Dict[str, str]:
        """Generate both access and refresh tokens."""
        access_token = create_access_token(username, role)
        refresh_token = create_refresh_token(username)
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": JWT_EXPIRATION_HOURS * 3600  # seconds
        }
    
    @staticmethod
    def refresh_access_token(refresh_token: str) -> Dict[str, str]:
        """Generate new access token from refresh token."""
        try:
            payload = jwt.decode(refresh_token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
            
            if payload.get("type") != "refresh":
                raise HTTPException(status_code=401, detail="Invalid refresh token")
            
            username = payload.get("sub")
            if not username or username not in USERS:
                raise HTTPException(status_code=401, detail="User not found")
            
            user = USERS[username]
            new_access_token = create_access_token(username, user["role"])
            
            return {
                "access_token": new_access_token,
                "token_type": "bearer",
                "expires_in": JWT_EXPIRATION_HOURS * 3600
            }
            
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Refresh token has expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid refresh token")

def rotate_api_key() -> str:
    """Generate a new API key for rotation and store it in secrets manager."""
    new_key = secrets.token_urlsafe(32)
    
    # Store in secrets manager
    set_secret("API_KEY", new_key, expires_in_days=90)  # 90-day rotation
    
    logger.info("Generated new API key for rotation")
    return new_key

# User management functions (in production, move to database)
def create_user(username: str, role: str = "viewer") -> Dict[str, Any]:
    """Create a new user."""
    if username in USERS:
        raise ValueError(f"User {username} already exists")
    
    if role not in ROLES:
        raise ValueError(f"Invalid role: {role}")
    
    user_id = max(user["id"] for user in USERS.values()) + 1
    USERS[username] = {
        "id": user_id,
        "username": username,
        "role": role,
        "active": True,
        "created_at": datetime.now(timezone.utc)
    }
    
    logger.info("Created new user", extra={
        "username": username,
        "role": role,
        "user_id": user_id
    })
    
    return USERS[username]

def deactivate_user(username: str) -> bool:
    """Deactivate a user account."""
    if username not in USERS:
        return False
    
    USERS[username]["active"] = False
    logger.info("Deactivated user", extra={"username": username})
    return True

def get_user_info(username: str) -> Optional[Dict[str, Any]]:
    """Get user information."""
    return USERS.get(username)
