"""
Secrets management utilities for Nexus platform.

Provides secure secret handling, rotation, and integration with external secret stores.
"""

import os
import json
import secrets
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from pathlib import Path

from .logging_utils import get_logger

logger = get_logger("secrets")

class SecretsManager:
    """Centralized secrets management with rotation support."""
    
    def __init__(self, storage_path: Optional[str] = None):
        """
        Initialize secrets manager.
        
        Args:
            storage_path: Path to encrypted secrets file (defaults to ~/.nexus/secrets.json)
        """
        if storage_path is None:
            home_dir = Path.home()
            storage_path = home_dir / ".nexus" / "secrets.json"
        
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self._secrets_cache: Dict[str, Dict[str, Any]] = {}
        self._load_secrets()
    
    def _load_secrets(self):
        """Load secrets from storage."""
        if self.storage_path.exists():
            try:
                with open(self.storage_path, 'r') as f:
                    self._secrets_cache = json.load(f)
                logger.info("Loaded secrets from storage", extra={
                    "secrets_count": len(self._secrets_cache)
                })
            except Exception as e:
                logger.error("Failed to load secrets", extra={"error": str(e)})
                self._secrets_cache = {}
        else:
            self._secrets_cache = {}
            self._save_secrets()
    
    def _save_secrets(self):
        """Save secrets to storage."""
        try:
            with open(self.storage_path, 'w') as f:
                json.dump(self._secrets_cache, f, indent=2, default=str)
            # Set secure permissions
            os.chmod(self.storage_path, 0o600)
            logger.debug("Secrets saved to storage")
        except Exception as e:
            logger.error("Failed to save secrets", extra={"error": str(e)})
    
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a secret value.
        
        Args:
            key: Secret key/name
            default: Default value if secret not found
            
        Returns:
            Secret value or default
        """
        # First check environment variables (for production)
        env_value = os.getenv(key.upper())
        if env_value:
            return env_value
        
        # Then check local secrets cache
        if key in self._secrets_cache:
            secret_data = self._secrets_cache[key]
            
            # Check if secret has expired
            if secret_data.get("expires_at"):
                expires_at = datetime.fromisoformat(secret_data["expires_at"])
                if datetime.now(timezone.utc) > expires_at:
                    logger.warning("Secret expired", extra={"key": key})
                    return default
            
            return secret_data["value"]
        
        return default
    
    def set_secret(self, key: str, value: str, expires_in_days: Optional[int] = None):
        """
        Set a secret value.
        
        Args:
            key: Secret key/name
            value: Secret value
            expires_in_days: Days until expiration (None for no expiration)
        """
        secret_data = {
            "value": value,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        if expires_in_days:
            expires_at = datetime.now(timezone.utc) + timedelta(days=expires_in_days)
            secret_data["expires_at"] = expires_at.isoformat()
        
        self._secrets_cache[key] = secret_data
        self._save_secrets()
        
        logger.info("Secret set", extra={
            "key": key,
            "expires_in_days": expires_in_days,
            "has_expiration": expires_in_days is not None
        })
    
    def rotate_secret(self, key: str, new_value: Optional[str] = None) -> str:
        """
        Rotate a secret with a new value.
        
        Args:
            key: Secret key to rotate
            new_value: New secret value (generated if None)
            
        Returns:
            New secret value
        """
        if new_value is None:
            new_value = secrets.token_urlsafe(32)
        
        old_value = self.get_secret(key)
        self.set_secret(key, new_value)
        
        logger.info("Secret rotated", extra={
            "key": key,
            "had_old_value": bool(old_value)
        })
        
        return new_value
    
    def delete_secret(self, key: str) -> bool:
        """
        Delete a secret.
        
        Args:
            key: Secret key to delete
            
        Returns:
            True if secret was deleted, False if not found
        """
        if key in self._secrets_cache:
            del self._secrets_cache[key]
            self._save_secrets()
            logger.info("Secret deleted", extra={"key": key})
            return True
        return False
    
    def list_secrets(self) -> List[Dict[str, Any]]:
        """
        List all secrets (without values).
        
        Returns:
            List of secret metadata
        """
        secrets_list = []
        for key, data in self._secrets_cache.items():
            secret_info = {
                "key": key,
                "created_at": data.get("created_at"),
                "updated_at": data.get("updated_at"),
                "expires_at": data.get("expires_at"),
                "has_expiration": "expires_at" in data
            }
            secrets_list.append(secret_info)
        
        return secrets_list
    
    def cleanup_expired_secrets(self) -> int:
        """
        Remove expired secrets.
        
        Returns:
            Number of secrets removed
        """
        removed_count = 0
        current_time = datetime.now(timezone.utc)
        
        keys_to_remove = []
        for key, data in self._secrets_cache.items():
            if data.get("expires_at"):
                expires_at = datetime.fromisoformat(data["expires_at"])
                if current_time > expires_at:
                    keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self._secrets_cache[key]
            removed_count += 1
        
        if removed_count > 0:
            self._save_secrets()
            logger.info("Cleaned up expired secrets", extra={"removed_count": removed_count})
        
        return removed_count
    
    def generate_secure_key(self, length: int = 32) -> str:
        """Generate a cryptographically secure key."""
        return secrets.token_urlsafe(length)
    
    def hash_secret(self, value: str) -> str:
        """Hash a secret for verification."""
        return hashlib.sha256(value.encode()).hexdigest()
    
    def verify_secret(self, value: str, hash_value: str) -> bool:
        """Verify a secret against its hash."""
        return self.hash_secret(value) == hash_value

# Global secrets manager instance
_secrets_manager = None

def get_secrets_manager() -> SecretsManager:
    """Get the global secrets manager instance."""
    global _secrets_manager
    if _secrets_manager is None:
        _secrets_manager = SecretsManager()
    return _secrets_manager

def get_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    """Convenience function to get a secret."""
    return get_secrets_manager().get_secret(key, default)

def set_secret(key: str, value: str, expires_in_days: Optional[int] = None):
    """Convenience function to set a secret."""
    return get_secrets_manager().set_secret(key, value, expires_in_days)

def rotate_secret(key: str, new_value: Optional[str] = None) -> str:
    """Convenience function to rotate a secret."""
    return get_secrets_manager().rotate_secret(key, new_value)

# Environment-specific secret loading
def load_environment_secrets():
    """Load secrets from environment variables."""
    env_secrets = [
        "API_KEY",
        "JWT_SECRET_KEY", 
        "DATABASE_URL",
        "KAFKA_PASSWORD",
        "OLLAMA_API_KEY",
        "PROMETHEUS_PASSWORD",
        "GRAFANA_PASSWORD"
    ]
    
    secrets_manager = get_secrets_manager()
    loaded_count = 0
    
    for secret_key in env_secrets:
        env_value = os.getenv(secret_key)
        if env_value and not secrets_manager.get_secret(secret_key):
            secrets_manager.set_secret(secret_key, env_value)
            loaded_count += 1
    
    if loaded_count > 0:
        logger.info("Loaded environment secrets", extra={"count": loaded_count})
    
    return loaded_count

# Secret validation utilities
def validate_secret_strength(secret: str, min_length: int = 16) -> Dict[str, Any]:
    """
    Validate secret strength.
    
    Args:
        secret: Secret to validate
        min_length: Minimum required length
        
    Returns:
        Validation result with recommendations
    """
    result = {
        "is_valid": True,
        "score": 0,
        "issues": [],
        "recommendations": []
    }
    
    # Length check
    if len(secret) < min_length:
        result["is_valid"] = False
        result["issues"].append(f"Secret too short (min: {min_length})")
        result["recommendations"].append(f"Use at least {min_length} characters")
    
    # Complexity checks
    has_upper = any(c.isupper() for c in secret)
    has_lower = any(c.islower() for c in secret)
    has_digit = any(c.isdigit() for c in secret)
    has_special = any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in secret)
    
    complexity_score = sum([has_upper, has_lower, has_digit, has_special])
    result["score"] = complexity_score
    
    if complexity_score < 3:
        result["recommendations"].append("Use mix of uppercase, lowercase, digits, and special characters")
    
    # Common patterns
    if secret.lower() in ["password", "secret", "key", "admin", "123456"]:
        result["is_valid"] = False
        result["issues"].append("Secret is too common")
        result["recommendations"].append("Use a unique, unpredictable secret")
    
    return result

def generate_database_url(
    host: str, 
    port: int, 
    database: str, 
    username: str, 
    password: str,
    ssl_mode: str = "require"
) -> str:
    """Generate a secure database URL."""
    return f"postgresql://{username}:{password}@{host}:{port}/{database}?sslmode={ssl_mode}"

def mask_secret_for_logging(secret: str, visible_chars: int = 4) -> str:
    """Mask a secret for safe logging."""
    if len(secret) <= visible_chars:
        return "*" * len(secret)
    return secret[:visible_chars] + "*" * (len(secret) - visible_chars)
