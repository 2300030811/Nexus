import os
from fastapi import Security, HTTPException
from fastapi.security import APIKeyHeader

API_KEY = os.getenv("API_KEY", "")
ENV = os.getenv("ENV", "development").strip().lower()
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def verify_api_key(api_key: str = Security(api_key_header)):
    if not API_KEY:
        if ENV in {"development", "local", "test"}:
            return
        raise HTTPException(status_code=500, detail="API_KEY is not configured")
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
