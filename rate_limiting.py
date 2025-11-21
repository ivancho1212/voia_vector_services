"""
Rate Limiting for FastAPI using slowapi
"""
try:
    from slowapi import Limiter
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
except ImportError:
    print("⚠️  Warning: slowapi not installed. Rate limiting disabled.")
    Limiter = None
    RateLimitExceeded = Exception

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
try:
    import redis
except ImportError:
    print("⚠️  Warning: redis not installed. Redis backend unavailable.")
    redis = None

import os
from typing import Optional

# Crear instancia del limiter con Redis backend (si está disponible)
redis_url = os.getenv("REDIS_URL", None)

if Limiter:
    if redis_url and redis:
        try:
            # Conectar a Redis para almacenamiento distribuido
            r = redis.from_url(redis_url)
            r.ping()  # Verificar conexión
            limiter = Limiter(
                key_func=get_remote_address,
                storage_uri=redis_url,
                default_limits=["300/minute"]  # Default: 300 req/min
            )
            print(f"✅ Rate limiter usando Redis: {redis_url}")
        except Exception as e:
            print(f"⚠️ Redis no disponible para rate limiting: {e}. Usando memoria local.")
            limiter = Limiter(
                key_func=get_remote_address,
                default_limits=["300/minute"]
            )
    else:
        # Fallback a memoria local
        limiter = Limiter(
            key_func=get_remote_address,
            default_limits=["300/minute"]
        )
        print("⚠️ Rate limiter usando memoria local (no Redis)")
else:
    limiter = None
    print("⚠️ Rate limiting disabled (slowapi not available)")

def setup_rate_limiting(app: FastAPI):
    """
    Setup rate limiting para la aplicación FastAPI
    """
    if limiter:
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

async def _rate_limit_exceeded_handler(request: Request, exc: Exception):
    """
    Manejador personalizado para excepciones de rate limit
    """
    return JSONResponse(
        status_code=429,
        content={
            "error": "Rate limit exceeded",
            "message": f"Too many requests. {str(exc)}",
            "retry_after": "60"
        },
    )

# Constantes de límites específicos por endpoint
LIMITS = {
    "search": "50/minute",  # /search y /search_vectors
    "process_documents": "10/minute",  # /process_documents - es heavy
    "process_urls": "10/minute",  # /process_urls - es heavy
    "process_texts": "10/minute",  # /process_texts - es heavy
    "process_all": "5/minute",  # /process_all - muy heavy
    "validate": "20/minute",  # /validate/{bot_id}
    "sync": "5/minute",  # /sync endpoints - muy heavy
}

# Default para otros endpoints
DEFAULT_LIMIT = "100/minute"
