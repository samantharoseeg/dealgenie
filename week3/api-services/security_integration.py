#!/usr/bin/env python3
"""
Security Integration Library for DealGenie API Ecosystem
Easy-to-integrate authentication, rate limiting, and logging for all services
"""

import requests
import time
import logging
from typing import Dict, Optional, Callable, Any
from functools import wraps
from fastapi import HTTPException, Request, Depends, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime
import sqlite3

logger = logging.getLogger(__name__)

class SecurityClient:
    """Client for communicating with the central security system"""
    
    def __init__(self, security_service_url: str = "http://localhost:8012"):
        self.security_service_url = security_service_url
        self.auth_cache = {}  # Simple in-memory cache for auth results
        self.cache_ttl = 60  # Cache TTL in seconds
    
    def validate_api_key(self, api_key: str) -> Optional[Dict]:
        """Validate API key against security service"""
        # Check cache first
        cache_key = api_key
        if cache_key in self.auth_cache:
            cached_result, timestamp = self.auth_cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return cached_result
        
        try:
            response = requests.get(
                f"{self.security_service_url}/auth/validate",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=5
            )
            
            if response.status_code == 200:
                result = response.json()
                # Cache successful validation
                self.auth_cache[cache_key] = (result, time.time())
                return result
            else:
                # Cache negative result briefly
                self.auth_cache[cache_key] = (None, time.time())
                return None
                
        except requests.RequestException as e:
            logger.error(f"Security service unreachable: {e}")
            # In production, you might want to fail open or use a backup validation method
            return None
    
    def log_request(self, user_id: str, api_key: str, endpoint: str, method: str, 
                   status_code: int, response_time_ms: float, service_port: int,
                   request_size: int = 0, response_size: int = 0, 
                   ip_address: str = None, user_agent: str = None, 
                   error_message: str = None):
        """Log request to security service"""
        try:
            requests.post(
                f"{self.security_service_url}/internal/log-request",
                json={
                    "user_id": user_id,
                    "api_key": api_key,
                    "endpoint": endpoint,
                    "method": method,
                    "status_code": status_code,
                    "response_time_ms": response_time_ms,
                    "request_size": request_size,
                    "response_size": response_size,
                    "ip_address": ip_address,
                    "user_agent": user_agent,
                    "service_port": service_port,
                    "error_message": error_message
                },
                timeout=2
            )
        except requests.RequestException as e:
            logger.warning(f"Failed to log request: {e}")

class SecureAPIMiddleware:
    """FastAPI middleware for securing existing APIs"""
    
    def __init__(self, service_name: str, service_port: int, 
                 security_service_url: str = "http://localhost:8012"):
        self.service_name = service_name
        self.service_port = service_port
        self.security_client = SecurityClient(security_service_url)
        self.security = HTTPBearer()
    
    async def verify_api_key(self, 
                           credentials: HTTPAuthorizationCredentials = Security(HTTPBearer()),
                           request: Request = None) -> Dict:
        """Verify API key and return user info"""
        start_time = time.time()
        
        try:
            api_key = credentials.credentials
            user_info = self.security_client.validate_api_key(api_key)
            
            if not user_info:
                # Log failed authentication attempt
                self.security_client.log_request(
                    user_id="anonymous",
                    api_key=api_key[:10] + "...",  # Partial key for security
                    endpoint=str(request.url.path) if request else "/unknown",
                    method=request.method if request else "GET",
                    status_code=401,
                    response_time_ms=(time.time() - start_time) * 1000,
                    service_port=self.service_port,
                    ip_address=request.client.host if request else None,
                    user_agent=request.headers.get("user-agent") if request else None,
                    error_message="Invalid API key"
                )
                
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or expired API key",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            
            # Check if rate limit was exceeded (included in user_info response)
            if not user_info.get("valid", True):
                rate_info = user_info.get("rate_limit_info", {})
                
                # Log rate limit exceeded
                self.security_client.log_request(
                    user_id=user_info.get("user_info", {}).get("user_id", "unknown"),
                    api_key=api_key[:10] + "...",
                    endpoint=str(request.url.path) if request else "/unknown", 
                    method=request.method if request else "GET",
                    status_code=429,
                    response_time_ms=(time.time() - start_time) * 1000,
                    service_port=self.service_port,
                    ip_address=request.client.host if request else None,
                    user_agent=request.headers.get("user-agent") if request else None,
                    error_message="Rate limit exceeded"
                )
                
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Rate limit exceeded",
                    headers={
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(rate_info.get("reset_time", "")),
                        "X-RateLimit-Limit": str(rate_info.get("limit", ""))
                    }
                )
            
            # Extract user info from successful validation
            user_data = user_info.get("user_info", {})
            user_data["api_key"] = api_key
            user_data["service_name"] = self.service_name
            user_data["start_time"] = start_time
            
            return user_data
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Authentication error in {self.service_name}: {e}")
            
            # Log internal error
            self.security_client.log_request(
                user_id="system",
                api_key="system",
                endpoint=str(request.url.path) if request else "/unknown",
                method=request.method if request else "GET", 
                status_code=500,
                response_time_ms=(time.time() - start_time) * 1000,
                service_port=self.service_port,
                error_message=f"Authentication system error: {str(e)}"
            )
            
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Authentication service error"
            )
    
    def log_successful_request(self, user_info: Dict, request: Request, 
                             response_size: int = 0, additional_data: Dict = None):
        """Log successful API request"""
        try:
            response_time_ms = (time.time() - user_info.get("start_time", time.time())) * 1000
            
            self.security_client.log_request(
                user_id=user_info.get("user_id", "unknown"),
                api_key=user_info.get("api_key", "")[:10] + "...",
                endpoint=str(request.url.path),
                method=request.method,
                status_code=200,
                response_time_ms=response_time_ms,
                service_port=self.service_port,
                request_size=len(str(request.url.query)) if request.url.query else 0,
                response_size=response_size,
                ip_address=request.client.host,
                user_agent=request.headers.get("user-agent")
            )
            
        except Exception as e:
            logger.warning(f"Failed to log successful request: {e}")

def secure_endpoint(service_name: str, service_port: int, 
                   log_response: bool = True, required_permissions: list = None):
    """Decorator to secure individual API endpoints"""
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract request object and user_info from kwargs
            request = None
            user_info = None
            
            for arg in args:
                if hasattr(arg, 'method') and hasattr(arg, 'url'):  # Request object
                    request = arg
                elif isinstance(arg, dict) and 'user_id' in arg:  # User info
                    user_info = arg
            
            for key, value in kwargs.items():
                if hasattr(value, 'method') and hasattr(value, 'url'):  # Request object  
                    request = value
                elif isinstance(value, dict) and 'user_id' in value:  # User info
                    user_info = value
            
            # Check permissions if required
            if required_permissions and user_info:
                user_permissions = user_info.get('permissions', [])
                if not any(perm in user_permissions for perm in required_permissions):
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Required permissions: {required_permissions}"
                    )
            
            try:
                # Execute the original function
                result = await func(*args, **kwargs)
                
                # Log successful request if enabled
                if log_response and user_info and request:
                    middleware = SecureAPIMiddleware(service_name, service_port)
                    response_size = len(str(result)) if result else 0
                    middleware.log_successful_request(user_info, request, response_size)
                
                return result
                
            except Exception as e:
                logger.error(f"Error in secured endpoint {func.__name__}: {e}")
                raise
                
        return wrapper
    return decorator

class LocalSecurityCache:
    """Local fallback security when central service is unavailable"""
    
    def __init__(self, cache_db_path: str = "local_security_cache.db"):
        self.cache_db_path = cache_db_path
        self._init_cache_db()
    
    def _init_cache_db(self):
        """Initialize local cache database"""
        with sqlite3.connect(self.cache_db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS auth_cache (
                    api_key TEXT PRIMARY KEY,
                    user_data TEXT NOT NULL,
                    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL
                )
            ''')
    
    def cache_auth_result(self, api_key: str, user_data: Dict, ttl_seconds: int = 300):
        """Cache authentication result locally"""
        import json
        expires_at = datetime.now().timestamp() + ttl_seconds
        
        with sqlite3.connect(self.cache_db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO auth_cache (api_key, user_data, expires_at)
                VALUES (?, ?, ?)
            ''', (api_key, json.dumps(user_data), expires_at))
    
    def get_cached_auth(self, api_key: str) -> Optional[Dict]:
        """Get cached authentication result"""
        import json
        
        with sqlite3.connect(self.cache_db_path) as conn:
            cursor = conn.execute('''
                SELECT user_data FROM auth_cache 
                WHERE api_key = ? AND expires_at > ?
            ''', (api_key, datetime.now().timestamp()))
            
            result = cursor.fetchone()
            if result:
                return json.loads(result[0])
        return None

def add_security_to_existing_app(app, service_name: str, service_port: int, 
                                security_service_url: str = "http://localhost:8012"):
    """Add security middleware to existing FastAPI app"""
    
    middleware = SecureAPIMiddleware(service_name, service_port, security_service_url)
    
    # Add security dependency that can be used in route decorators
    app.security_middleware = middleware
    
    # Add a reusable dependency
    async def get_current_user(request: Request, user_info = Depends(middleware.verify_api_key)):
        return user_info
    
    app.get_current_user = get_current_user
    
    return app

# Example usage functions for testing
def create_test_api_keys():
    """Create test API keys for development"""
    security_client = SecurityClient()
    
    # This would typically be done through the security service API
    test_keys = {
        "free_user": "dk_test_free_user_key_12345",
        "premium_user": "dk_test_premium_user_key_67890", 
        "enterprise_user": "dk_test_enterprise_user_key_11111"
    }
    
    return test_keys

def test_security_integration():
    """Test the security integration"""
    client = SecurityClient()
    
    # Test with a mock API key
    test_key = "dk_test_key"
    result = client.validate_api_key(test_key)
    
    print(f"Validation result: {result}")
    
    # Test logging
    if result:
        client.log_request(
            user_id=result.get("user_info", {}).get("user_id", "test"),
            api_key=test_key,
            endpoint="/test",
            method="GET", 
            status_code=200,
            response_time_ms=45.5,
            service_port=8000
        )
        print("Request logged successfully")

if __name__ == "__main__":
    test_security_integration()