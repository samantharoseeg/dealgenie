#!/usr/bin/env python3
"""
Production-Ready Authentication & Security System for DealGenie API Ecosystem
Centralized security, user management, rate limiting, and analytics
"""

import sqlite3
import hashlib
import secrets
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import logging
from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api_security.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class UserTier:
    """User subscription tier configuration"""
    name: str
    rate_limit_per_minute: int
    max_requests_per_day: int
    max_portfolio_properties: int
    analytics_retention_days: int
    premium_features: List[str]

# Define user tiers
USER_TIERS = {
    "free": UserTier(
        name="Free",
        rate_limit_per_minute=30,
        max_requests_per_day=1000,
        max_portfolio_properties=10,
        analytics_retention_days=7,
        premium_features=[]
    ),
    "premium": UserTier(
        name="Premium", 
        rate_limit_per_minute=100,
        max_requests_per_day=5000,
        max_portfolio_properties=100,
        analytics_retention_days=30,
        premium_features=["batch_analysis", "advanced_preferences", "export_data"]
    ),
    "enterprise": UserTier(
        name="Enterprise",
        rate_limit_per_minute=500,
        max_requests_per_day=50000,
        max_portfolio_properties=1000,
        analytics_retention_days=365,
        premium_features=["batch_analysis", "advanced_preferences", "export_data", "api_webhooks", "white_label"]
    )
}

class SecurityModels:
    """Pydantic models for security operations"""
    
    class UserCreate(BaseModel):
        username: str
        email: str
        password: str
        tier: str = "free"
        organization: Optional[str] = None
    
    class UserLogin(BaseModel):
        username: str
        password: str
    
    class APIKeyCreate(BaseModel):
        name: str
        expires_days: Optional[int] = 30
        permissions: List[str] = ["read"]
    
    class RateLimitStatus(BaseModel):
        requests_remaining: int
        reset_time: datetime
        tier: str
        upgrade_available: bool

class AuthenticationSystem:
    """Centralized authentication and user management"""
    
    def __init__(self, db_path: str = "api_security.db"):
        self.db_path = db_path
        self.security = HTTPBearer()
        self._init_database()
        
    def _init_database(self):
        """Initialize security database tables"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    tier TEXT DEFAULT 'free',
                    organization TEXT,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1,
                    metadata TEXT
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS api_keys (
                    key_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    api_key TEXT UNIQUE NOT NULL,
                    key_name TEXT NOT NULL,
                    permissions TEXT NOT NULL,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_date TIMESTAMP,
                    last_used TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1,
                    usage_count INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS request_logs (
                    log_id TEXT PRIMARY KEY,
                    user_id TEXT,
                    api_key TEXT,
                    endpoint TEXT NOT NULL,
                    method TEXT NOT NULL,
                    status_code INTEGER,
                    response_time_ms REAL,
                    request_size INTEGER,
                    response_size INTEGER,
                    ip_address TEXT,
                    user_agent TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    service_port INTEGER,
                    error_message TEXT
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS rate_limits (
                    user_id TEXT NOT NULL,
                    minute_window TEXT NOT NULL,
                    request_count INTEGER DEFAULT 1,
                    last_request TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, minute_window)
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS usage_analytics (
                    user_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    total_requests INTEGER DEFAULT 0,
                    successful_requests INTEGER DEFAULT 0,
                    failed_requests INTEGER DEFAULT 0,
                    avg_response_time REAL DEFAULT 0,
                    data_transferred_mb REAL DEFAULT 0,
                    PRIMARY KEY (user_id, date)
                )
            ''')
            
        # Create default admin user
        self._create_default_users()
        
    def _create_default_users(self):
        """Create default system users"""
        default_users = [
            {
                "username": "admin",
                "email": "admin@dealgenie.ai",
                "password": "secure_admin_2024!",
                "tier": "enterprise"
            },
            {
                "username": "demo_free",
                "email": "demo@dealgenie.ai", 
                "password": "demo_free_2024",
                "tier": "free"
            },
            {
                "username": "demo_premium",
                "email": "premium@dealgenie.ai",
                "password": "demo_premium_2024",
                "tier": "premium"
            }
        ]
        
        for user_data in default_users:
            try:
                user_id = self.create_user(
                    username=user_data["username"],
                    email=user_data["email"],
                    password=user_data["password"],
                    tier=user_data["tier"]
                )
                # Create default API key for each user
                self.create_api_key(
                    user_id=user_id,
                    name=f"{user_data['username']}_default_key",
                    permissions=["read", "write"]
                )
                logger.info(f"Created default user: {user_data['username']}")
            except Exception as e:
                logger.debug(f"Default user {user_data['username']} may already exist: {e}")
    
    def create_user(self, username: str, email: str, password: str, tier: str = "free") -> str:
        """Create new user account"""
        user_id = secrets.token_urlsafe(16)
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        if tier not in USER_TIERS:
            raise ValueError(f"Invalid tier: {tier}")
            
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute('''
                    INSERT INTO users (user_id, username, email, password_hash, tier)
                    VALUES (?, ?, ?, ?, ?)
                ''', (user_id, username, email, password_hash, tier))
                
                logger.info(f"Created user: {username} with tier: {tier}")
                return user_id
            except sqlite3.IntegrityError as e:
                raise ValueError(f"Username or email already exists: {e}")
    
    def authenticate_user(self, username: str, password: str) -> Optional[Dict]:
        """Authenticate user credentials"""
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT * FROM users 
                WHERE username = ? AND password_hash = ? AND is_active = 1
            ''', (username, password_hash))
            
            user = cursor.fetchone()
            if user:
                # Update last login
                conn.execute('''
                    UPDATE users SET last_login = CURRENT_TIMESTAMP 
                    WHERE user_id = ?
                ''', (user['user_id'],))
                
                return dict(user)
        return None
    
    def create_api_key(self, user_id: str, name: str, permissions: List[str], expires_days: int = 30) -> str:
        """Create API key for user"""
        key_id = secrets.token_urlsafe(16)
        api_key = f"dk_{secrets.token_urlsafe(32)}"  # dk = dealgenie key
        expires_date = datetime.now() + timedelta(days=expires_days)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT INTO api_keys (key_id, user_id, api_key, key_name, permissions, expires_date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (key_id, user_id, api_key, name, json.dumps(permissions), expires_date))
            
        logger.info(f"Created API key '{name}' for user: {user_id}")
        return api_key
    
    def validate_api_key(self, api_key: str) -> Optional[Dict]:
        """Validate API key and return user info"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT u.*, ak.api_key, ak.permissions, ak.expires_date, ak.key_id
                FROM users u
                JOIN api_keys ak ON u.user_id = ak.user_id
                WHERE ak.api_key = ? AND ak.is_active = 1 AND u.is_active = 1
                AND (ak.expires_date IS NULL OR ak.expires_date > CURRENT_TIMESTAMP)
            ''', (api_key,))
            
            result = cursor.fetchone()
            if result:
                # Update key usage
                conn.execute('''
                    UPDATE api_keys 
                    SET last_used = CURRENT_TIMESTAMP, usage_count = usage_count + 1
                    WHERE key_id = ?
                ''', (result['key_id'],))
                
                return dict(result)
        return None

class RateLimitSystem:
    """Advanced rate limiting with tier-based quotas"""
    
    def __init__(self, db_path: str = "api_security.db"):
        self.db_path = db_path
        self.rate_limit_cache = defaultdict(lambda: defaultdict(int))
        
    def check_rate_limit(self, user_id: str, tier: str) -> Tuple[bool, Dict]:
        """Check if user is within rate limits"""
        now = datetime.now()
        minute_window = now.strftime("%Y-%m-%d %H:%M")
        
        tier_config = USER_TIERS.get(tier, USER_TIERS["free"])
        
        with sqlite3.connect(self.db_path) as conn:
            # Get current minute's request count
            cursor = conn.execute('''
                SELECT request_count FROM rate_limits 
                WHERE user_id = ? AND minute_window = ?
            ''', (user_id, minute_window))
            
            result = cursor.fetchone()
            current_count = result[0] if result else 0
            
            # Check if limit exceeded
            if current_count >= tier_config.rate_limit_per_minute:
                reset_time = datetime.strptime(minute_window, "%Y-%m-%d %H:%M") + timedelta(minutes=1)
                return False, {
                    "error": "Rate limit exceeded",
                    "requests_remaining": 0,
                    "reset_time": reset_time,
                    "tier": tier,
                    "limit": tier_config.rate_limit_per_minute
                }
            
            # Update count
            conn.execute('''
                INSERT OR REPLACE INTO rate_limits (user_id, minute_window, request_count, last_request)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, minute_window, current_count + 1))
            
            remaining = tier_config.rate_limit_per_minute - (current_count + 1)
            reset_time = datetime.strptime(minute_window, "%Y-%m-%d %H:%M") + timedelta(minutes=1)
            
            return True, {
                "requests_remaining": remaining,
                "reset_time": reset_time,
                "tier": tier,
                "limit": tier_config.rate_limit_per_minute
            }

class RequestLogger:
    """Comprehensive request logging and analytics"""
    
    def __init__(self, db_path: str = "api_security.db"):
        self.db_path = db_path
    
    def log_request(self, 
                   user_id: str, 
                   api_key: str,
                   endpoint: str,
                   method: str,
                   status_code: int,
                   response_time_ms: float,
                   request_size: int = 0,
                   response_size: int = 0,
                   ip_address: str = None,
                   user_agent: str = None,
                   service_port: int = None,
                   error_message: str = None):
        """Log API request details"""
        log_id = secrets.token_urlsafe(16)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT INTO request_logs (
                    log_id, user_id, api_key, endpoint, method, status_code,
                    response_time_ms, request_size, response_size, ip_address,
                    user_agent, service_port, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (log_id, user_id, api_key, endpoint, method, status_code,
                  response_time_ms, request_size, response_size, ip_address,
                  user_agent, service_port, error_message))
            
            # Update daily analytics
            self._update_daily_analytics(user_id, status_code, response_time_ms, response_size)
    
    def _update_daily_analytics(self, user_id: str, status_code: int, response_time: float, response_size: int):
        """Update daily usage analytics"""
        today = datetime.now().strftime("%Y-%m-%d")
        data_mb = response_size / (1024 * 1024) if response_size else 0
        
        with sqlite3.connect(self.db_path) as conn:
            # Get existing analytics
            cursor = conn.execute('''
                SELECT * FROM usage_analytics WHERE user_id = ? AND date = ?
            ''', (user_id, today))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing record
                total_requests = existing[2] + 1
                successful_requests = existing[3] + (1 if status_code < 400 else 0)
                failed_requests = existing[4] + (1 if status_code >= 400 else 0)
                avg_response_time = ((existing[5] * existing[2]) + response_time) / total_requests
                total_data_mb = existing[6] + data_mb
                
                conn.execute('''
                    UPDATE usage_analytics 
                    SET total_requests = ?, successful_requests = ?, failed_requests = ?,
                        avg_response_time = ?, data_transferred_mb = ?
                    WHERE user_id = ? AND date = ?
                ''', (total_requests, successful_requests, failed_requests, 
                      avg_response_time, total_data_mb, user_id, today))
            else:
                # Create new record
                conn.execute('''
                    INSERT INTO usage_analytics (
                        user_id, date, total_requests, successful_requests, 
                        failed_requests, avg_response_time, data_transferred_mb
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, today, 1, 
                      1 if status_code < 400 else 0,
                      1 if status_code >= 400 else 0,
                      response_time, data_mb))

class SecurityMiddleware:
    """FastAPI middleware for authentication and security"""
    
    def __init__(self):
        self.auth_system = AuthenticationSystem()
        self.rate_limiter = RateLimitSystem()
        self.request_logger = RequestLogger()
    
    async def verify_api_key(self, credentials: HTTPAuthorizationCredentials = Security(HTTPBearer())):
        """Verify API key authentication"""
        try:
            api_key = credentials.credentials
            user_info = self.auth_system.validate_api_key(api_key)
            
            if not user_info:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or expired API key",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            
            # Check rate limits
            allowed, rate_info = self.rate_limiter.check_rate_limit(
                user_info['user_id'], 
                user_info['tier']
            )
            
            if not allowed:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=rate_info['error'],
                    headers={
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(int(rate_info['reset_time'].timestamp())),
                        "X-RateLimit-Limit": str(rate_info['limit'])
                    }
                )
            
            return {
                "user_id": user_info['user_id'],
                "username": user_info['username'],
                "tier": user_info['tier'],
                "permissions": json.loads(user_info['permissions']),
                "api_key": api_key,
                "rate_limit_info": rate_info
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Authentication service error"
            )

# Initialize FastAPI app for security management
app = FastAPI(
    title="DealGenie Security & Authentication System",
    description="Production-ready authentication, rate limiting, and analytics for DealGenie API ecosystem",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize security components
security_middleware = SecurityMiddleware()
auth_system = AuthenticationSystem()
request_logger = RequestLogger()

@app.get("/")
async def security_system_info():
    """Get security system information"""
    return {
        "system": "DealGenie Security & Authentication",
        "version": "1.0.0",
        "features": [
            "API key authentication",
            "Tier-based rate limiting",
            "Request logging and analytics",
            "User management",
            "Usage tracking"
        ],
        "tiers": {
            tier_name: {
                "rate_limit_per_minute": tier.rate_limit_per_minute,
                "max_requests_per_day": tier.max_requests_per_day,
                "premium_features": tier.premium_features
            }
            for tier_name, tier in USER_TIERS.items()
        },
        "endpoints": [
            "/auth/register - User registration",
            "/auth/login - User authentication", 
            "/auth/api-keys - API key management",
            "/analytics/usage - Usage analytics",
            "/admin/users - User management (admin only)"
        ]
    }

@app.post("/auth/register")
async def register_user(user_data: SecurityModels.UserCreate):
    """Register new user account"""
    try:
        user_id = auth_system.create_user(
            username=user_data.username,
            email=user_data.email,
            password=user_data.password,
            tier=user_data.tier
        )
        
        # Create default API key
        api_key = auth_system.create_api_key(
            user_id=user_id,
            name="default_key",
            permissions=["read", "write"]
        )
        
        return {
            "message": "User registered successfully",
            "user_id": user_id,
            "api_key": api_key,
            "tier": user_data.tier,
            "rate_limit": USER_TIERS[user_data.tier].rate_limit_per_minute
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/auth/login")
async def login_user(login_data: SecurityModels.UserLogin):
    """Authenticate user and return API key"""
    user = auth_system.authenticate_user(login_data.username, login_data.password)
    
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    # Get user's API keys
    with sqlite3.connect(auth_system.db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute('''
            SELECT api_key, key_name FROM api_keys 
            WHERE user_id = ? AND is_active = 1
            ORDER BY created_date DESC LIMIT 1
        ''', (user['user_id'],))
        
        api_key_info = cursor.fetchone()
    
    return {
        "message": "Login successful",
        "user_id": user['user_id'],
        "username": user['username'],
        "tier": user['tier'],
        "api_key": api_key_info['api_key'] if api_key_info else None,
        "rate_limit": USER_TIERS[user['tier']].rate_limit_per_minute
    }

@app.get("/auth/validate")
async def validate_token(user_info = Depends(security_middleware.verify_api_key)):
    """Validate API key and return user info"""
    return {
        "valid": True,
        "user_info": user_info,
        "tier_config": {
            "name": USER_TIERS[user_info['tier']].name,
            "rate_limit": USER_TIERS[user_info['tier']].rate_limit_per_minute,
            "premium_features": USER_TIERS[user_info['tier']].premium_features
        }
    }

@app.get("/analytics/usage")
async def get_usage_analytics(
    days: int = 7,
    user_info = Depends(security_middleware.verify_api_key)
):
    """Get user usage analytics"""
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    with sqlite3.connect(auth_system.db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute('''
            SELECT * FROM usage_analytics 
            WHERE user_id = ? AND date >= ?
            ORDER BY date DESC
        ''', (user_info['user_id'], start_date))
        
        analytics = [dict(row) for row in cursor.fetchall()]
    
    # Calculate summary statistics
    total_requests = sum(row['total_requests'] for row in analytics)
    avg_response_time = sum(row['avg_response_time'] * row['total_requests'] for row in analytics) / total_requests if total_requests > 0 else 0
    
    return {
        "user_id": user_info['user_id'],
        "period_days": days,
        "summary": {
            "total_requests": total_requests,
            "average_response_time_ms": round(avg_response_time, 2),
            "total_data_transferred_mb": round(sum(row['data_transferred_mb'] for row in analytics), 2),
            "success_rate": round(sum(row['successful_requests'] for row in analytics) / total_requests * 100, 2) if total_requests > 0 else 0
        },
        "daily_breakdown": analytics,
        "tier_limits": {
            "current_tier": user_info['tier'],
            "rate_limit_per_minute": USER_TIERS[user_info['tier']].rate_limit_per_minute,
            "max_requests_per_day": USER_TIERS[user_info['tier']].max_requests_per_day
        }
    }

@app.post("/auth/api-keys")
async def create_api_key(
    key_data: SecurityModels.APIKeyCreate,
    user_info = Depends(security_middleware.verify_api_key)
):
    """Create new API key for user"""
    api_key = auth_system.create_api_key(
        user_id=user_info['user_id'],
        name=key_data.name,
        permissions=key_data.permissions,
        expires_days=key_data.expires_days or 30
    )
    
    return {
        "message": "API key created successfully",
        "api_key": api_key,
        "name": key_data.name,
        "permissions": key_data.permissions,
        "expires_in_days": key_data.expires_days or 30
    }

@app.get("/status/rate-limits")
async def get_rate_limit_status(user_info = Depends(security_middleware.verify_api_key)):
    """Get current rate limit status"""
    return SecurityModels.RateLimitStatus(
        requests_remaining=user_info['rate_limit_info']['requests_remaining'],
        reset_time=user_info['rate_limit_info']['reset_time'],
        tier=user_info['tier'],
        upgrade_available=user_info['tier'] != "enterprise"
    )

if __name__ == "__main__":
    logger.info("Starting DealGenie Security & Authentication System...")
    uvicorn.run(app, host="0.0.0.0", port=8012)