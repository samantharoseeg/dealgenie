# Socrata Authentication Setup Guide

## Overview

This guide explains how to set up authentication for LA City's Socrata Open Data API to access building permits data for the DealGenie permits processing pipeline.

## Why Authentication is Required

### Rate Limits
- **Without Authentication**: 1,000 requests per hour
- **With App Token**: 10,000 requests per hour  
- **With App Token + Basic Auth**: 10,000 requests per hour (more stable)

### Data Access
- Some datasets may require authentication
- Authentication provides more stable access
- Better error handling and support from Socrata

## Step-by-Step Setup

### 1. Create Socrata Account

1. Visit [Socrata Open Data Signup](https://opendata.socrata.com/signup)
2. Create account with your email address
3. Verify your email address
4. Complete profile setup

### 2. Register for App Token

1. Go to [LA Building Permits Dataset](https://dev.socrata.com/foundry/data.lacity.org/yv23-pmwf)
2. Click **"Sign up for an app token"**
3. Fill out the application form:
   - **Application Name**: DealGenie Permits Pipeline
   - **Description**: Real estate development intelligence platform for LA permit analysis
   - **Website**: Your project website (or GitHub repo)
   - **Contact Email**: Your email address
4. Submit application
5. Copy the generated app token

### 3. Configure Environment Variables

#### Development Setup
1. Copy the example configuration:
   ```bash
   cp config/socrata_auth_example.env .env
   ```

2. Edit `.env` with your credentials:
   ```bash
   # Your app token from step 2
   SOCRATA_APP_TOKEN="your_actual_app_token_here"
   
   # Your Socrata account credentials
   SOCRATA_USERNAME="your_email@example.com"
   SOCRATA_PASSWORD="your_password"
   ```

#### Production Setup
For production environments, use secure credential management:

**AWS Secrets Manager**:
```bash
aws secretsmanager create-secret \
    --name "dealgenie/socrata-credentials" \
    --description "Socrata API credentials for DealGenie" \
    --secret-string '{"app_token":"your_token","username":"your_username","password":"your_password"}'
```

**Docker Environment**:
```yaml
# docker-compose.yml
version: '3.8'
services:
  dealgenie:
    build: .
    environment:
      - SOCRATA_APP_TOKEN=${SOCRATA_APP_TOKEN}
      - SOCRATA_USERNAME=${SOCRATA_USERNAME}
      - SOCRATA_PASSWORD=${SOCRATA_PASSWORD}
    env_file:
      - .env.production
```

**Kubernetes Secrets**:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: socrata-credentials
type: Opaque
data:
  app-token: <base64-encoded-token>
  username: <base64-encoded-username>
  password: <base64-encoded-password>
```

## Testing Your Setup

### 1. Test API Connection
```python
from src.etl.permits_extractor import PermitsExtractor

# Initialize with your credentials
extractor = PermitsExtractor()

# Test connection
result = extractor.test_connection()
if result['success']:
    print("✅ Connection successful!")
    print(f"Rate limit: {result['rate_limit']}")
else:
    print("❌ Connection failed:", result['error'])
```

### 2. Test Data Extraction
```python
# Extract small sample
permits = extractor.extract_permits(limit=10)
print(f"✅ Successfully extracted {len(permits)} permits")
```

### 3. Verify Rate Limits
```bash
# Run the rate limit test script
python scripts/test_socrata_rate_limits.py
```

## Configuration Options

### Basic Configuration
```python
# Minimal setup - uses environment variables
extractor = PermitsExtractor()
```

### Custom Configuration  
```python
# Explicit configuration
config = {
    'domain': 'data.lacity.org',
    'app_token': 'your_token',
    'username': 'your_username', 
    'password': 'your_password',
    'timeout': 30,
    'batch_size': 5000
}
extractor = PermitsExtractor(config)
```

### Production Configuration
```python
# Production with retry and circuit breaker
config = {
    'domain': 'data.lacity.org',
    'app_token': os.getenv('SOCRATA_APP_TOKEN'),
    'username': os.getenv('SOCRATA_USERNAME'),
    'password': os.getenv('SOCRATA_PASSWORD'),
    'max_retries': 5,
    'retry_delay': 2.0,
    'circuit_breaker_threshold': 5,
    'rate_limit_buffer': 0.1  # 10% buffer under rate limit
}
extractor = PermitsExtractor(config)
```

## Troubleshooting

### Common Issues

#### 1. Authentication Failures
**Error**: `401 Unauthorized`
```python
# Check credentials
import os
print("App Token:", os.getenv('SOCRATA_APP_TOKEN')[:10] + "..." if os.getenv('SOCRATA_APP_TOKEN') else "Not set")
print("Username:", os.getenv('SOCRATA_USERNAME'))
```

**Solutions**:
- Verify app token is correctly copied
- Check username/password are correct
- Ensure .env file is in correct location
- Check environment variable loading

#### 2. Rate Limit Exceeded
**Error**: `429 Too Many Requests`
```python
# Check current rate limit status
status = extractor.get_rate_limit_status()
print(f"Requests used: {status['used']}/{status['limit']}")
print(f"Reset time: {status['reset_time']}")
```

**Solutions**:
- Wait for rate limit reset (hourly)
- Reduce batch_size in configuration
- Increase delay between requests
- Implement exponential backoff

#### 3. Dataset Access Issues
**Error**: `403 Forbidden`
```python
# Test dataset access
result = extractor.test_dataset_access('yv23-pmwf')
print(f"Dataset accessible: {result['accessible']}")
```

**Solutions**:
- Verify dataset ID is correct
- Check if dataset requires special permissions
- Try alternative dataset ID
- Contact LA Open Data support

#### 4. Network/Timeout Issues
**Error**: `RequestTimeout` or `ConnectionError`
```python
# Test with longer timeout
config = {'timeout': 60}  # 60 seconds
extractor = PermitsExtractor(config)
```

**Solutions**:
- Increase timeout values
- Check network connectivity
- Verify Socrata service status
- Implement retry mechanism

### Debug Mode
Enable debug mode for detailed logging:
```bash
# Environment variable
export DEBUG_MODE=true

# Or in .env file
DEBUG_MODE=true
```

## Security Best Practices

### 1. Credential Protection
- ✅ Never commit credentials to version control
- ✅ Use environment variables or secure vaults
- ✅ Rotate credentials regularly (quarterly)
- ✅ Use least-privilege access principles

### 2. Network Security
- ✅ Use HTTPS for all API requests (default)
- ✅ Implement certificate validation
- ✅ Consider IP whitelisting for production
- ✅ Monitor for suspicious access patterns

### 3. Access Logging
```python
# Enable access logging
import logging
logging.getLogger('socrata_client').setLevel(logging.INFO)
```

### 4. Credential Rotation
```python
# Implement credential rotation
def rotate_credentials():
    new_token = generate_new_app_token()
    update_environment_variable('SOCRATA_APP_TOKEN', new_token)
    test_new_credentials()
```

## Monitoring and Alerting

### Rate Limit Monitoring
```python
def monitor_rate_limits():
    status = extractor.get_rate_limit_status()
    usage_percentage = (status['used'] / status['limit']) * 100
    
    if usage_percentage > 80:
        send_alert(f"Rate limit at {usage_percentage}%")
```

### API Health Monitoring
```python
def health_check():
    try:
        result = extractor.test_connection()
        return result['success']
    except Exception as e:
        log_error(f"API health check failed: {e}")
        return False
```

## Support and Resources

### Socrata Resources
- [Socrata Developer Portal](https://dev.socrata.com/)
- [API Documentation](https://dev.socrata.com/docs/)
- [Rate Limits Guide](https://dev.socrata.com/docs/app-tokens.html)
- [Python Client Library](https://github.com/xmunoz/sodapy)

### LA City Open Data
- [LA Open Data Portal](https://data.lacity.org/)
- [Building Permits Dataset](https://data.lacity.org/A-Prosperous-City/Building-and-Safety-Permit-Information/yv23-pmwf)
- [Data Dictionary](https://data.lacity.org/api/views/yv23-pmwf/files/e4d9aa53-70ba-4b76-9e18-b8cc5e5bf50a?download=true&filename=Building%20and%20Safety%20Permit%20Information%20-%20DataDictionary.pdf)

### Getting Help
1. **Technical Issues**: Create GitHub issue with debug logs
2. **API Problems**: Contact Socrata support with app token
3. **Data Questions**: Reach out to LA Building & Safety Department
4. **Rate Limit Issues**: Review usage patterns and optimize requests

## Quick Reference

### Environment Variables
```bash
SOCRATA_DOMAIN="data.lacity.org"
SOCRATA_APP_TOKEN="your_app_token"
SOCRATA_USERNAME="your_username"
SOCRATA_PASSWORD="your_password"
BATCH_SIZE=5000
RATE_LIMIT_DELAY=1.0
```

### Testing Commands
```bash
# Test connection
python -c "from src.etl.permits_extractor import PermitsExtractor; print(PermitsExtractor().test_connection())"

# Test data extraction
python -c "from src.etl.permits_extractor import PermitsExtractor; print(len(PermitsExtractor().extract_permits(limit=10)))"

# Check rate limits
python scripts/check_rate_limits.py
```

### Rate Limit Headers
- `X-RateLimit-Limit`: Maximum requests per hour
- `X-RateLimit-Remaining`: Requests remaining in current window
- `X-RateLimit-Reset`: Time when rate limit resets (Unix timestamp)