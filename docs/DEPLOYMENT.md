# 🚀 DealGenie Deployment Guide

**Week 2 - Address Processing & Geocoding Components**

This guide covers deployment of DealGenie's Week 2 address processing and geocoding services for production environments.

## 📋 Prerequisites

### System Requirements
- **Python 3.8+** (Recommended: Python 3.11+)
- **Redis Server 6.0+** for caching
- **SQLite 3.35+** or **PostgreSQL 13+** for database
- **2GB+ RAM** for full dataset processing
- **Network access** for external geocoding APIs

### Operating System Support
- ✅ **macOS** (Intel & Apple Silicon)
- ✅ **Linux** (Ubuntu 20.04+, CentOS 8+, RHEL 8+)
- ✅ **Windows** (WSL2 recommended)

## 🔧 Installation Steps

### 1. System Dependencies

#### macOS
```bash
# Install Homebrew if not present
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Redis
brew install redis
brew services start redis

# Optional: Install libpostal for advanced address parsing
brew install libpostal
```

#### Ubuntu/Debian
```bash
# Update package list
sudo apt update

# Install Redis
sudo apt install redis-server
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Optional: Install libpostal for advanced address parsing
sudo apt install curl autoconf automake libtool pkg-config
curl -L https://github.com/openvenues/libpostal/archive/v1.1.tar.gz | tar -xzf -
cd libpostal-1.1
./bootstrap.sh
./configure
make -j4
sudo make install
sudo ldconfig
```

#### CentOS/RHEL
```bash
# Install EPEL repository
sudo yum install epel-release

# Install Redis
sudo yum install redis
sudo systemctl start redis
sudo systemctl enable redis

# Configure firewall if needed
sudo firewall-cmd --add-port=6379/tcp --permanent
sudo firewall-cmd --reload
```

### 2. Python Environment Setup

```bash
# Clone the repository
git clone https://github.com/your-org/dealgenie.git
cd dealgenie

# Create virtual environment
python3 -m venv geocoding_env
source geocoding_env/bin/activate  # Linux/macOS
# geocoding_env\Scripts\activate  # Windows

# Install required dependencies
pip install -r requirements.txt
```

### 3. Configuration

```bash
# Copy example configuration
cp config/example.env .env

# Edit configuration with your values
nano .env  # or vim .env
```

**Required Configuration:**
```env
# Redis Configuration
REDIS_URL=redis://localhost:6379

# Optional: Google Geocoding API for enhanced accuracy
GOOGLE_GEOCODING_API_KEY=your_api_key_here

# Database URL
DATABASE_URL=sqlite:///./data/dealgenie.db
```

### 4. Database Setup

```bash
# Run database migrations
python3 db/run_migration.py

# Verify database structure
sqlite3 data/dealgenie.db "PRAGMA table_info(raw_permits);"
```

### 5. Service Verification

```bash
# Run health checks
./scripts/daily_health_check.sh

# Test address parser
python3 -c "from src.normalization.address_parser import AddressParser; print('Parser OK')"

# Test geocoder (requires Redis running)
python3 -c "
import asyncio
from src.geocoding.geocoder import HierarchicalGeocoder
result = asyncio.run(HierarchicalGeocoder().geocode('123 Main St, Los Angeles, CA'))
print(f'Geocoder OK: {result.status.value}')
"
```

## 🏗️ Production Deployment

### Docker Deployment (Recommended)

Create `docker-compose.yml`:
```yaml
version: '3.8'
services:
  dealgenie:
    build: .
    environment:
      - REDIS_URL=redis://redis:6379
      - DATABASE_URL=postgresql://user:pass@postgres:5432/dealgenie
    depends_on:
      - redis
      - postgres
    ports:
      - "8000:8000"
  
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data
  
  postgres:
    image: postgis/postgis:15-3.3
    environment:
      POSTGRES_DB: dealgenie
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  redis_data:
  postgres_data:
```

Deploy with Docker:
```bash
# Build and start services
docker-compose up -d

# Check service health
docker-compose exec dealgenie ./scripts/daily_health_check.sh
```

### Kubernetes Deployment

Create Kubernetes manifests in `k8s/`:

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dealgenie
spec:
  replicas: 3
  selector:
    matchLabels:
      app: dealgenie
  template:
    metadata:
      labels:
        app: dealgenie
    spec:
      containers:
      - name: dealgenie
        image: dealgenie:latest
        ports:
        - containerPort: 8000
        env:
        - name: REDIS_URL
          value: "redis://redis-service:6379"
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: dealgenie-secrets
              key: database-url
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
```

Deploy to Kubernetes:
```bash
# Apply configurations
kubectl apply -f k8s/

# Check deployment status
kubectl get deployments
kubectl get pods

# View logs
kubectl logs -f deployment/dealgenie
```

### Systemd Service (Linux)

Create `/etc/systemd/system/dealgenie.service`:
```ini
[Unit]
Description=DealGenie Address Processing Service
After=network.target redis.service

[Service]
Type=simple
User=dealgenie
Group=dealgenie
WorkingDirectory=/opt/dealgenie
Environment=PATH=/opt/dealgenie/geocoding_env/bin
ExecStart=/opt/dealgenie/geocoding_env/bin/python3 -m uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
# Reload systemd and start service
sudo systemctl daemon-reload
sudo systemctl enable dealgenie
sudo systemctl start dealgenie

# Check status
sudo systemctl status dealgenie
```

## 📊 Monitoring & Maintenance

### Health Monitoring

Set up automated health checks:
```bash
# Add to crontab (every 5 minutes)
*/5 * * * * /opt/dealgenie/scripts/daily_health_check.sh >> /var/log/dealgenie-health.log 2>&1
```

### Performance Monitoring

Run performance benchmarks:
```bash
# Check parsing performance
./scripts/performance_monitor.sh

# Monitor geocoding success rates
python3 -c "
from src.geocoding.geocoder import HierarchicalGeocoder
import asyncio

async def check_stats():
    geocoder = HierarchicalGeocoder()
    stats = geocoder.get_stats()
    print(f'Success rate: {stats.get(\"success_rate\", 0):.1%}')
    print(f'Cache hit rate: {stats.get(\"cache_hit_rate\", 0):.1%}')

asyncio.run(check_stats())
"
```

### Log Management

Configure log rotation in `/etc/logrotate.d/dealgenie`:
```
/var/log/dealgenie/*.log {
    daily
    missingok
    rotate 30
    compress
    notifempty
    create 0644 dealgenie dealgenie
    postrotate
        systemctl reload dealgenie
    endscript
}
```

### Database Maintenance

Regular maintenance tasks:
```bash
# Vacuum SQLite database
sqlite3 data/dealgenie.db "VACUUM;"

# Check database size
du -h data/dealgenie.db

# Backup database
cp data/dealgenie.db data/dealgenie.db.backup.$(date +%Y%m%d)
```

## 🔒 Security Considerations

### API Key Management

1. **Never commit API keys to version control**
2. **Use environment variables or secret management systems**
3. **Rotate API keys regularly**
4. **Monitor API usage and quotas**

```bash
# Store secrets securely (example with Kubernetes)
kubectl create secret generic dealgenie-secrets \
  --from-literal=google-api-key=YOUR_KEY_HERE \
  --from-literal=database-url=YOUR_DB_URL_HERE
```

### Network Security

1. **Firewall Configuration**:
```bash
# Allow only necessary ports
sudo ufw allow 8000/tcp  # Application port
sudo ufw allow 6379/tcp  # Redis (internal only)
sudo ufw enable
```

2. **Redis Security**:
```bash
# Configure Redis password
echo "requirepass your_strong_password_here" >> /etc/redis/redis.conf
sudo systemctl restart redis
```

3. **Database Security**:
   - Use strong passwords
   - Enable SSL/TLS connections
   - Restrict network access
   - Regular security updates

## 🚨 Troubleshooting

### Common Issues

#### Redis Connection Failed
```bash
# Check Redis status
redis-cli ping  # Should return PONG

# Check Redis logs
sudo journalctl -u redis

# Restart Redis
sudo systemctl restart redis
```

#### Geocoding API Failures
```bash
# Check API quotas and limits
curl "https://maps.googleapis.com/maps/api/geocode/json?address=test&key=YOUR_KEY"

# Monitor rate limiting
python3 -c "
from src.geocoding.geocoder import HierarchicalGeocoder
import asyncio
geocoder = HierarchicalGeocoder()
print(geocoder.get_stats())
"
```

#### Memory Issues
```bash
# Monitor memory usage
free -h
top -p $(pgrep -f dealgenie)

# Check for memory leaks
python3 -m memory_profiler scripts/performance_monitor.sh
```

#### Database Connection Issues
```bash
# Check database file permissions
ls -la data/dealgenie.db

# Test database connectivity
sqlite3 data/dealgenie.db "SELECT COUNT(*) FROM parcels;"
```

### Performance Tuning

#### Optimize Redis
```bash
# Redis performance tuning in /etc/redis/redis.conf
maxmemory 2gb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
save 60 10000
```

#### Database Optimization
```bash
# SQLite optimization
sqlite3 data/dealgenie.db "
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=10000;
PRAGMA temp_store=memory;
"
```

#### Application Tuning
```env
# Optimize batch sizes in .env
GEOCODER_BATCH_SIZE=20
GEOCODER_MAX_CONCURRENT=10
PARSER_BATCH_SIZE=2000
```

## 📞 Support & Maintenance

### Regular Maintenance Schedule

- **Daily**: Health checks, log review
- **Weekly**: Performance monitoring, cache cleanup
- **Monthly**: Database maintenance, security updates
- **Quarterly**: Dependency updates, capacity planning

### Getting Help

1. **Check logs**: Application and system logs
2. **Run diagnostics**: Health check scripts
3. **Monitor resources**: CPU, memory, disk usage
4. **Check external services**: Redis, database, APIs

### Backup & Recovery

```bash
# Create backup script
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backup/dealgenie"

# Backup database
cp data/dealgenie.db "$BACKUP_DIR/dealgenie_$DATE.db"

# Backup configuration
cp .env "$BACKUP_DIR/config_$DATE.env"

# Cleanup old backups (keep 30 days)
find "$BACKUP_DIR" -name "*.db" -mtime +30 -delete
```

---

**✅ Deployment Complete!**

Your DealGenie address processing and geocoding services are now ready for production use. Monitor the health checks and performance metrics to ensure optimal operation.