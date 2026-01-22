#!/bin/bash
set -e

echo "==> Configuring Slurm..."

# Get system info for slurm.conf
HOSTNAME=$(hostname)
CPUS=$(nproc)
MEMORY=$(free -m | awk '/^Mem:/{print $2}')

# Configure slurm.conf with actual values
sed -i "s/<<HOSTNAME>>/$HOSTNAME/g" /etc/slurm/slurm.conf
sed -i "s/<<CPU>>/$CPUS/g" /etc/slurm/slurm.conf
sed -i "s/<<MEMORY>>/$MEMORY/g" /etc/slurm/slurm.conf

# Enable accounting storage for slurmrestd (use internal file-based accounting)
# This avoids needing slurmdbd for basic REST API functionality
sed -i "s/AccountingStorageType=accounting_storage\/none/AccountingStorageType=accounting_storage\/slurmdbd/g" /etc/slurm/slurm.conf

# Add slurmdbd configuration if not present
if ! grep -q "AccountingStorageHost" /etc/slurm/slurm.conf; then
    echo "AccountingStorageHost=localhost" >> /etc/slurm/slurm.conf
fi

# Configure Prolog and Epilog scripts for job lifecycle events
# These scripts notify the observability service when jobs start/finish
echo "==> Configuring prolog/epilog scripts..."

# Create directories for prolog/epilog scripts (writable location)
PROLOG_DIR=/opt/slurm/prolog.d
EPILOG_DIR=/opt/slurm/epilog.d
mkdir -p "$PROLOG_DIR" "$EPILOG_DIR"

# Create log directory for prolog/epilog
mkdir -p /var/log/slurm
chmod 755 /var/log/slurm

# Create environment config file for prolog/epilog scripts
# This passes the Docker environment variables to the scripts
cat > /etc/slurm/observability.conf << EOF
# HPC Job Observability Service configuration
# This file is sourced by prolog/epilog scripts
OBSERVABILITY_API_URL="${OBSERVABILITY_API_URL:-http://localhost:8080}"
OBSERVABILITY_TIMEOUT="${OBSERVABILITY_TIMEOUT:-5}"
EOF
chmod 644 /etc/slurm/observability.conf
echo "Created observability config at /etc/slurm/observability.conf"
echo "  OBSERVABILITY_API_URL=$OBSERVABILITY_API_URL"

# Copy mounted scripts to writable location and make executable
if [ -f /etc/slurm/prolog.d/50-observability.sh ]; then
    # Add config sourcing to the beginning of the script
    echo '#!/bin/bash' > "$PROLOG_DIR/50-observability.sh"
    echo '# Source configuration' >> "$PROLOG_DIR/50-observability.sh"
    echo '[ -f /etc/slurm/observability.conf ] && source /etc/slurm/observability.conf' >> "$PROLOG_DIR/50-observability.sh"
    echo '' >> "$PROLOG_DIR/50-observability.sh"
    # Append the rest of the script (skipping the shebang if present)
    tail -n +2 /etc/slurm/prolog.d/50-observability.sh >> "$PROLOG_DIR/50-observability.sh"
    chmod +x "$PROLOG_DIR/50-observability.sh"
    echo "Copied prolog script to $PROLOG_DIR/50-observability.sh"
fi

if [ -f /etc/slurm/epilog.d/50-observability.sh ]; then
    # Add config sourcing to the beginning of the script
    echo '#!/bin/bash' > "$EPILOG_DIR/50-observability.sh"
    echo '# Source configuration' >> "$EPILOG_DIR/50-observability.sh"
    echo '[ -f /etc/slurm/observability.conf ] && source /etc/slurm/observability.conf' >> "$EPILOG_DIR/50-observability.sh"
    echo '' >> "$EPILOG_DIR/50-observability.sh"
    # Append the rest of the script (skipping the shebang if present)
    tail -n +2 /etc/slurm/epilog.d/50-observability.sh >> "$EPILOG_DIR/50-observability.sh"
    chmod +x "$EPILOG_DIR/50-observability.sh"
    echo "Copied epilog script to $EPILOG_DIR/50-observability.sh"
fi

# Add Prolog/Epilog configuration to slurm.conf
# Point to the writable location where scripts are copied
if ! grep -q "^Prolog=" /etc/slurm/slurm.conf; then
    echo "# Job lifecycle event scripts" >> /etc/slurm/slurm.conf
    echo "Prolog=$PROLOG_DIR/*" >> /etc/slurm/slurm.conf
    echo "Epilog=$EPILOG_DIR/*" >> /etc/slurm/slurm.conf
    echo "Added Prolog/Epilog configuration to slurm.conf"
fi

echo "Configured slurm.conf for host=$HOSTNAME, cpus=$CPUS, memory=${MEMORY}MB"

# Create slurmdbd.conf if it doesn't exist
if [ ! -f /etc/slurm/slurmdbd.conf ]; then
    cat > /etc/slurm/slurmdbd.conf << EOF
AuthType=auth/munge
DbdHost=localhost
DbdPort=6819
SlurmUser=slurm
DebugLevel=info
LogFile=/var/log/slurmdbd.log
PidFile=/var/run/slurmdbd.pid
StorageType=accounting_storage/mysql
StorageHost=localhost
StoragePort=3306
StorageUser=slurm
StoragePass=password
StorageLoc=slurm_acct_db
EOF
    chown slurm:slurm /etc/slurm/slurmdbd.conf
    chmod 600 /etc/slurm/slurmdbd.conf
fi

echo "==> Starting Slurm services..."

# Start munge (authentication)
echo "Starting munge..."
service munge start

# Wait for munge to be ready
sleep 2

# Start MariaDB for slurmdbd
echo "Starting MariaDB..."
service mariadb start || service mysql start || true
sleep 3

# Create database and user for slurmdbd
mysql -u root << EOF || true
CREATE DATABASE IF NOT EXISTS slurm_acct_db;
CREATE USER IF NOT EXISTS 'slurm'@'localhost' IDENTIFIED BY 'password';
GRANT ALL ON slurm_acct_db.* TO 'slurm'@'localhost';
FLUSH PRIVILEGES;
EOF

# Start slurmdbd
echo "Starting slurmdbd..."
slurmdbd &
sleep 5

# Register the cluster with slurmdbd
sacctmgr -i add cluster cluster 2>/dev/null || true

# Start slurmctld
echo "Starting slurmctld..."
slurmctld &
sleep 3

# Start slurmd (compute daemon)
echo "Starting slurmd..."
slurmd &
sleep 2

# Wait for slurmctld to be ready
echo "Waiting for slurmctld to be ready..."
max_retries=30
retry_count=0
until sinfo > /dev/null 2>&1 || [ $retry_count -ge $max_retries ]; do
    echo "  Waiting... ($retry_count/$max_retries)"
    sleep 2
    ((retry_count++))
done

if [ $retry_count -ge $max_retries ]; then
    echo "ERROR: slurmctld failed to start"
    cat /var/log/slurmctld.log 2>/dev/null | tail -20
    exit 1
fi

echo "==> Slurm cluster is ready!"
sinfo

# Create a dedicated user for slurmrestd (can't be root or SlurmUser)
if ! id slurmrest > /dev/null 2>&1; then
    useradd -r -s /bin/false slurmrest
fi

# Create UNIX socket directory
SOCKET_DIR=/var/run/slurmrestd
mkdir -p $SOCKET_DIR
chown slurmrest:slurmrest $SOCKET_DIR

# Create a simple socat TCP-to-socket proxy script
cat > /usr/local/bin/slurm-api-proxy.sh << 'PROXY_SCRIPT'
#!/bin/bash
# Proxy TCP connections to slurmrestd UNIX socket
# This allows external access while using rest_auth/local
exec socat TCP-LISTEN:6820,fork,reuseaddr UNIX-CONNECT:/var/run/slurmrestd/slurmrestd.socket
PROXY_SCRIPT
chmod +x /usr/local/bin/slurm-api-proxy.sh

# Start socat proxy in background
echo "Starting TCP->UNIX socket proxy on port 6820..."
/usr/local/bin/slurm-api-proxy.sh &
PROXY_PID=$!

# Wait for socket proxy to start
sleep 1

echo ""
echo "==> Starting slurmrestd on UNIX socket (rest_auth/local)..."
echo "    The TCP proxy on port 6820 forwards to the UNIX socket."
echo "    Example: curl http://localhost:6820/slurm/v0.0.38/jobs"
echo ""

# Start slurmrestd on UNIX socket with local auth
exec su -s /bin/bash slurmrest -c "slurmrestd -a rest_auth/local unix:$SOCKET_DIR/slurmrestd.socket"
