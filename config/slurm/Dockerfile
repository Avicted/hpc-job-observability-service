# Slurm with slurmrestd for integration testing
# Based on nathanhess/slurm which provides a complete single-node Slurm environment
FROM nathanhess/slurm:full-root

# Install slurmrestd, curl for health checks, MariaDB for slurmdbd, socat for TCP proxy,
# and jq for JSON processing in prolog/epilog scripts
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        slurmrestd \
        slurmdbd \
        curl \
        mariadb-server \
        socat \
        jq && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create directories for prolog/epilog scripts
RUN mkdir -p /etc/slurm/prolog.d /etc/slurm/epilog.d /var/log/slurm

# Create startup script that initializes Slurm and starts slurmrestd
COPY start-slurm.sh /usr/local/bin/start-slurm.sh
RUN chmod +x /usr/local/bin/start-slurm.sh

EXPOSE 6820

ENTRYPOINT ["/usr/local/bin/start-slurm.sh"]
