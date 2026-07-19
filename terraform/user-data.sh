#!/bin/bash
# Infra bootstrap only (swap + Docker + Compose); app deployment is a manual step.
set -euxo pipefail

# Swap cushion for the JVM-heavy stack.
if [ ! -f /swapfile ]; then
  dd if=/dev/zero of=/swapfile bs=1M count=4096
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# --- Docker Engine (Amazon Linux 2023)
dnf install -y docker
systemctl enable --now docker
usermod -aG docker ec2-user

# --- Docker Compose v2 CLI plugin
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL "https://github.com/docker/compose/releases/download/v5.3.1/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# Completion marker, readable over SSH to confirm cloud-init finished.
docker --version > /var/log/pageledger-bootstrap.log 2>&1
docker compose version >> /var/log/pageledger-bootstrap.log 2>&1
echo "pageledger bootstrap complete" >> /var/log/pageledger-bootstrap.log
