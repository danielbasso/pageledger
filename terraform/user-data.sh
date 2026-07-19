#!/bin/bash
# Infra bootstrap only (swap + Docker + Compose); app deployment is a separate manual step.
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

# --- Docker Compose + buildx CLI plugins (Compose v5 build needs buildx >= 0.17)
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL "https://github.com/docker/compose/releases/download/v5.3.1/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
curl -SL "https://github.com/docker/buildx/releases/download/v0.35.0/buildx-v0.35.0.linux-amd64" \
  -o /usr/local/lib/docker/cli-plugins/docker-buildx
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/lib/docker/cli-plugins/docker-buildx

# Completion marker, readable over SSH to confirm cloud-init finished.
docker --version > /var/log/pageledger-bootstrap.log 2>&1
docker compose version >> /var/log/pageledger-bootstrap.log 2>&1
echo "pageledger bootstrap complete" >> /var/log/pageledger-bootstrap.log
