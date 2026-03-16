#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/evidence-web"
SERVICE_NAME="evidence-web"
DOMAIN="${1:-}"

if [[ -z "$DOMAIN" ]]; then
  echo "Usage: sudo bash deploy/install_ubuntu.sh <your-domain>"
  exit 1
fi

sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip nginx chromium-browser chromium-chromedriver

sudo mkdir -p "$APP_DIR"
sudo rsync -av --delete ./ "$APP_DIR"/

cd "$APP_DIR"
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

if [[ ! -f .env ]]; then
  cp .env.example .env
fi

sudo cp deploy/evidence-web.service /etc/systemd/system/${SERVICE_NAME}.service
sudo systemctl daemon-reload
sudo systemctl enable ${SERVICE_NAME}
sudo systemctl restart ${SERVICE_NAME}

sudo sed "s/YOUR_DOMAIN/${DOMAIN}/g" deploy/evidence-web.nginx | sudo tee /etc/nginx/sites-available/${SERVICE_NAME} >/dev/null
sudo ln -sf /etc/nginx/sites-available/${SERVICE_NAME} /etc/nginx/sites-enabled/${SERVICE_NAME}
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl restart nginx

echo "Install done. Next: sudo certbot --nginx -d ${DOMAIN}"
