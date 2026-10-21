#!/bin/bash
# =========================================================================
# Sinyal botu – Hetzner (Ubuntu 24.04) kurulumu
#
# Sunucuda root olarak çalıştır:
#   curl -fsSL https://raw.githubusercontent.com/effekk/btbot/main/kurulum.sh -o kurulum.sh && bash kurulum.sh
#
# Tekrar çalıştırmak güvenlidir: secrets.env ve state.json korunur.
# =========================================================================
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/effekk/btbot.git}"
DIR=/opt/sinyalbot
BOT_USER=sinyalbot

if [ "$(id -u)" -ne 0 ]; then
  echo "Bu script root olarak çalışmalı: sudo bash kurulum.sh"
  exit 1
fi

echo "==> 1/6 Gerekli paketler kuruluyor (1-2 dakika sürebilir)"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git python3-venv tzdata >/dev/null

echo "==> 2/6 Bot kullanıcısı ve klasör"
id "$BOT_USER" >/dev/null 2>&1 || useradd --system --home-dir "$DIR" --shell /usr/sbin/nologin "$BOT_USER"
mkdir -p "$DIR"
chown "$BOT_USER:$BOT_USER" "$DIR"

echo "==> 3/6 Kod GitHub'dan çekiliyor"
if [ -d "$DIR/repo/.git" ]; then
  runuser -u "$BOT_USER" -- git -C "$DIR/repo" pull --ff-only -q
else
  runuser -u "$BOT_USER" -- git clone -q "$REPO_URL" "$DIR/repo"
fi
for f in signal_bot.py binance_trader.py requirements.txt ayarlar.env; do
  if [ ! -f "$DIR/repo/$f" ]; then
    echo "HATA: depoda $f bulunamadı. Önce GitHub'a yükle, sonra kurulumu tekrar çalıştır."
    exit 1
  fi
done

echo "==> 4/6 Python ortamı"
[ -x "$DIR/venv/bin/python" ] || runuser -u "$BOT_USER" -- python3 -m venv "$DIR/venv"
runuser -u "$BOT_USER" -- "$DIR/venv/bin/pip" install -q --disable-pip-version-check -r "$DIR/repo/requirements.txt"

echo "==> 5/6 Gizli bilgiler"
if [ -f "$DIR/secrets.env" ]; then
  echo "    secrets.env zaten var, olduğu gibi bırakıldı."
  echo "    Değiştirmek istersen: sudo nano $DIR/secrets.env"
else
  echo "    Yapıştırdıkların güvenlik için ekranda GÖRÜNMEZ. Yapıştır ve Enter'a bas."
  read -rsp "    Telegram bot token: " TG_TOKEN </dev/tty; echo
  read -rp  "    Telegram chat id: " TG_CHAT </dev/tty
  read -rsp "    Binance DEMO API key: " BN_KEY </dev/tty; echo
  read -rsp "    Binance DEMO API secret: " BN_SECRET </dev/tty; echo
  umask 077
  cat > "$DIR/secrets.env" <<EOF
TELEGRAM_BOT_TOKEN=$TG_TOKEN
TELEGRAM_CHAT_ID=$TG_CHAT
BINANCE_API_KEY=$BN_KEY
BINANCE_API_SECRET=$BN_SECRET
EOF
  umask 022
fi
chown "$BOT_USER:$BOT_USER" "$DIR/secrets.env"
chmod 600 "$DIR/secrets.env"

# Her çalıştırmada: GitHub'dan güncelle, ayarları yükle, botu çalıştır
cat > "$DIR/run.sh" <<'EOF'
#!/bin/bash
cd /opt/sinyalbot
git -C repo pull --ff-only -q 2>/dev/null || echo "Uyarı: GitHub'dan güncelleme alınamadı, mevcut kodla devam"
if ! cmp -s repo/requirements.txt .requirements.kurulu 2>/dev/null; then
  venv/bin/pip install -q --disable-pip-version-check -r repo/requirements.txt && cp repo/requirements.txt .requirements.kurulu
fi
set -a
. repo/ayarlar.env
. ./secrets.env
set +a
exec venv/bin/python repo/signal_bot.py
EOF
chmod 755 "$DIR/run.sh"
cp "$DIR/repo/requirements.txt" "$DIR/.requirements.kurulu"
chown "$BOT_USER:$BOT_USER" "$DIR/run.sh" "$DIR/.requirements.kurulu"

echo "==> 6/6 Zamanlayıcı (her dakika; coin taraması sadece mum kapanışlarında)"
cat > /etc/systemd/system/sinyalbot.service <<EOF
[Unit]
Description=Sinyal botu (Gate.io sinyal + Binance Futures)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$BOT_USER
WorkingDirectory=$DIR
ExecStart=$DIR/run.sh
TimeoutStartSec=300
EOF

cat > /etc/systemd/system/sinyalbot.timer <<'EOF'
[Unit]
Description=Sinyal botunu her dakika çalıştır (pozisyon takibi; tarama mum kapanışlarında)

[Timer]
OnCalendar=*:*:20
AccuracySec=1s
Unit=sinyalbot.service

[Install]
WantedBy=timers.target
EOF

# Kısa yardımcı komutlar
cat > /usr/local/bin/sinyalbot-test <<'EOF'
#!/bin/bash
# Telegram'a test mesajı (Binance bağlantısı + trendler) gönderir
exec runuser -u sinyalbot -- env TEST_RUN=true /opt/sinyalbot/run.sh
EOF
cat > /usr/local/bin/sinyalbot-log <<'EOF'
#!/bin/bash
# Son çalıştırmaların çıktısı (varsayılan son 80 satır): sinyalbot-log 200
exec journalctl -u sinyalbot.service -n "${1:-80}" --no-pager
EOF
cat > /usr/local/bin/sinyalbot-durum <<'EOF'
#!/bin/bash
systemctl list-timers sinyalbot.timer --no-pager
echo
systemctl status sinyalbot.service --no-pager -n 5 || true
EOF
chmod 755 /usr/local/bin/sinyalbot-test /usr/local/bin/sinyalbot-log /usr/local/bin/sinyalbot-durum

timedatectl set-ntp true 2>/dev/null || true
systemctl daemon-reload
systemctl enable --now sinyalbot.timer >/dev/null
systemctl restart sinyalbot.timer

echo
echo "==> Kurulum tamam. Şimdi test mesajı gönderiliyor..."
echo
sinyalbot-test || true
echo
echo "Telegram'a test mesajı gelmiş olmalı."
echo "Faydalı komutlar:"
echo "  sinyalbot-test    -> test mesajı gönder"
echo "  sinyalbot-log     -> son çalıştırmaların çıktısı"
echo "  sinyalbot-durum   -> bir sonraki çalışma zamanı"
