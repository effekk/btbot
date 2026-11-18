#!/bin/bash
# =========================================================================
# Bir kerelik sunucu ayarı.
# Sorun: Bot dakikada bir "oneshot" servis olarak çalışıyor. Servis bitince
# systemd, botun arka planda başlattığı backtest sürecini de öldürüyor.
# Çözüm: servis bittiğinde sadece ana süreci kapat, arka plandakilere dokunma
# (KillMode=process). Mevcut servis dosyası DEĞİŞMEZ; yanına ayrı bir ek
# dosya (drop-in) yazılır. Geri almak için o ek dosyayı silmek yeterli.
#
# Sunucuda root olarak çalıştır:
#   bash /opt/sinyalbot/repo/sunucu_ayar.sh
# =========================================================================
set -e

if [ "$(id -u)" -ne 0 ]; then
  echo "Bu script root olarak çalışmalı."
  exit 1
fi

KLASOR=/etc/systemd/system/sinyalbot.service.d
DOSYA=$KLASOR/arka-plan.conf

mkdir -p "$KLASOR"
cat > "$DOSYA" <<'EOF'
[Service]
KillMode=process
EOF

systemctl daemon-reload

echo "Tamam. Yeni ayar:"
systemctl show sinyalbot.service -p KillMode
