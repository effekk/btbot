"""Telegram gönderimi. Token ve chat id ortam değişkenlerinden (secrets.env) okunur."""
import json
import os
from pathlib import Path

import requests


def _api(yontem):
    return f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}/{yontem}"


def hazir():
    return bool(os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"))


def mesaj(metin):
    if not hazir():
        print(metin)
        return
    parcalar, buf = [], ""
    for satir in metin.split("\n"):
        if len(buf) + len(satir) + 1 > 3900:
            parcalar.append(buf)
            buf = ""
        buf += satir + "\n"
    if buf.strip():
        parcalar.append(buf)
    for p in parcalar:
        try:
            requests.post(_api("sendMessage"), data={"chat_id": os.environ["TELEGRAM_CHAT_ID"], "text": p},
                          timeout=30)
        except Exception as ex:
            print("telegram mesaj hatası:", ex)


def dosya(yol, aciklama=""):
    if not hazir():
        print("dosya:", yol)
        return
    try:
        with open(yol, "rb") as f:
            requests.post(_api("sendDocument"),
                          data={"chat_id": os.environ["TELEGRAM_CHAT_ID"], "caption": aciklama[:1000]},
                          files={"document": (Path(yol).name, f)}, timeout=120)
    except Exception as ex:
        print("telegram dosya hatası:", ex)


def resimler(yollar, aciklama=""):
    """En fazla 10'arlı gruplar halinde fotoğraf gönderir."""
    if not hazir():
        print("resimler:", yollar)
        return
    for g in range(0, len(yollar), 10):
        grup = yollar[g:g + 10]
        medya, dosyalar = [], {}
        for j, y in enumerate(grup):
            ad = f"r{j}"
            medya.append({"type": "photo", "media": f"attach://{ad}",
                          **({"caption": aciklama} if (g == 0 and j == 0 and aciklama) else {})})
            dosyalar[ad] = (Path(y).name, open(y, "rb"))
        try:
            requests.post(_api("sendMediaGroup"),
                          data={"chat_id": os.environ["TELEGRAM_CHAT_ID"], "media": json.dumps(medya)},
                          files=dosyalar, timeout=180)
        except Exception as ex:
            print("telegram resim hatası:", ex)
        finally:
            for _, f in dosyalar.values():
                f.close()
