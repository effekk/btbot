"""
ayarlar.env tetikleyicisi: IFVG_BT değeri değişince backtest'i AYRI bir süreçte başlatır.

signal_bot.py her dakika en başta  `import bt_tetik; bt_tetik.kontrol()`  çağırır.
Değer biçimi:  IFVG_BT=<mod>-<sayı>   (mod: grafik | gelistirme | dogrulama)
  Örn. IFVG_BT=grafik-1 ; aynı modu tekrar çalıştırmak için sayıyı artır (grafik-2).
Backtest systemd-run ile ayrı birim olarak başlar; botun kilidini tutmaz, bot dakikalık
çalışmasına devam eder. Bir backtest sürerken yeni değer gelirse, o bitince başlar.
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent
KOK = REPO.parent
CIKTI = KOK / "bt_calisma" / "ifvg_5m"
MODLAR = ("grafik", "gelistirme", "dogrulama")


def _telegram(metin):
    try:
        import requests
        tok, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
        if tok and chat:
            requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          data={"chat_id": chat, "text": metin}, timeout=15)
    except Exception:
        pass


def _oku(yol):
    try:
        return json.loads(yol.read_text())
    except Exception:
        return {}


def _yasiyor(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def kontrol():
    try:
        _kontrol()
    except Exception as ex:
        print("bt_tetik hata:", ex)


def _kontrol():
    CIKTI.mkdir(parents=True, exist_ok=True)
    durum_yol, tetik_yol = CIKTI / "durum.json", CIKTI / "tetik_durum.json"
    durum, tetik = _oku(durum_yol), _oku(tetik_yol)

    # çalışan backtest beklenmedik şekilde öldüyse ya da hiç başlamadıysa bir kez bildir
    calisiyor = durum.get("durum") == "calisiyor" and _yasiyor(durum.get("pid", -1))
    gecen = time.time() - tetik.get("baslatma_zamani", 0)
    if (durum.get("durum") == "calisiyor" and not calisiyor) or (durum.get("durum") == "baslatildi" and gecen > 600):
        durum["durum"] = "coktu"
        durum_yol.write_text(json.dumps(durum, ensure_ascii=False, indent=1))
        _telegram(f"⚠️ ifvg_5m {durum.get('mod')} backtest beklenmedik şekilde durdu ya da başlamadı "
                  f"(bellek yetmemiş olabilir). Log: bt_calisma/ifvg_5m/")
    deger = os.environ.get("IFVG_BT", "").strip()
    if not deger or deger.lower() == "yok" or deger == tetik.get("son_deger"):
        return
    if calisiyor or (durum.get("durum") == "baslatildi" and gecen < 600):
        return                                 # önceki iş bitince başlar
    mod = deger.split("-")[0].strip().lower()
    tetik["son_deger"] = deger
    if mod not in MODLAR:
        tetik_yol.write_text(json.dumps(tetik, ensure_ascii=False, indent=1))
        _telegram(f"IFVG_BT={deger} tanınmadı. Geçerli: grafik-1, gelistirme-1, dogrulama-1")
        return
    betik = REPO / "bt_ifvg_calistir.py"
    birim = f"ifvg-bt-{mod}-{int(time.time())}"
    komut = ["systemd-run", f"--unit={birim}", "--collect", "--quiet", "--nice=10",
             "-p", "MemoryMax=700M", f"--working-directory={KOK}", sys.executable, str(betik), mod]
    yontem = "systemd-run"
    try:
        r = subprocess.run(komut, capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            raise RuntimeError(r.stderr.strip()[:300])
    except Exception as ex:
        yontem = f"yedek (systemd-run olmadı: {ex})"
        log = open(CIKTI / "arka_plan.log", "a")
        subprocess.Popen([sys.executable, str(betik), mod], cwd=str(KOK), stdout=log, stderr=log,
                         stdin=subprocess.DEVNULL, start_new_session=True, close_fds=True)
    tetik["baslatma_zamani"] = time.time()
    tetik["mod"] = mod
    tetik_yol.write_text(json.dumps(tetik, ensure_ascii=False, indent=1))
    durum_yol.write_text(json.dumps({"mod": mod, "durum": "baslatildi"}, ensure_ascii=False))
    _telegram(f"▶️ ifvg_5m backtest başladı: {mod} ({yontem}). Bitince sonuç buraya gelecek.")
