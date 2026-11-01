"""
Backtest: sinyal botunun bir stratejisini (strateji_<kod>.py) geçmiş veride işletir.

BAŞLATMA (SSH gerekmez) – ayarlar.env:
  BACKTEST_ISTEGI   – değişince yeni backtest başlar (1, 2, 3 ...)
  BT_STRATEJI       – test edilecek strateji kodu (boşsa STRATEJILER'in ilki)
  BACKTEST_VERI     – boş: kripto (Binance USDⓈ-M Futures geçmiş verisi); dukascopy: kripto dışı 10 piyasa
  DOGRULAMA_GOSTER  – false/tanımsız: doğrulama dönemi hiç hesaplanmaz; true: hesaplanır ve gösterilir
  BT_STRATEJI, BACKTEST_VERI ya da DOGRULAMA_GOSTER değişince de yeni backtest başlar.
Canlı bot her dakika kendi işini bitirdikten sonra, bir sonraki dakikaya kadar kalan birkaç saniyede
backtest bir adım ilerler; canlı bot hiç beklemez. Bitince rapor ve CSV dosyaları Telegram'a gelir.

Strateji (signal_bot.load_strategy ile yüklenir) – birebir canlıdaki fonksiyonlar:
  - Hazırlık (gerçek DataFrame): signal_bot.compute_indicators(df), ardından strat.hazirla(df).
  - Üst zaman dilimi trendi: signal_bot.market_state(df, idx) (UST_ZAMAN_DILIMI None ise None).
  - Giriş: signal_bot.check_alert(strat, df, ust_trend), 200 mumluk pencere, son kapanmış mum = len-2.
  - Çıkış: strat.CIKIS (stop_atr / stop(), tp_r, yari_kar, sure_mum, iz) – aşağıda _islem.
Varsayımlar:
  - Giriş sinyal mumundan sonraki mumun açılışında; çıkışlar stratejinin zaman dilimindeki mumlarla işletilir.
  - Stop mum içinde değerse stop fiyatından (mum stopun ötesinde açıldıysa açılıştan). Aynı mumda stop ve
    başka bir çıkış oluşursa önce stop. TP / yarı kâr mum içinde; iz çıkışı mum kapanışında; süre sınırı ve
    SL'yi girişe çekme (yarı kârdan sonra) bir sonraki mumun açılışında.
  - Kripto: komisyon taker %0,05 (her dolumda), funding kaba tahmin 8 saatte %0,01 (ayrı gösterilir), kayma yok.
  - Geliştirme dönemi sabit (GELISTIRME); doğrulama dönemi ondan sonraki aylar. Geliştirme işlemi doğrulama
    başlarken açıksa o mumun açılışından kapatılır (DÖNEM), böylece doğrulama açık da kapalı da olsa sonuç aynı.

Çalışma klasörü: /opt/sinyalbot/bt_calisma (repo dışında; indirilen veri burada saklanır).
"""
import concurrent.futures
import contextlib
import csv
import datetime
import io
import json
import math
import os
import shutil
import time
import urllib.error
import urllib.request
import zipfile

import numpy as np
import pandas as pd

import binance_trader as bt
import rapor
import signal_bot as sb

# =========================================================================
# AYARLAR
# =========================================================================
ISTEK_DEGISKENI = "BACKTEST_ISTEGI"
CALISMA = "bt_calisma"
VERI = os.path.join(CALISMA, "veri")
SINYAL = os.path.join(CALISMA, "sinyaller")
SONUC = os.path.join(CALISMA, "sonuclar")
DURUM = os.path.join(CALISMA, "durum.json")
DOGRULAMA_KAYDI = os.path.join(CALISMA, "dogrulama_kaydi.json")

GELISTIRME = ("2023-10", "2026-05")   # sabit geliştirme dönemi; sonrası doğrulama
ISINMA_AY = 8                 # indikatörler için geliştirme başından önce indirilen ay (1d 200 mum ≈ 7 ay)
KOMISYON = 0.0005             # taker %0,05
FUNDING_8S = 0.0001           # kaba funding tahmini: 8 saatte pozisyon büyüklüğünün %0,01'i (maliyet sayılır)
UFUK_MUM = 16                 # çıkıştan bağımsız MFE/MAE: girişten sonraki 16 mum (stratejinin zaman diliminde)
# Giriş anındaki bağlam bilgileri (işlem kayıtlarına ve CSV'ye yazılır)
BAGLAM = ["risk_atr", "stop_pct", "trend_guc", "ema26_uzaklik", "rsi", "atr_pct",
          "htf_trend", "htf_guc", "btc_trend", "btc_htf_trend"]

SURE_SONU = 48                # süreç başladıktan bu kadar saniye sonra backtest bu dakikalık işini bırakır
MIN_KALAN = 10                # yeni bir iş birimine başlamak için gereken en az süre (sn)
INDIRME_ESZAMANLI = 6
VERI_URL = "https://data.binance.vision/data/futures/um/monthly/klines"
ONEK = {"PEPE": "1000PEPE", "SHIB": "1000SHIB", "BONK": "1000BONK", "FLOKI": "1000FLOKI"}

# Önceden kabul edilen karar kuralları (doğrulama dönemi; kripto dışı testte geliştirme dönemi)
KARAR_ORT_R = 0.10
KARAR_PF = 1.2
KARAR_KARLI_CEYREK = 0.60

# Önceden kayıtlı test planları (strateji koduna göre). Listede olmayan stratejiler GELISTIRME ile test edilir.
PLANLAR = {
    "sikisma_15m": {
        "tip": "varyant",                    # V0 / V1 / V2 / RASTGELE – aşağıdaki VARYANT TESTİ bölümü
        "veri": ("2021-01", "2026-10"),      # 15m ve 1h
        "gelistirme": ("2021-01", "2026-06"),
        "dogrulama": ("2026-07", "2026-10"),
        "isinma_mum": 216,                   # her coin ilk 216 mumu ısınma olarak geçirir (200 pencere + 16 kutu)
        "varyantlar": ["V0", "V1", "V2"],
        "retest_mum": 4,                     # V2: limit emir en fazla 4 mum bekler
        "maker": 0.0002, "taker": 0.0005,
        "kayma": {"BTCUSDT": 0.0002, "ETHUSDT": 0.0002, "diger": 0.0005},
        "funding_8s": 0.0001,                # 00/08/16 UTC'yi geçen pozisyonlarda
        "rastgele_tekrar": 200, "rastgele_tohum": 20261101,
        "gunluk_islem": 8, "gunluk_zarar_r": -6.0,
        "sahte_kirilim_mum": 4,
    },
}
# sikisma_1h: istek 9 ile aynı düzen; 1h sinyal + 4h üst zaman dilimi, V2 yok
PLANLAR["sikisma_1h"] = dict(PLANLAR["sikisma_15m"], varyantlar=["V0", "V1"])

SEC = dict(sb.INTERVAL_SECONDS)
_IMPORT = time.time()


# =========================================================================
# Zaman bütçesi (canlı botu bekletmemek için)
# =========================================================================
def _surec_yasi():
    """Bu çalıştırmanın (run.sh) başlangıcından beri geçen saniye."""
    try:
        with open("/proc/self/stat") as f:
            alanlar = f.read().rsplit(")", 1)[1].split()
        with open("/proc/uptime") as f:
            uptime = float(f.read().split()[0])
        return uptime - int(alanlar[19]) / os.sysconf("SC_CLK_TCK")
    except Exception:
        return time.time() - _IMPORT + 30


def _kalan():
    return SURE_SONU - _surec_yasi()


# =========================================================================
# Yardımcılar
# =========================================================================
def _json_oku(yol, varsayilan=None):
    try:
        with open(yol) as f:
            return json.load(f)
    except Exception:
        return varsayilan


def _json_yaz(yol, veri):
    tmp = yol + ".tmp"
    with open(tmp, "w") as f:
        json.dump(veri, f, ensure_ascii=False)
    os.replace(tmp, yol)


def _ay_kaydir(y, m, k):
    t = y * 12 + (m - 1) + k
    return t // 12, t % 12 + 1


def _aylar(bas, son):
    """'YYYY-MM' bas..son (dahil) ay listesi."""
    out = []
    y, m = map(int, bas.split("-"))
    while f"{y}-{m:02d}" <= son:
        out.append(f"{y}-{m:02d}")
        y, m = _ay_kaydir(y, m, 1)
    return out


def _ay_bas_ts(ay):
    y, m = map(int, ay.split("-"))
    return int(datetime.datetime(y, m, 1, tzinfo=datetime.timezone.utc).timestamp())


def _ts_ay(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m")


def _ts_str(ts):
    return datetime.datetime.fromtimestamp(ts, rapor.TZ).strftime("%Y-%m-%d %H:%M")


def _var_mi(url):
    try:
        req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "sinyalbot-backtest"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status == 200
    except Exception:
        return False


def _bsym(gate_symbol):
    base, quote = gate_symbol.split("_")
    return ONEK.get(base, base) + quote


def _f2(v):
    return "–" if v is None else (f"{v:.2f}".replace(".", ",") if math.isfinite(v) else "∞")


def _usd(v):
    return rapor._usd(v)


def _r(v):
    return rapor._r(v)


def _pct(v):
    return rapor._pct(v)


def _sayi(x, basamak=3):
    x = float(x)
    return round(x, basamak) if math.isfinite(x) else None


def _sure(dk):
    return f"{dk / 1440:.1f} gün".replace(".", ",") if dk >= 2880 else f"{dk / 60:.0f} saat"


class _Pencere:
    """check_alert / market_state'in beklediği df["kolon"][i] erişimini numpy dilimleriyle sağlar."""

    def __init__(self, kolonlar, a, b):
        self.k = {ad: v[a:b] for ad, v in kolonlar.items()}
        self.n = b - a

    def __getitem__(self, ad):
        return self.k[ad]

    def __len__(self):
        return self.n


# =========================================================================
# Telegram (uzun mesaj bölme + dosya gönderme)
# =========================================================================
def _gonder(notify, metin):
    parca = []
    uzunluk = 0
    for satir in metin.split("\n"):
        if uzunluk + len(satir) + 1 > 3800 and parca:
            notify("\n".join(parca))
            parca, uzunluk = [], 0
        parca.append(satir)
        uzunluk += len(satir) + 1
    if parca:
        notify("\n".join(parca))


def _belge_gonder(yol, aciklama=""):
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat:
        print(f"[Telegram kapalı] dosya: {yol}")
        return
    sinir = "----btsinir" + str(int(time.time() * 1000))
    with open(yol, "rb") as f:
        icerik = f.read()
    govde = io.BytesIO()
    for ad, deger in (("chat_id", chat), ("caption", aciklama[:1000])):
        govde.write(f"--{sinir}\r\nContent-Disposition: form-data; name=\"{ad}\"\r\n\r\n{deger}\r\n".encode())
    govde.write(f"--{sinir}\r\nContent-Disposition: form-data; name=\"document\"; "
                f"filename=\"{os.path.basename(yol)}\"\r\nContent-Type: application/octet-stream\r\n\r\n".encode())
    govde.write(icerik)
    govde.write(f"\r\n--{sinir}--\r\n".encode())
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendDocument", data=govde.getvalue(),
                                 headers={"Content-Type": f"multipart/form-data; boundary={sinir}"})
    try:
        urllib.request.urlopen(req, timeout=60).read()
    except Exception as e:
        print(f"Telegram dosyası gönderilemedi ({yol}): {e}")


# =========================================================================
# Ayarlar ve iş akışı
# =========================================================================
_NOTIFY = [print]          # rapor aşaması Telegram için bunu kullanır


def _env(ad):
    return os.environ.get(ad, "").strip().strip('"').strip("'").strip()


def _strateji_kodu():
    return (_env("BT_STRATEJI") or (sb.STRATEJILER[0] if sb.STRATEJILER else "")).lower()


def _veri_turu():
    """ayarlar.env BACKTEST_VERI: boş = kripto (Binance), dukascopy = kripto dışı 10 piyasa."""
    return _env("BACKTEST_VERI").lower() or "kripto"


def _dogrulama_ayari():
    """DOGRULAMA_GOSTER=true ise son 4 ay hesaplanır ve gösterilir. Tanımsızsa KAPALI (yanlışlıkla bakılmasın)."""
    return sb._flag("DOGRULAMA_GOSTER")


def _dosya_adi(x):
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in x)


def _strateji(d):
    return sb.load_strategy(d["strateji"])


def _strateji_ozeti(strat):
    c = strat.CIKIS
    parca = [f"stop {'strateji stop()' if hasattr(strat, 'stop') else str(c.get('stop_atr')) + ' ATR'}",
             f"TP {c['tp_r']}R" if c.get("tp_r") else "TP yok",
             f"yarı kâr {c['yari_kar']}" if c.get("yari_kar") and c.get("tp_r") else "yarı kâr yok",
             f"süre {c['sure_mum']} mum" if c.get("sure_mum") else "süre yok",
             "iz çıkışı" if c.get("iz") else "iz yok"]
    ust = getattr(strat, "UST_ZAMAN_DILIMI", None)
    return (f"{strat.KOD} – {strat.AD} | yön {strat.YON} | {strat.ZAMAN_DILIMI}"
            + (f" (üst {ust})" if ust else "") + " | çıkış: " + ", ".join(parca))


def adim(notify):
    _NOTIFY[0] = notify
    istek = _env(ISTEK_DEGISKENI)
    if not istek or _kalan() < MIN_KALAN:
        return
    os.makedirs(CALISMA, exist_ok=True)
    d = _json_oku(DURUM, {}) or {}
    kod, veri, dogr = _strateji_kodu(), _veri_turu(), _dogrulama_ayari()
    # Eski sürümün durum dosyasında bu alanlar yok: o durumda sadece istek değişince başlar
    if (d.get("istek") != istek or d.get("strateji", kod) != kod or d.get("veri", veri) != veri
            or d.get("dogrulama", dogr) != dogr):
        try:
            d = _yeni_is(istek, kod, veri, dogr, notify)
        except Exception as e:
            notify(f"❌ Backtest ({istek}) başlatılamadı: {e}")
            d = {"istek": istek, "strateji": kod, "veri": veri, "dogrulama": dogr, "asama": "bitti"}
        _json_yaz(DURUM, d)
    _AKTIF["aylar"] = d.get("indir_aylar")
    if d.get("asama") == "bitti":
        return
    try:
        os.nice(10)                      # canlı işler bitti; bundan sonrası düşük öncelikle
    except Exception:
        pass
    while _kalan() >= MIN_KALAN and d["asama"] != "bitti":
        devam = _bir_birim(d, notify)
        _json_yaz(DURUM, d)
        if not devam:
            break


def _yeni_is(istek, kod, veri, dogr, notify):
    strat = sb.load_strategy(kod)              # dosya yoksa / alan eksikse burada hata verir
    if veri == "dukascopy":
        return _k_yeni_is(istek, strat, dogr, notify)
    if PLANLAR.get(kod, {}).get("tip") == "varyant":
        return _v_yeni_is(istek, strat, dogr, notify)
    tf = strat.ZAMAN_DILIMI
    simdi = datetime.datetime.now(datetime.timezone.utc)
    son = None
    for geri in (1, 2, 3):
        y, m = _ay_kaydir(simdi.year, simdi.month, -geri)
        if _var_mi(f"{VERI_URL}/BTCUSDT/{tf}/BTCUSDT-{tf}-{y}-{m:02d}.zip"):
            son = f"{y}-{m:02d}"
            break
    if not son:
        raise RuntimeError("data.binance.vision'a ulaşılamadı. Tekrar denemek için "
                           f"{ISTEK_DEGISKENI} değerini değiştir")
    gel = _aylar(*GELISTIRME)
    y, m = map(int, GELISTIRME[1].split("-"))
    dog = _aylar("%d-%02d" % _ay_kaydir(y, m, 1), son)
    y, m = map(int, GELISTIRME[0].split("-"))
    indir = _aylar("%d-%02d" % _ay_kaydir(y, m, -ISINMA_AY), son)
    ust = getattr(strat, "UST_ZAMAN_DILIMI", None)
    zfs = [tf] + ([ust] if ust and ust != tf else [])
    coinler = list(getattr(strat, "COINLER", None) or sb.SYMBOLS)
    if "BTC_USDT" not in coinler:
        coinler.append("BTC_USDT")                 # sadece bağlam (BTC trendi) için indirilir, işlem açılmaz
    d = {
        "istek": istek, "strateji": kod, "veri": veri, "dogrulama": dogr, "asama": "indir",
        "sira": 0, "alt": 0, "baslangic": time.time(), "zfs": zfs,
        "semboller": [[g, _bsym(g)] for g in coinler],
        "islem_sembolleri": [_bsym(g) for g in (getattr(strat, "COINLER", None) or sb.SYMBOLS)],
        "indir_aylar": indir, "ayar_aylar": gel, "dogrulama_aylar": dog,
        "hatalar": [], "deneme": {}, "eksik": {}, "atlanan_coin": [],
    }
    notify("\n".join([
        f"🧪 Backtest başladı (istek {istek})",
        "Strateji: " + _strateji_ozeti(strat),
        f"Veri: Binance USDⓈ-M Futures, {len(d['islem_sembolleri'])} coin, mumlar {'+'.join(zfs)}",
        f"Geliştirme: {gel[0]} – {gel[-1]} ({len(gel)} ay)",
        (f"Doğrulama (tek sefer): {dog[0]} – {dog[-1]} ({len(dog)} ay)" if dogr and dog else
         "Doğrulama: KAPALI (DOGRULAMA_GOSTER=false) – hiç hesaplanmayacak" if dog else "Doğrulama: veri yok"),
        "Canlı bot her dakikadan sonra kalan sürede ilerler. Aşama bitince haber veririm.",
    ]))
    return d


def _birimler(d):
    if d.get("kd"):
        return _k_birimler(d)
    if d.get("vt"):
        return _v_birimler(d)
    ok = [b for _, b in d["semboller"] if b not in d["atlanan_coin"]]
    if d["asama"] == "indir":
        return [(b, tf) for b in ok for tf in d["zfs"]]
    if d["asama"] in ("sinyal", "simule"):
        return [(b,) for b in ok if b in d["islem_sembolleri"]]
    if d["asama"] == "rapor":
        return [("rapor",)]
    return []


def _bir_birim(d, notify):
    birimler = _birimler(d)
    if d["sira"] >= len(birimler):
        _sonraki_asama(d, notify)
        return True
    b = birimler[d["sira"]]
    anahtar = d["asama"] + "|" + "|".join(b)
    try:
        if d.get("kd"):
            islev = {"indir": _k_indir, "sinyal": _k_sinyal, "simule": _k_simule, "rapor": _k_rapor}
        elif d.get("vt"):
            islev = {"indir": _b_indir, "sinyal": _v_sinyal, "simule": _v_simule, "rapor": _v_rapor}
        else:
            islev = {"indir": _b_indir, "sinyal": _b_sinyal, "simule": _b_simule, "rapor": _b_rapor}
        bitti = islev[d["asama"]](d, b)
    except Exception as e:
        n = d["deneme"].get(anahtar, 0) + 1
        d["deneme"][anahtar] = n
        print(f"Backtest hata ({anahtar}, deneme {n}): {e}")
        if n >= 3:
            d["hatalar"].append(f"{anahtar}: {e}"[:300])
            if d.get("kd") and d["asama"] == "indir":
                d.setdefault("indirilemeyen", []).append(f"{b[0]} {b[1]}: {e}"[:200])
            elif d["asama"] == "indir" and b[1] == d["zfs"][0]:
                d["atlanan_coin"].append(b[0])          # ana veri yoksa coin tamamen dışarıda kalır
            elif d["asama"] == "rapor":
                notify(f"❌ Backtest raporu oluşturulamadı: {e}")
                d["asama"] = "bitti"
                return False
            d["sira"] += 1
            d["alt"] = 0
        return False
    if bitti:
        d["sira"] += 1
        d["alt"] = 0
        return True
    return False


def _sonraki_asama(d, notify):
    if d.get("kd"):
        return _k_sonraki_asama(d, notify)
    if d.get("vt"):
        return _v_sonraki_asama(d, notify)
    a = d["asama"]
    gecen = (time.time() - d["baslangic"]) / 60
    if a == "indir":
        coin = len(d["islem_sembolleri"]) - len([b for b in d["atlanan_coin"] if b in d["islem_sembolleri"]])
        satir = [f"📥 Backtest: veri indirildi ({coin}/{len(d['islem_sembolleri'])} coin, {gecen:.0f} dk)"]
        if d["eksik"]:
            satir.append("Eksik aylar: " + "; ".join(f"{k}: {','.join(v)}" for k, v in list(d["eksik"].items())[:8]))
        if d["atlanan_coin"]:
            satir.append("Dışarıda kalan coin: " + ", ".join(d["atlanan_coin"]))
        notify("\n".join(satir))
        d["asama"] = "sinyal"
    elif a == "sinyal":
        say = sum(len((_json_oku(os.path.join(SINYAL, f"{b[0]}.json"), {}) or {}).get("sinyaller", []))
                  for b in _birimler(d))
        notify(f"📶 Backtest: sinyaller hesaplandı – {say} sinyal, {gecen:.0f} dk. Şimdi çıkışlar işletiliyor.")
        d["asama"] = "simule"
    elif a == "simule":
        d["asama"] = "rapor"
    elif a == "rapor":
        d["asama"] = "bitti"
    d["sira"] = 0
    d["alt"] = 0


# =========================================================================
# 1) Veri indirme (data.binance.vision aylık zip)
# =========================================================================
def _indir_ay(bsym, tf, ay):
    url = f"{VERI_URL}/{bsym}/{tf}/{bsym}-{tf}-{ay}.zip"
    req = urllib.request.Request(url, headers={"User-Agent": "sinyalbot-backtest"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            veri = r.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    with zipfile.ZipFile(io.BytesIO(veri)) as z:
        with z.open(z.namelist()[0]) as f:
            df = pd.read_csv(f, header=None, usecols=[0, 1, 2, 3, 4], dtype=str)
    return df.apply(pd.to_numeric, errors="coerce").dropna().to_numpy(float)   # başlık satırı varsa düşer


_AKTIF = {"aylar": None}


def _veri_yolu(bsym, tf):
    aylar = _AKTIF.get("aylar")
    if not aylar:
        return os.path.join(VERI, f"{bsym}_{tf}.npz")
    return os.path.join(VERI, f"{bsym}_{tf}_{aylar[0]}_{aylar[-1]}.npz")


def _mevcut_aylar(bsym, tf, aylar):
    """VERI klasöründeki aynı coin/zaman dilimi dosyalarından istenen ayların satırları (ts ms, o, h, l, c)."""
    out = {}
    try:
        adlar = [a for a in os.listdir(VERI) if a.startswith(f"{bsym}_{tf}") and a.endswith(".npz")]
    except FileNotFoundError:
        return out
    for ad in adlar:
        if ad != f"{bsym}_{tf}.npz" and not ad.startswith(f"{bsym}_{tf}_"):
            continue
        try:
            with np.load(os.path.join(VERI, ad)) as z:
                kapsam = str(z["aylar"]).split(",")
                t, o, h, l, c = z["t"], z["o"], z["h"], z["l"], z["c"]
        except Exception:
            continue
        for ay in aylar:
            if ay in out or ay not in kapsam or ay == kapsam[-1]:
                continue                                   # dosyanın son ayı eksik kaydedilmiş olabilir
            a, b = _ay_bas_ts(ay), _ay_bas_ts("%d-%02d" % _ay_kaydir(*map(int, ay.split("-")), 1))
            m = (t >= a) & (t < b)
            if m.sum():
                out[ay] = np.column_stack([t[m] * 1000.0, o[m], h[m], l[m], c[m]])
    return out


def _b_indir(d, b):
    bsym, tf = b
    os.makedirs(VERI, exist_ok=True)
    yol = _veri_yolu(bsym, tf)
    anahtar = ",".join(d["indir_aylar"])
    eski = os.path.join(VERI, f"{bsym}_{tf}.npz")           # önceki sürümün dosya adı
    if not os.path.exists(yol) and os.path.exists(eski):
        try:
            with np.load(eski) as z:
                ayni = str(z["aylar"]) == anahtar
            if ayni:
                os.replace(eski, yol)
        except Exception:
            pass
    if os.path.exists(yol):
        try:
            with np.load(yol) as z:
                if str(z["aylar"]) == anahtar:
                    return True
        except Exception:
            pass
    if shutil.disk_usage(CALISMA).free < 1.5e9:
        raise RuntimeError("diskte 1,5 GB'tan az yer var")
    aylar = d["indir_aylar"]
    mevcut = _mevcut_aylar(bsym, tf, aylar)          # önceki testlerin dosyalarında olan aylar tekrar indirilmez
    eksik_aylar = [ay for ay in aylar if ay not in mevcut]
    with concurrent.futures.ThreadPoolExecutor(INDIRME_ESZAMANLI) as ex:
        inen = dict(zip(eksik_aylar, ex.map(lambda ay: _indir_ay(bsym, tf, ay), eksik_aylar)))
    sonuc = [mevcut[ay] if ay in mevcut else inen.get(ay) for ay in aylar]
    parcalar = [a for a in sonuc if a is not None and len(a)]
    eksik = [ay for ay, a in zip(aylar, sonuc) if a is None or not len(a)]
    if not parcalar:
        raise RuntimeError("Binance geçmiş verisinde bulunamadı")
    a = np.concatenate(parcalar)
    ts = a[:, 0]
    ts = np.where(ts > 1e14, ts / 1000, ts)                 # mikro saniye → milisaniye
    t = (ts // 1000).astype(np.int64)
    t, idx = np.unique(t, return_index=True)
    a = a[idx]
    tmp = yol + ".tmp"
    with open(tmp, "wb") as f:
        np.savez(f, t=t, o=a[:, 1], h=a[:, 2], l=a[:, 3], c=a[:, 4], aylar=np.array(anahtar))
    os.replace(tmp, yol)
    if eksik:
        d["eksik"][f"{bsym} {tf}"] = eksik
    return True


def _yukle(bsym, tf):
    with np.load(_veri_yolu(bsym, tf)) as z:
        return {k: z[k] for k in ("t", "o", "h", "l", "c")}


def _df(v, strat=None):
    """Canlıdaki fetch_candles ile aynı biçim (datetime TR saati, open/high/low/close) + ortak indikatörler
    + stratejinin hazirla()'sı. Dönüş: (df, kolonlar) – kolonlar _Pencere için numpy dizileri."""
    df = pd.DataFrame({
        "datetime": pd.to_datetime(v["t"], unit="s", utc=True).tz_convert(rapor.TZ).tz_localize(None),
        "open": v["o"], "high": v["h"], "low": v["l"], "close": v["c"],
    })
    sb.compute_indicators(df)
    if strat is not None:
        strat.hazirla(df)
    return df, {c: df[c].to_numpy() for c in df.columns}


def _ust_okuyucu(bsym, tf):
    """T anında son KAPANMIŞ mumun trendi (market_state, 200 mumluk pencere) ve gücü |EMA12-EMA26|/ATR."""
    if not tf:
        return lambda T: (None, None)
    try:
        v = _yukle(bsym, tf)
    except Exception:
        return lambda T: ("VERİ YOK", None)
    _, kol = _df(v)
    t = v["t"]
    sec = SEC[tf]
    onb = {}

    def oku(T):
        j_open = T // sec * sec - sec
        j = int(np.searchsorted(t, j_open))
        if j >= len(t) or t[j] != j_open or j < 199:
            return "VERİ YOK", None                        # filtre uygulanamıyorsa sinyal geçmez
        if j not in onb:
            at = float(kol["atr"][j])
            guc = float(abs(kol["ema_12"][j] - kol["ema_26"][j]) / at) if at > 0 else None
            onb[j] = (sb.market_state(_Pencere(kol, j - 198, j + 2), 198), guc)
        return onb[j]
    return oku


def _baglam(kol, i, side, entry, stop, ust_t, ust_g, btc_t, btc_ht):
    s = 1 if side == "AL" else -1

    def k(ad):
        try:
            x = float(kol[ad][i])
            return x if math.isfinite(x) else None
        except Exception:
            return None
    c, at, e12, e26, rs = k("close"), k("atr"), k("ema_12"), k("ema_26"), k("rsi")
    ok = at is not None and at > 0
    return {
        "risk_atr": _sayi(abs(entry - stop) / at, 2) if ok else None,
        "stop_pct": _sayi(abs(entry - stop) / entry * 100),
        "trend_guc": _sayi(abs(e12 - e26) / at) if ok and e12 is not None and e26 is not None else None,
        "ema26_uzaklik": _sayi(s * (c - e26) / at) if ok and e26 is not None else None,
        "rsi": _sayi(rs, 1) if rs is not None else None,
        "atr_pct": _sayi(at / c * 100) if ok else None,
        "htf_trend": ust_t, "htf_guc": _sayi(ust_g) if ust_g is not None else None,
        "btc_trend": btc_t, "btc_htf_trend": btc_ht,
    }


# =========================================================================
# 2) Sinyaller (canlıdaki check_alert, 200 mumluk pencere)
# =========================================================================
def _b_sinyal(d, b):
    bsym = b[0]
    os.makedirs(SINYAL, exist_ok=True)
    strat = _strateji(d)
    tf = strat.ZAMAN_DILIMI
    ust = getattr(strat, "UST_ZAMAN_DILIMI", None)
    sec = SEC[tf]
    v = _yukle(bsym, tf)
    _, kol = _df(v, strat)
    t = v["t"]
    n = len(t)
    ust_oku = _ust_okuyucu(bsym, ust)
    btc_oku = _ust_okuyucu("BTCUSDT", tf)
    btc_ust_oku = _ust_okuyucu("BTCUSDT", ust)

    bas = _ay_bas_ts(d["ayar_aylar"][0])
    sinir = _ay_bas_ts(d["dogrulama_aylar"][0]) if d["dogrulama_aylar"] else 2 ** 62
    son = sinir if not d.get("dogrulama") else 2 ** 62
    gecici = os.path.join(SINYAL, f"{bsym}.yarim.json")
    liste = (_json_oku(gecici, []) or []) if d["alt"] else []
    i0 = max(199, d["alt"])
    with contextlib.redirect_stdout(io.StringIO()):
        for i in range(i0, n - 1):
            if (i - i0) % 400 == 0 and i > i0 and _kalan() < 3:
                d["alt"] = i
                _json_yaz(gecici, liste)
                return False
            T = int(t[i]) + sec                            # sinyal mumunun kapanışı = giriş anı
            if T < bas or T >= son or int(t[i + 1]) != T:
                continue
            ust_t, ust_g = ust_oku(T) if ust else (None, None)
            a = i - 198                                     # canlıdaki gibi 200 mum: son kapanmış = n-2
            alert = sb.check_alert(strat, _Pencere(kol, a, i + 2), ust_t)
            if not alert:
                continue
            sig = {"T": T, "i": i, "tf": tf, "sym": bsym, "side": alert["side"], "entry": float(alert["entry"]),
                   "stop": float(alert["stop"]),
                   "target": None if alert.get("target") is None else float(alert["target"]),
                   "src": "+".join(alert.get("sources") or [strat.KOD])}
            sig.update(_baglam(kol, i, alert["side"], sig["entry"], sig["stop"], ust_t, ust_g,
                               btc_oku(T)[0], btc_ust_oku(T)[0] if ust else None))
            liste.append(sig)
    _json_yaz(os.path.join(SINYAL, f"{bsym}.json"), {"strateji": strat.KOD, "sinyaller": liste})
    if os.path.exists(gecici):
        os.remove(gecici)
    return True


# =========================================================================
# 3) Çıkışlar – tamamen strat.CIKIS'ten
# =========================================================================
def _islem(sig, strat, v, kol, sinir, maliyet):
    """
    Tek işlem, stratejinin zaman dilimindeki mumlarla. maliyet: ("kripto",) ya da ("kd", spread).
    Sonuçlar R cinsinden (1R = giriş ile stop arası); kriptoda USDT için Binance büyüklük kuralı kullanılır.
    """
    t, O, H, L, C = v["t"], v["o"], v["h"], v["l"], v["c"]
    s = 1 if sig["side"] == "AL" else -1
    sec = SEC[sig["tf"]]
    k0 = int(sig["i"]) + 1
    if k0 >= len(t) or int(t[k0]) != sig["T"]:
        return {"atla": "veri yok"}
    fill = float(O[k0])
    sl = float(sig["stop"])
    if s * (fill - sl) <= 0:
        return {"atla": "fiyat stopun ötesinde açıldı"}
    cik = strat.CIKIS
    tp = sig.get("target")
    if tp is not None and s * (tp - fill) <= 0:
        return {"atla": "fiyat TP'nin ötesinde açıldı"}
    mid = fill + (tp - fill) * cik["yari_kar"] if tp is not None and cik.get("yari_kar") else None
    sure = cik.get("sure_mum")
    iz = bool(cik.get("iz"))
    dist = abs(fill - sl)
    kesim = sinir if sig["T"] < sinir else None     # geliştirme işlemi doğrulama dönemine taşmaz
    kd = maliyet[0] == "kd"
    payi = bt.PARTIAL_SIZE

    parcalar = []                                   # (oran, fiyat, zaman)
    kalan = 1.0
    yarim = be_bekliyor = be = False
    mfe = mae = 0.0
    k = k0
    while True:
        tk = int(t[k])
        if kesim is not None and tk >= kesim:
            parcalar.append((kalan, float(O[k]), tk + (0 if kd else 10)))
            cikis = "DÖNEM"
            break
        op, hi, lo, cl = float(O[k]), float(H[k]), float(L[k]), float(C[k])
        # Bir önceki mumda yarı kâr alındıysa SL girişe çekilir; süre sınırı (mum açılışında)
        if k > k0:
            if be_bekliyor:
                be_bekliyor, be = False, True
                if s * (op - fill) <= 0:                # SL girişe konamaz → kalan açılıştan kapanır
                    parcalar.append((kalan, op, tk + 10))
                    cikis = "GİRİŞ"
                    break
                sl = fill
            elif sure and not (yarim and not bt.AFTER_PARTIAL_MULT):
                if k - k0 >= sure * (bt.AFTER_PARTIAL_MULT if yarim else 1):
                    parcalar.append((kalan, op, tk + 10))
                    cikis = "SÜRE"
                    break
        # Stop önce
        if (s == 1 and lo <= sl) or (s == -1 and hi >= sl):
            bosluk = s * (op - sl) <= 0
            px = op if bosluk else sl
            parcalar.append((kalan, px, tk + (10 if bosluk else sec // 2)))
            mae = max(mae, abs(px - fill) / dist) if not be else mae
            cikis = "GİRİŞ" if be else "SL"
            break
        mfe = max(mfe, ((hi - fill) if s == 1 else (fill - lo)) / dist)
        mae = max(mae, ((fill - lo) if s == 1 else (hi - fill)) / dist)
        if mid is not None and not yarim and ((s == 1 and hi >= mid) or (s == -1 and lo <= mid)):
            parcalar.append((payi, op if s * (op - mid) >= 0 else mid, tk + sec // 2))
            kalan -= payi
            yarim = be_bekliyor = True
        if tp is not None and ((s == 1 and hi >= tp) or (s == -1 and lo <= tp)):
            parcalar.append((kalan, op if s * (op - tp) >= 0 else tp, tk + sec // 2))
            cikis = "TP"
            break
        if iz:
            a = max(0, k - 198)
            if strat.iz_cikis(_Pencere(kol, a, k + 2), k - a, sig["side"]):
                parcalar.append((kalan, cl, tk + sec + 10))
                cikis = "İZ"
                break
        if k >= len(t) - 1:
            parcalar.append((kalan, cl, tk + sec + 10))
            cikis = "SON"
            break
        k += 1

    T = sig["T"]
    cikis_t = parcalar[-1][2]
    r_brut = sum(o * s * (px - fill) for o, px, _ in parcalar) / dist
    sonuc = {"sym": sig["sym"], "tf": sig["tf"], "side": sig["side"], "T": T, "cikis_t": cikis_t, "cikis": cikis,
             "kismi": yarim, "giris_px": fill, "cikis_px": parcalar[-1][1], "stop": float(sig["stop"]),
             "mfe": round(mfe, 3), "mae": round(mae, 3), "dk": int((cikis_t - T) // 60), "src": sig["src"]}
    if kd:
        r_spread = maliyet[1] / dist
        geceler = [int(zt // 86400 - T // 86400) for _, _, zt in parcalar]
        r_swap = sum(o * g for (o, _, _), g in zip(parcalar, geceler)) * KD_SWAP_YILLIK / 365 * fill / dist
        r = r_brut - r_spread - r_swap
        sonuc.update({"r_brut": round(r_brut, 4), "r_spread": round(r_spread, 4), "r_swap": round(r_swap, 4),
                      "r": round(r, 4), "net": round(r * KD_R_USDT, 2), "geceler": max(geceler),
                      "stop_pct": round(dist / fill * 100, 3)})
        return sonuc
    # Kripto: open_trade ile aynı büyüklük (kaldıraç sınırı) – R'yi değiştirmez, USDT karşılığını belirler
    notional = bt.MARGIN_USDT * bt.LOSS_PCT_OF_MARGIN / (dist / fill)
    lev = max(1, math.ceil(notional / bt.MARGIN_USDT))
    if lev > bt.MAX_LEVERAGE:
        lev = bt.MAX_LEVERAGE
        notional = bt.MARGIN_USDT * lev
    risk = notional / fill * dist
    kom_r = sum(o * (fill + px) for o, px, _ in parcalar) * KOMISYON / dist
    fund_r = sum(o * (zt - T) / 3600 for o, _, zt in parcalar) / 8 * FUNDING_8S * fill / dist
    r = r_brut - kom_r
    sonuc.update({"net": round(r * risk, 2), "brut": round(r_brut * risk, 2), "kom": round(kom_r * risk, 2),
                  "fund": round(fund_r * risk, 2), "risk": round(risk, 2), "r": round(r, 4),
                  "r_brut": round(r_brut, 4), "kom_r": round(kom_r, 4), "r_fund": round(r - fund_r, 4), "lev": lev})
    return sonuc


def _ufuk(sig, v):
    """Çıkıştan bağımsız: girişten sonraki UFUK_MUM mumda MFE/MAE (R) ve önce +1R mi −1R mi."""
    k0 = int(sig["i"]) + 1
    if k0 >= len(v["t"]):
        return {}
    fill = float(v["o"][k0])
    dist = abs(fill - sig["stop"])
    if dist <= 0:
        return {}
    h, l = v["h"][k0:k0 + UFUK_MUM], v["l"][k0:k0 + UFUK_MUM]
    if sig["side"] == "AL":
        leh, ale = (h - fill) / dist, (fill - l) / dist
    else:
        leh, ale = (fill - l) / dist, (h - fill) / dist
    kar, zar = np.flatnonzero(leh >= 1), np.flatnonzero(ale >= 1)
    if not len(kar) and not len(zar):
        ilk = "yok"
    elif not len(zar) or (len(kar) and kar[0] < zar[0]):
        ilk = "kâr"
    else:
        ilk = "zarar"
    return {"mfe16": round(float(leh.max()), 3), "mae16": round(float(ale.max()), 3), "ilk1r": ilk}


def _b_simule(d, b):
    bsym = b[0]
    os.makedirs(SONUC, exist_ok=True)
    strat = _strateji(d)
    sinyaller = (_json_oku(os.path.join(SINYAL, f"{bsym}.json"), {}) or {}).get("sinyaller", [])
    hedef = os.path.join(SONUC, f"{bsym}.json")
    gecici = hedef + ".yarim"
    sonuc = (_json_oku(gecici, []) or []) if d["alt"] else []
    sinir = _ay_bas_ts(d["dogrulama_aylar"][0]) if d["dogrulama_aylar"] else 2 ** 62
    if sinyaller:
        v = _yukle(bsym, strat.ZAMAN_DILIMI)
        _, kol = _df(v, strat)
        with contextlib.redirect_stdout(io.StringIO()):
            for j in range(d["alt"], len(sinyaller)):
                if j > d["alt"] and j % 10 == 0 and _kalan() < 3:
                    d["alt"] = j
                    _json_yaz(gecici, sonuc)
                    return False
                sig = sinyaller[j]
                r = _islem(sig, strat, v, kol, sinir, ("kripto",))
                if "atla" in r:
                    r.update({"sym": bsym, "tf": sig["tf"], "T": sig["T"], "side": sig["side"], "src": sig["src"]})
                r.update({k: sig.get(k) for k in BAGLAM})
                r.update(_ufuk(sig, v))
                sonuc.append(r)
    _json_yaz(hedef, sonuc)
    if os.path.exists(gecici):
        os.remove(gecici)
    return True


# =========================================================================
# 4) Portföy sınırları ve istatistik
# =========================================================================
def _portfoy(islemler, sym_sira, tf_sira):
    """Canlıdaki sıra: aynı anda kapanan mumlarda önce 15m sonra 1h, coinler SYMBOLS sırasıyla."""
    sirali = sorted(islemler, key=lambda x: (x["T"], tf_sira.get(x["tf"], 9), sym_sira.get(x["sym"], 99)))
    acik, alinan = [], []
    atl = {"aynı coin": 0, "pozisyon sınırı": 0, "yön sınırı": 0}
    for x in sirali:
        an = x["T"] + 20
        acik = [a for a in acik if a["cikis_t"] > an]
        if any(a["sym"] == x["sym"] for a in acik):
            atl["aynı coin"] += 1
        elif len(acik) >= bt.MAX_OPEN_POSITIONS:
            atl["pozisyon sınırı"] += 1
        elif bt.MAX_SAME_DIRECTION and sum(a["side"] == x["side"] for a in acik) >= bt.MAX_SAME_DIRECTION:
            atl["yön sınırı"] += 1
        else:
            acik.append(x)
            alinan.append(x)
    return alinan, atl


def _dd(degerler):
    eq = np.concatenate([[0.0], np.cumsum(degerler)])
    return float((np.maximum.accumulate(eq) - eq).max())


def _ist(tr, aylar):
    s = {"n": len(tr)}
    if not tr:
        return s
    r = np.array([x["r"] for x in tr])
    net = np.array([x["net"] for x in tr])
    s["ort_r"] = float(r.mean())
    rng = np.random.default_rng(7)
    ort = np.concatenate([r[rng.integers(0, len(r), (200, len(r)))].mean(1) for _ in range(10)])
    s["ga"] = (float(np.percentile(ort, 2.5)), float(np.percentile(ort, 97.5)))
    kayip = net[net <= 0].sum()
    s["pf"] = float(net[net > 0].sum() / -kayip) if kayip < 0 else float("inf")
    s["kazanma"] = float((net > 0).mean())
    s["net"] = float(net.sum())
    brut = np.array([x["brut"] for x in tr])
    s["r_brut"] = float(np.mean([x["r_brut"] for x in tr]))
    s["kom_r"] = float(np.mean([x["kom_r"] for x in tr]))
    s["kom_usdt"] = float(np.sum([x["kom"] for x in tr]))
    bk = brut[brut <= 0].sum()
    s["pf_brut"] = float(brut[brut > 0].sum() / -bk) if bk < 0 else float("inf")
    s["kazanma_brut"] = float((brut > 0).mean())
    kap = sorted(tr, key=lambda x: x["cikis_t"])
    s["dd"] = _dd([x["net"] for x in kap])
    s["dd_r"] = _dd([x["r"] for x in kap])
    seri = en = 0
    for x in kap:
        seri = seri + 1 if x["net"] <= 0 else 0
        en = max(en, seri)
    s["seri"] = en
    ay = {a: [] for a in aylar}
    for x in tr:
        ay.setdefault(_ts_ay(x["T"]), []).append(x)
    s["aylar"] = [(a, len(v), sum(x["net"] for x in v), (sum(x["r"] for x in v) / len(v)) if v else None)
                  for a, v in ay.items()]
    s["karli_ay"] = sum(1 for _, _, n_, _ in s["aylar"] if n_ > 0)
    s["ay_say"] = len(s["aylar"])
    cik = {}
    for x in tr:
        cik[x["cikis"]] = cik.get(x["cikis"], 0) + 1
    s["cikis"] = cik
    s["kismi"] = sum(1 for x in tr if x["kismi"]) / len(tr)
    s["ort_dk"] = float(np.mean([x["dk"] for x in tr]))
    s["medyan_dk"] = float(np.median([x["dk"] for x in tr]))
    # Kazancın yoğunlaşması: en büyük 5 kazanç
    kaz = sorted((v for v in net if v > 0), reverse=True)
    s["top5"] = float(sum(kaz[:5]))
    s["top5_kazanc"] = s["top5"] / sum(kaz) if kaz else None
    s["top5_net"] = s["top5"] / s["net"] if s["net"] > 0 else None
    s["net_top5siz"] = s["net"] - s["top5"]
    # Funding tahmini (sadece trend işlemlerinde hesaplanıyor)
    if all("fund" in x for x in tr):
        s["fund_usdt"] = float(sum(x["fund"] for x in tr))
        s["r_fund"] = float(np.mean([x["r_fund"] for x in tr]))
        nf = net - np.array([x["fund"] for x in tr])
        fk = nf[nf <= 0].sum()
        s["pf_fund"] = float(nf[nf > 0].sum() / -fk) if fk < 0 else float("inf")
    # Çeyrekler (takvim çeyreği, girişe göre)
    cey = {}
    for a in aylar:
        y, m = map(int, a.split("-"))
        cey.setdefault(f"{y}-Ç{(m - 1) // 3 + 1}", [])
    for x in tr:
        y, m = map(int, _ts_ay(x["T"]).split("-"))
        cey.setdefault(f"{y}-Ç{(m - 1) // 3 + 1}", []).append(x)
    s["ceyrekler"] = [(c, len(v), sum(x["net"] for x in v), (sum(x["r"] for x in v) / len(v)) if v else None)
                      for c, v in cey.items()]
    s["karli_ceyrek"] = sum(1 for _, _, n_, _ in s["ceyrekler"] if n_ > 0)
    s["ceyrek_say"] = len(s["ceyrekler"])
    return s


def _kisa(ad, tr):
    if not tr:
        return f"  {ad}: işlem yok"
    r = np.array([x["r"] for x in tr])
    net = np.array([x["net"] for x in tr])
    brut = np.array([x["brut"] for x in tr])
    kayip, bk = net[net <= 0].sum(), brut[brut <= 0].sum()
    pf = net[net > 0].sum() / -kayip if kayip < 0 else float("inf")
    pfb = brut[brut > 0].sum() / -bk if bk < 0 else float("inf")
    return (f"  {ad}: {len(tr)} işlem | {_r(r.mean())}/işlem (komisyonsuz {_r(np.mean([x['r_brut'] for x in tr]))}) | "
            f"PF {_f2(pf)} (komisyonsuz {_f2(pfb)}) | kazanma {_pct((net > 0).mean())} | net {_usd(net.sum())}")


def _monte_carlo(tr, ufuklar):
    """İşlem sırasını rastgele karıştırarak (yerine koymalı) %95'lik düşüş ve kayıp serisi."""
    net = np.array([x["net"] for x in tr])
    rng = np.random.default_rng(11)
    out = []
    for u in ufuklar:
        if u < 5 or not len(net):
            continue
        dds, seriler = [], []
        for _ in range(10):
            orn = net[rng.integers(0, len(net), (200, u))]
            eq = np.concatenate([np.zeros((200, 1)), np.cumsum(orn, 1)], 1)
            dds.append((np.maximum.accumulate(eq, 1) - eq).max(1))
            for satir in (orn <= 0):
                en = seri = 0
                for kay in satir:
                    seri = seri + 1 if kay else 0
                    en = max(en, seri)
                seriler.append(en)
        out.append((u, float(np.percentile(np.concatenate(dds), 95)), int(np.percentile(seriler, 95))))
    return out


def _grup(g):
    """Bir sinyal grubunun özeti: A kurallarıyla R (komisyonlu/komisyonsuz) + çıkıştan bağımsız ölçüler."""
    r = np.mean([x["r"] for x in g])
    rb = np.mean([x["r_brut"] for x in g])
    u = [x for x in g if x.get("ilk1r")]
    kar = sum(x["ilk1r"] == "kâr" for x in u) / len(u) if u else None
    zar = sum(x["ilk1r"] == "zarar" for x in u) / len(u) if u else None
    mfe = np.median([x["mfe16"] for x in u]) if u else None
    mae = np.median([x["mae16"] for x in u]) if u else None
    return (f"{len(g)} | {_r(r)} (kom.suz {_r(rb)}) | önce +1R {_pct(kar)} / önce −1R {_pct(zar)} | "
            f"MFE16 med {_f2(mfe)} · MAE16 med {_f2(mae)}")


def _sayisal_tablo(ad, tr, anahtar):
    deger = [(x[anahtar], x) for x in tr if x.get(anahtar) is not None]
    if len(deger) < 40:
        return []
    v = np.array([a for a, _ in deger], float)
    kenar = list(np.unique(np.round(np.percentile(v, [25, 50, 75]), 3)))
    sinirlar = [-float("inf")] + kenar + [float("inf")]
    satir = [f"  {ad}:"]
    for a, b in zip(sinirlar[:-1], sinirlar[1:]):
        g = [x for val, x in deger if a <= val < b]
        if not g:
            continue
        etiket = f"< {_f2(b)}" if not math.isfinite(a) else (f"≥ {_f2(a)}" if not math.isfinite(b) else f"{_f2(a)}–{_f2(b)}")
        satir.append(f"   {etiket}: {_grup(g)}")
    return satir


def _kategori_tablo(ad, tr, fonk, en_az=15):
    gruplar = {}
    for x in tr:
        gruplar.setdefault(fonk(x), []).append(x)
    satir = [f"  {ad}:"]
    for k, g in sorted(gruplar.items(), key=lambda kv: -len(kv[1])):
        if len(g) >= en_az:
            satir.append(f"   {k}: {_grup(g)}")
    kucuk = sum(len(g) for g in gruplar.values() if len(g) < en_az)
    if kucuk:
        satir.append(f"   (≤{en_az - 1} sinyalli gruplar: toplam {kucuk})")
    return satir


def _iliski(x, alan):
    t = x.get(alan)
    istenen = "YÜKSELİŞ" if x["side"] == "AL" else "DÜŞÜŞ"
    if t in (None, "VERİ YOK"):
        return "veri yok"
    return "aynı yön" if t == istenen else ("yatay" if t == "YATAY" else "ters yön")


def _dagilim(ad, degerler):
    m = np.array(degerler, float)
    kovalar = [(0, 0.25), (0.25, 0.5), (0.5, 0.75), (0.75, 1.0), (1.0, 1.5), (1.5, 2.0), (2.0, 3.0), (3.0, float("inf"))]
    satir = [f"  {ad} ({len(m)} sinyal): medyan {_f2(float(np.median(m)))}R"]
    for a, b in kovalar:
        oran = float(((m >= a) & (m < b)).mean())
        etiket = f"{_f2(a)}–{_f2(b)}R" if math.isfinite(b) else f"≥{_f2(a)}R"
        satir.append(f"   {etiket}: {_pct(oran)} {'█' * int(round(oran * 40))}")
    return satir


def _t_ozet(ad, s, ceyrek=True):
    if not s["n"]:
        return [f"{ad}: işlem yok"]
    satir = [
        f"{ad}: {s['n']} işlem | {_r(s['ort_r'])}/işlem (%95 GA {_r(s['ga'][0])} … {_r(s['ga'][1])}) | "
        f"PF {_f2(s['pf'])} | kazanma {_pct(s['kazanma'])} | net {_usd(s['net'])} USDT | "
        f"en büyük düşüş {_usd(-s['dd'])} USDT ({_f2(s['dd_r'])}R) | "
        + (f"kârlı çeyrek {s['karli_ceyrek']}/{s['ceyrek_say']}" if ceyrek else f"kârlı ay {s['karli_ay']}/{s['ay_say']}"),
        f"   komisyon hariç: {_r(s['r_brut'])}/işlem | PF {_f2(s['pf_brut'])} · komisyon payı {_r(-s['kom_r'])}/işlem "
        f"(toplam {_usd(-s['kom_usdt'])} USDT)",
    ]
    if "r_fund" in s:
        satir.append(f"   komisyon + funding tahmini (8 saatte %{FUNDING_8S * 100:g}): {_r(s['r_fund'])}/işlem | "
                     f"PF {_f2(s['pf_fund'])} · funding ≈ {_usd(-s['fund_usdt'])} USDT".replace("0.01", "0,01"))
    satir.append(f"   süre: ortalama {_sure(s['ort_dk'])}, medyan {_sure(s['medyan_dk'])} | en büyük 5 kazanç "
                 f"{_usd(s['top5'])} USDT = kazançların {_pct(s['top5_kazanc'])}'i, net kârın "
                 f"{_pct(s['top5_net']) if s['top5_net'] is not None else '– (net ≤ 0)'}'i · onlarsız net "
                 f"{_usd(s['net_top5siz'])} USDT | en uzun kayıp serisi {s['seri']}")
    return satir


def _dokum(tr, s):
    if not s["n"]:
        return ["  işlem yok"]
    satir = [_kisa("LONG", [x for x in tr if x["side"] == "AL"]), _kisa("SHORT", [x for x in tr if x["side"] == "SAT"])]
    satir.append("  Çıkışlar: " + " · ".join(f"{k} {v}" for k, v in sorted(s["cikis"].items(), key=lambda kv: -kv[1]))
                 + (f" | yarı kâra ulaşma {_pct(s['kismi'])}" if s["kismi"] else ""))
    for c in sorted(s["cikis"], key=lambda c: -s["cikis"][c]):
        g = [x for x in tr if x["cikis"] == c]
        satir.append(f"   {c}: ort. {_r(np.mean([x['r'] for x in g]))}, ort. süre {_sure(np.mean([x['dk'] for x in g]))}")
    yil = {}
    for x in tr:
        yil.setdefault(_ts_ay(x["T"])[:4], []).append(x)
    satir.append("  Yıllık:")
    for y in sorted(yil):
        satir.append(_kisa(y, yil[y]).replace("  ", "   ", 1))
    satir.append("  Çeyreklik:")
    for c, n_, net_, rr in s["ceyrekler"]:
        satir.append(f"   {c}: {n_} işlem | net {_usd(net_)} USDT | {_r(rr)}/işlem" + (" ✅" if net_ > 0 else ""))
    return satir


def _durma_limiti(n, kod):
    try:
        return rapor.stop_limits(n, kod)
    except TypeError:
        return rapor.stop_limits(n)
    except Exception:
        return None


# =========================================================================
# 5) Kripto raporu
# =========================================================================
def _b_rapor(d, b):
    strat = _strateji(d)
    kod = strat.KOD
    hepsi = []
    for b_ in _birimler(dict(d, asama="simule")):
        hepsi += _json_oku(os.path.join(SONUC, f"{b_[0]}.json"), []) or []
    sinir = _ay_bas_ts(d["dogrulama_aylar"][0]) if d["dogrulama_aylar"] else 2 ** 62
    dogr_acik = bool(d.get("dogrulama")) and bool(d["dogrulama_aylar"])
    if not dogr_acik:
        hepsi = [x for x in hepsi if x["T"] < sinir]       # güvenlik: doğrulama dönemi hiçbir yere girmez
    sym_sira = {bs: i for i, bs in enumerate(d["islem_sembolleri"])}
    gel_aylar, dog_aylar = d["ayar_aylar"], d["dogrulama_aylar"]

    tr_hepsi = [x for x in hepsi if "atla" not in x]
    alinan, atl = _portfoy(tr_hepsi, sym_sira, {})
    gel_tr = [x for x in alinan if x["T"] < sinir]
    dog_tr = [x for x in alinan if x["T"] >= sinir]
    gel = _ist(gel_tr, gel_aylar)
    dg = _ist(dog_tr, dog_aylar) if dogr_acik else {"n": 0}
    sinirsiz = [x for x in tr_hepsi if x["T"] < sinir]
    atla_say = {}
    for x in hepsi:
        if "atla" in x:
            atla_say[x["atla"]] = atla_say.get(x["atla"], 0) + 1

    kayit = _json_oku(DOGRULAMA_KAYDI, {}) or {}
    kayitli = dict(kayit.get("modlar", {}))
    if kayit.get("ilk") and "eski strateji (GIRIS_MODU öncesi)" not in kayitli:
        kayitli["eski strateji (GIRIS_MODU öncesi)"] = kayit["ilk"]
    onceki = kayitli.get(kod)
    baska = [f"{k} ({x.get('tarih', '?')})" for k, x in kayitli.items() if k != kod]

    satir = [
        f"🧪 BACKTEST SONUCU (istek {d['istek']}) – {_ts_str(time.time())}",
        "🔧 Strateji: " + _strateji_ozeti(strat),
        f"Doğrulama: {'AÇIK' if dogr_acik else 'KAPALI (sadece geliştirme dönemi)'}",
        f"Veri: Binance USDⓈ-M Futures | {len(d['islem_sembolleri'])} coin | mumlar {'+'.join(d['zfs'])}",
        f"Geliştirme: {gel_aylar[0]} – {gel_aylar[-1]} ({len(gel_aylar)} ay) | Doğrulama: "
        + (f"{dog_aylar[0]} – {dog_aylar[-1]}" if dogr_acik else "hesaplanmadı"),
        f"Pozisyon: {bt.MARGIN_USDT:.0f} USDT teminat, stopta ≈{bt.MARGIN_USDT * bt.LOSS_PCT_OF_MARGIN:.0f} USDT (1R), "
        f"en fazla {bt.MAX_OPEN_POSITIONS} pozisyon / aynı yönde {bt.MAX_SAME_DIRECTION} | komisyon %0,05 (her dolumda)",
    ]
    if atla_say:
        satir.append("İşleme dönmeyen sinyal: " + " · ".join(f"{k} {n_}" for k, n_ in atla_say.items()))

    satir += ["", "1) GELİŞTİRME DÖNEMİ – portföy sınırları dahil"]
    satir += _t_ozet(kod, gel)
    satir.append("   sınır nedeniyle atlanan: " + " · ".join(f"{k} {n_}" for k, n_ in atl.items()))

    gecti = False
    satir.append("")
    if not dogr_acik:
        satir += ["2) DOĞRULAMA – KAPALI (DOGRULAMA_GOSTER=false): hesaplanmadı, karar kuralları uygulanmadı."]
    else:
        satir.append(f"2) DOĞRULAMA – {kod}, tek sefer")
        if onceki:
            satir.append(f"⚠️ Bu strateji için doğrulama ilk kez {onceki.get('tarih')} tarihinde (istek {onceki.get('istek')}) "
                         f"görüldü. Karar o ilk sonuca göre verilir; bu tekrar bakıştır.")
        if baska:
            satir.append("⚠️ Doğrulama dönemi daha önce başka stratejilerle de görüldü: " + ", ".join(baska) + ".")
        satir += _t_ozet(kod, dg)
        kural = []
        if dg["n"]:
            mc = _monte_carlo(gel_tr, [max(5, dg["n"])])
            dd_sinir = mc[0][1] if mc else None
            kural = [
                ("Ortalama ≥ +0,10R", dg["ort_r"] >= KARAR_ORT_R, _r(dg["ort_r"])),
                ("%95 GA alt sınırı > 0", dg["ga"][0] > 0, _r(dg["ga"][0])),
                (f"Kâr faktörü ≥ {_f2(KARAR_PF)}", dg["pf"] >= KARAR_PF, _f2(dg["pf"])),
                (f"Kârlı çeyrek ≥ %{KARAR_KARLI_CEYREK * 100:.0f}",
                 dg["karli_ceyrek"] >= math.ceil(KARAR_KARLI_CEYREK * dg["ceyrek_say"] - 1e-9),
                 f"{dg['karli_ceyrek']}/{dg['ceyrek_say']}"),
                (f"Düşüş ≤ geliştirme döneminin {dg['n']} işlemlik %95 düşüşü"
                 + (f" ({_usd(-dd_sinir)} USDT)" if dd_sinir is not None else ""),
                 dd_sinir is not None and dg["dd"] <= dd_sinir, _usd(-dg["dd"])),
            ]
        gecti = bool(kural) and all(k[1] for k in kural)
        satir.append("Karar kuralları (komisyon dahil):")
        satir += [f"  {'✅' if ok else '❌'} {ad}: {deger}" for ad, ok, deger in kural] or ["  doğrulamada işlem yok"]
        if dg["n"] and "r_fund" in dg:
            satir.append(f"  bilgi: funding tahmini dahil ortalama {_r(dg['r_fund'])}, PF {_f2(dg['pf_fund'])}")
        satir.append("SONUÇ: " + ("✅ GEÇTİ" if gecti else "❌ KALDI"))

    satir += ["", "3) DÖKÜM – geliştirme dönemi"] + _dokum(gel_tr, gel)
    if dogr_acik:
        satir += ["", "   doğrulama dönemi"] + _dokum(dog_tr, dg)

    if gel_tr:
        mf = np.array([x["mfe"] for x in gel_tr])
        rr = np.sort(np.array([x["r"] for x in gel_tr]))[::-1]
        satir += ["", "4) KAZANÇ YAPISI – geliştirme, portföyde alınan işlemler",
                  "  R dağılımı – en iyi 5: " + ", ".join(_r(v_) for v_ in rr[:5])
                  + f" | medyan {_r(float(np.median(rr)))} | ≥3R işlem {_pct(float((rr >= 3).mean()))}, "
                  f"≥5R {_pct(float((rr >= 5).mean()))}",
                  f"  işlem içinde ulaşılan en yüksek kâr (MFE): medyan {_f2(float(np.median(mf)))}R · "
                  + " · ".join(f"≥{e}R {_pct(float((mf >= e).mean()))}" for e in (1, 2, 3, 5))]
        iz = [x for x in gel_tr if x["cikis"] == "İZ" and x["mfe"] > 0]
        if iz:
            satir.append(f"  İZ çıkışlarında zirveden geri verilen (medyan): "
                         f"{_f2(float(np.median([x['mfe'] - x['r_brut'] for x in iz])))}R")

    u = [x for x in sinirsiz if x.get("ilk1r")]
    if u:
        kar = sum(x["ilk1r"] == "kâr" for x in u) / len(u)
        zar = sum(x["ilk1r"] == "zarar" for x in u) / len(u)
        satir += ["", f"5) ÇIKIŞTAN BAĞIMSIZ: girişten sonraki {UFUK_MUM} mum "
                      f"({_sure(UFUK_MUM * SEC[strat.ZAMAN_DILIMI] / 60)}) – geliştirme, her sinyal bağımsız",
                  f"  {len(u)} sinyal: önce +1R {_pct(kar)} · önce stop (−1R) {_pct(zar)} · ikisi de yok {_pct(1 - kar - zar)}"]
        satir += _dagilim(f"MFE{UFUK_MUM}", [x["mfe16"] for x in u])
        satir += _dagilim(f"MAE{UFUK_MUM}", [x["mae16"] for x in u])

    satir += ["", "6) SINIRSIZ SONUÇ ve BAĞLAM – geliştirme dönemi, her sinyal bağımsız işlem"]
    satir += _t_ozet(f"{kod} (sınırsız)", _ist(sinirsiz, gel_aylar))
    al_k = {(x["sym"], x["T"]) for x in alinan}
    satir.append(_kisa("portföyde alınan", [x for x in sinirsiz if (x["sym"], x["T"]) in al_k]))
    satir.append(_kisa("sınır yüzünden atlanan", [x for x in sinirsiz if (x["sym"], x["T"]) not in al_k]))
    if len(sinirsiz) >= 40:
        satir.append(f"  Bağlama göre (sütunlar: sinyal | R (kom.suz) | {UFUK_MUM} mumda önce +1R / −1R | MFE16 · MAE16 medyan)")
        satir += _sayisal_tablo("Stop mesafesi (%)", sinirsiz, "stop_pct")
        satir += _sayisal_tablo("Trend gücü |EMA12−EMA26|/ATR", sinirsiz, "trend_guc")
        satir += _sayisal_tablo("EMA26'ya uzaklık (ATR)", sinirsiz, "ema26_uzaklik")
        satir += _sayisal_tablo("RSI", sinirsiz, "rsi")
        satir += _sayisal_tablo("Volatilite (ATR %)", sinirsiz, "atr_pct")
        if getattr(strat, "UST_ZAMAN_DILIMI", None):
            satir += _sayisal_tablo("Üst ZD trend gücü", sinirsiz, "htf_guc")
        satir += _kategori_tablo("Yön", sinirsiz, lambda x: "LONG" if x["side"] == "AL" else "SHORT")
        satir += _kategori_tablo("BTC trendi (aynı ZD)", sinirsiz, lambda x: _iliski(x, "btc_trend"))
        if getattr(strat, "UST_ZAMAN_DILIMI", None):
            satir += _kategori_tablo("BTC trendi (üst ZD)", sinirsiz, lambda x: _iliski(x, "btc_htf_trend"))

    mc_aylar = gel_aylar + (dog_aylar if dogr_acik else [])
    mc_tr = gel_tr + (dog_tr if dogr_acik else [])
    ay_basina = max(1, round(len(mc_tr) / max(1, len(mc_aylar))))
    ufuklar = sorted({20, 30, 50, 60, 100, max(5, ay_basina * 3)})
    satir += ["", f"7) CANLI İZLEME EŞİKLERİ – {len(mc_aylar)} ayın işlemlerinden Monte Carlo (2000 karıştırma) "
                  f"ve stratejinin DURMA sınırları"]
    for u_, dd95, seri95 in _monte_carlo(mc_tr, ufuklar):
        lim = _durma_limiti(u_, kod)
        etiket = f"{u_} işlem" + (" (≈3 ay)" if u_ == max(5, ay_basina * 3) else "")
        satir.append(f"  {etiket}: %95 düşüş {_usd(-dd95)} USDT ({_f2(dd95 / (bt.MARGIN_USDT * bt.LOSS_PCT_OF_MARGIN))}R)"
                     f" | %95 kayıp serisi {seri95}"
                     + (f" | DURMA: düşüş {_usd(-lim[0])} USDT, seri {lim[1]}" if lim else ""))

    satir += ["", "Notlar:",
              "- Funding gerçek oranlarla değil, kaba tahminle (her iki yön için maliyet) hesaplandı; karar kuralları komisyon dahil sonuca uygulanır.",
              "- Çıkışlar stratejinin zaman dilimindeki mumlarla işletildi (1 dakikalık veri kullanılmadı); aynı mumda stop önce.",
              "- Stop mumun ötesinde açılırsa (boşluk) açılış fiyatından çıkılır; kayma ve kaldıraç kademesi yok.",
              "- Canlı sinyaller Gate.io spot verisinden; backtest Binance Futures verisinden.",
              f"- Süre: {(time.time() - d['baslangic']) / 60:.0f} dk."]
    if d["hatalar"]:
        satir += ["", "Hatalar:"] + [f"- {h}" for h in d["hatalar"][:10]]
    if d["eksik"]:
        satir += ["Eksik veri ayları: " + "; ".join(f"{k}: {','.join(v)}" for k, v in list(d["eksik"].items())[:10])]
    metin = "\n".join(satir)

    if dogr_acik and not onceki:
        kayit.setdefault("modlar", {})[kod] = {"tarih": _ts_str(time.time()), "istek": d["istek"], "varyant": kod,
                                               "gecti": gecti, "ort_r": dg.get("ort_r"), "n": dg["n"]}
        _json_yaz(DOGRULAMA_KAYDI, kayit)

    ek = f"{d['istek']}_{_dosya_adi(kod)}"
    rapor_yol = os.path.join(CALISMA, f"bt_rapor_{ek}.txt")
    with open(rapor_yol, "w") as f:
        f.write(metin)
    sutun = ["strateji", "donem", "portfoyde_alindi", "sembol", "tf", "yon", "giris_zamani", "cikis_zamani", "cikis",
             "yari_kar", "giris_fiyati", "cikis_fiyati", "stop", "net_usdt", "brut_usdt", "komisyon_usdt",
             "funding_tahmini_usdt", "risk_usdt", "r", "r_komisyonsuz", "r_funding_dahil", "mfe_r", "mae_r",
             "kaldirac", "sure_saat", "sinyal"] + BAGLAM + ["mfe16_r", "mae16_r", "ilk_1r"]
    csv_yol = os.path.join(CALISMA, f"bt_islemler_{ek}.csv")
    with open(csv_yol, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(sutun)
        for x in sorted(tr_hepsi, key=lambda y: y["T"]):
            w.writerow([kod, "dogrulama" if x["T"] >= sinir else "gelistirme", int((x["sym"], x["T"]) in al_k),
                        x["sym"], x["tf"], "LONG" if x["side"] == "AL" else "SHORT", _ts_str(x["T"]),
                        _ts_str(x["cikis_t"]), x["cikis"], int(x["kismi"]), x["giris_px"], x["cikis_px"], x["stop"],
                        x["net"], x["brut"], x["kom"], x["fund"], x["risk"], x["r"], x["r_brut"], x["r_fund"],
                        x["mfe"], x["mae"], x["lev"], round(x["dk"] / 60, 1), x["src"]]
                       + [x.get(k) for k in BAGLAM] + [x.get("mfe16"), x.get("mae16"), x.get("ilk1r")])

    notify = _NOTIFY[0]
    _gonder(notify, metin)
    _belge_gonder(rapor_yol, f"Backtest raporu (istek {d['istek']}, {kod})")
    _belge_gonder(csv_yol, "Tüm sinyaller (sınırsız) – portfoyde_alindi sütunu portföy sınırlarıyla alınanları gösterir")
    return True


# =========================================================================
# KRİPTO DIŞI (BACKTEST_VERI=dukascopy): stratejinin kuralları 10 piyasada
#   Plan önceden kayıtlı; piyasa listesi, maliyetler ve karar kuralları burada sabit, tarama yok.
#   - Veri: dukascopy-python (4.0.1) ile Dukascopy'den 2023-01-01'den bugüne, stratejinin zaman dilimi ve
#     üst zaman dilimi, bid, UTC. bt_calisma/veri_dukascopy içine kaydedilir, varsa tekrar indirilmez.
#     Hacmi 0 ve high == low olan mumlar (piyasa kapalıyken veri akışının ürettiği düz mumlar) atılır.
#   - Kurallar: kripto testiyle aynı strateji fonksiyonları ve aynı _islem (CIKIS).
#     Giriş sinyal mumundan sonraki mevcut mumun açılışında (hafta sonundan sonra da olabilir).
#   - Maliyet: spread işlem başına bir kez (sabit); swap her UTC gece yarısı için pozisyon değerinin
#     yıllık %5'i / 365 (long ve short maliyet); komisyon yok.
#   - Pozisyon: her işlem 1R = 100 USDT; her piyasada aynı anda en fazla 1 pozisyon; başka sınır yok.
# =========================================================================
KD_PIYASALAR = [
    # (rapor adı, Dukascopy adı, dukascopy-python 4.0.1'deki karşılığı, grup, spread (fiyat birimi))
    ("ALTIN", "XAU/USD", "XAU/USD", "metaller", 0.30),
    ("GUMUS", "XAG/USD", "XAG/USD", "metaller", 0.03),
    ("PETROL", "LIGHT.CMD", "E_Light", "enerji", 0.04),
    ("SP500", "USA500.IDX", "E_SandP-500", "endeksler", 0.6),
    ("NASDAQ", "USATECH.IDX", "E_NQ-100", "endeksler", 1.5),
    ("DAX", "DEU.IDX", "E_DAAX", "endeksler", 1.5),
    ("EURUSD", "EUR/USD", "EUR/USD", "döviz", 0.00008),
    ("GBPUSD", "GBP/USD", "GBP/USD", "döviz", 0.00012),
    ("USDJPY", "USD/JPY", "USD/JPY", "döviz", 0.010),
    ("AUDUSD", "AUD/USD", "AUD/USD", "döviz", 0.00010),
]
KD_GRUPLAR = ["metaller", "enerji", "endeksler", "döviz"]
KD_BASLANGIC = "2023-01-01"
KD_SWAP_YILLIK = 0.05
KD_R_USDT = 100.0
KD_ARALIK = {"15m": "INTERVAL_MIN_15", "1h": "INTERVAL_HOUR_1", "4h": "INTERVAL_HOUR_4", "1d": "INTERVAL_DAY_1"}
KD_VERI = os.path.join(CALISMA, "veri_dukascopy")
KD_SONUC = os.path.join(CALISMA, "sonuclar_kd")
KD_KRIPTO_DOSYA = "bt_islemler_7_trend.csv"   # birleşik portföy için kripto işlemleri (istek 7, trend_4h)
KD_BOOT = 20000


def _k_bilgi(piyasa):
    return next(x for x in KD_PIYASALAR if x[0] == piyasa)


def _k_yeni_is(istek, strat, dogr, notify):
    simdi = datetime.datetime.now(datetime.timezone.utc)
    son_ay = f"{simdi.year}-{simdi.month:02d}"
    gel = _aylar(*GELISTIRME)
    y, m = map(int, GELISTIRME[1].split("-"))
    dog = _aylar("%d-%02d" % _ay_kaydir(y, m, 1), son_ay)
    tf = strat.ZAMAN_DILIMI
    ust = getattr(strat, "UST_ZAMAN_DILIMI", None)
    zfs = [tf] + ([ust] if ust and ust != tf else [])
    for z in zfs:
        if z not in KD_ARALIK:
            raise RuntimeError(f"Dukascopy testinde {z} zaman dilimi desteklenmiyor ({', '.join(KD_ARALIK)})")
    d = {
        "istek": istek, "strateji": strat.KOD, "veri": "dukascopy", "dogrulama": dogr, "kd": True,
        "asama": "indir", "sira": 0, "alt": 0, "baslangic": time.time(), "zfs": zfs,
        "piyasalar": [p_[0] for p_ in KD_PIYASALAR], "ayar_aylar": gel, "dogrulama_aylar": dog,
        "bitis": simdi.strftime("%Y-%m-%d"),
        "hatalar": [], "deneme": {}, "eksik": {}, "atlanan_coin": [], "indirilemeyen": [],
    }
    notify("\n".join([
        f"🧪 Kripto dışı backtest başladı (istek {istek})",
        "Strateji: " + _strateji_ozeti(strat),
        "Piyasalar: " + ", ".join(f"{p_[0]} ({p_[1]})" for p_ in KD_PIYASALAR),
        f"Veri: Dukascopy {'+'.join(zfs)}, bid, UTC, {KD_BASLANGIC} – bugün",
        f"Geliştirme: {gel[0]} – {gel[-1]} ({len(gel)} ay) | Doğrulama: "
        + ((f"{dog[0]} – {dog[-1]}" if dog else "yok") if dogr else "KAPALI (hiç hesaplanmayacak)"),
        "Maliyet: sabit spread (işlem başına bir kez), swap gece başına yıllık %5, komisyon yok. 1R = 100 USDT.",
        f"Birleşik portföy: + kripto portföyü ({KD_KRIPTO_DOSYA}).",
    ]))
    return d


def _k_birimler(d):
    if d["asama"] == "indir":
        return [(p_, tf) for p_ in d["piyasalar"] for tf in d["zfs"]]
    if d["asama"] in ("sinyal", "simule"):
        return [(p_,) for p_ in d["piyasalar"]]
    if d["asama"] == "rapor":
        return [("rapor",)]
    return []


def _k_sonraki_asama(d, notify):
    a = d["asama"]
    gecen = (time.time() - d["baslangic"]) / 60
    if a == "indir":
        if d.get("indirilemeyen"):
            notify("\n".join([
                f"⚠️ Kripto dışı backtest (istek {d['istek']}) DURDURULDU: şu veriler indirilemedi:",
                *[f"- {x}" for x in d["indirilemeyen"]],
                "Plan gereği hiçbir piyasa listeden çıkarılmadı ve karar verilmedi. Nasıl devam edelim?",
                f"(Sadece yeniden denemek istersen {ISTEK_DEGISKENI} değerini değiştir; indirilen veriler saklı.)",
            ]))
            d["asama"] = "bitti"
        else:
            satir = [f"📥 Kripto dışı veri hazır ({len(d['piyasalar'])} piyasa, {gecen:.0f} dk)"]
            for p_ in d["piyasalar"]:
                bilgi = []
                for tf in d["zfs"]:
                    v = _k_yukle(p_, tf)
                    bilgi.append(f"{tf} {len(v['t'])} mum ({_ts_str(int(v['t'][0]))[:10]} – {_ts_str(int(v['t'][-1]))[:10]})")
                satir.append(f"  {p_}: " + " · ".join(bilgi))
            notify("\n".join(satir))
            d["asama"] = "sinyal"
    elif a == "sinyal":
        say = sum(len((_json_oku(os.path.join(SINYAL, f"kd_{p_}.json"), {}) or {}).get("sinyaller", []))
                  for p_ in d["piyasalar"])
        notify(f"📶 Kripto dışı: sinyaller hesaplandı – {say} sinyal, {gecen:.0f} dk. Şimdi çıkışlar işletiliyor.")
        d["asama"] = "simule"
    elif a == "simule":
        d["asama"] = "rapor"
    elif a == "rapor":
        d["asama"] = "bitti"
    d["sira"] = 0
    d["alt"] = 0


# ---- Veri ----
def _k_veri_yolu(piyasa, tf):
    return os.path.join(KD_VERI, f"{piyasa}_{tf}.csv")


class _ZamanAsimli:
    """dukascopy-python requests.get'i zaman aşımısız çağırıyor; takılıp canlı botu bekletmesin diye 12 sn sınır."""

    def __init__(self, modul):
        self._m = modul

    def __getattr__(self, ad):
        return getattr(self._m, ad)

    def get(self, *a, **kw):
        kw.setdefault("timeout", 12)
        return self._m.get(*a, **kw)


def _k_indir(d, b):
    piyasa, tf = b
    os.makedirs(KD_VERI, exist_ok=True)
    yol = _k_veri_yolu(piyasa, tf)
    if os.path.exists(yol):
        return True                                   # bir kez indirilen veri tekrar indirilmez
    if _kalan() < 25:
        return False                                  # bir sonraki dakikada, yeterli süre varken dene
    try:
        import dukascopy_python as dp
    except Exception as e:
        raise RuntimeError(f"dukascopy-python yüklü değil ({e}); requirements.txt'ye dukascopy-python==4.0.1 ekle")
    import requests as _rq
    dp.requests = _ZamanAsimli(_rq)
    kod = _k_bilgi(piyasa)[2]
    bas = datetime.datetime.fromisoformat(KD_BASLANGIC).replace(tzinfo=datetime.timezone.utc)
    son = datetime.datetime.now(datetime.timezone.utc)
    df = dp.fetch(kod, getattr(dp, KD_ARALIK[tf]), dp.OFFER_SIDE_BID, bas, son, max_retries=0)
    if df is None or len(df) < 100:
        raise RuntimeError(f"Dukascopy'den yeterli mum gelmedi ({0 if df is None else len(df)} satır)")
    df = df.reset_index()
    t = (pd.to_datetime(df["timestamp"], utc=True) - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)
    out = pd.DataFrame({"t": t, "open": df["open"].astype(float), "high": df["high"].astype(float),
                        "low": df["low"].astype(float), "close": df["close"].astype(float),
                        "volume": df["volume"].astype(float)})
    out = out.drop_duplicates("t").sort_values("t")
    tmp = yol + ".tmp"
    out.to_csv(tmp, index=False)
    os.replace(tmp, yol)
    return True


def _k_yukle(piyasa, tf):
    """Kaydedilmiş veri; piyasa kapalıyken üretilen düz mumlar (hacim 0, high == low) atılır."""
    df = pd.read_csv(_k_veri_yolu(piyasa, tf))
    df = df[~((df["volume"] <= 0) & (df["high"] <= df["low"]))]
    df = df.dropna().reset_index(drop=True)
    return {"t": df["t"].to_numpy(np.int64), "o": df["open"].to_numpy(float), "h": df["high"].to_numpy(float),
            "l": df["low"].to_numpy(float), "c": df["close"].to_numpy(float)}


# ---- Sinyaller ----
def _k_sinyal(d, b):
    piyasa = b[0]
    os.makedirs(SINYAL, exist_ok=True)
    strat = _strateji(d)
    tf = strat.ZAMAN_DILIMI
    ust = getattr(strat, "UST_ZAMAN_DILIMI", None)
    v = _k_yukle(piyasa, tf)
    _, kol = _df(v, strat)
    t = v["t"]
    ust_onb = {}
    if ust:
        uv = _k_yukle(piyasa, ust)
        _, ukol = _df(uv)
        ut, usec = uv["t"], SEC[ust]

    def ust_trend(T_kapanis):
        j = int(np.searchsorted(ut + usec, T_kapanis, side="right")) - 1   # T anında kapanmış son üst ZD mumu
        if j < 39:
            return "VERİ YOK"
        if j not in ust_onb:
            a = max(0, j - 198)
            ust_onb[j] = sb.market_state(_Pencere(ukol, a, j + 2), j - a)
        return ust_onb[j]

    bas = _ay_bas_ts(d["ayar_aylar"][0])
    sinir = _ay_bas_ts(d["dogrulama_aylar"][0]) if d["dogrulama_aylar"] else 2 ** 62
    son = 2 ** 62 if d.get("dogrulama") else sinir
    liste = []
    with contextlib.redirect_stdout(io.StringIO()):
        for i in range(39, len(t) - 1):
            T = int(t[i + 1])                              # giriş: bir sonraki mevcut mumun açılışı
            if T < bas or T >= son:
                continue
            ht = ust_trend(int(t[i]) + SEC[tf]) if ust else None
            a = max(0, i - 198)
            alert = sb.check_alert(strat, _Pencere(kol, a, i + 2), ht)
            if alert:
                liste.append({"T": T, "i": i, "sym": piyasa, "tf": tf, "side": alert["side"],
                              "entry": float(alert["entry"]), "stop": float(alert["stop"]),
                              "target": None if alert.get("target") is None else float(alert["target"]),
                              "src": "+".join(alert.get("sources") or [strat.KOD]), "htf_trend": ht})
    _json_yaz(os.path.join(SINYAL, f"kd_{piyasa}.json"), {"strateji": strat.KOD, "sinyaller": liste})
    return True


def _k_simule(d, b):
    piyasa = b[0]
    os.makedirs(KD_SONUC, exist_ok=True)
    strat = _strateji(d)
    v = _k_yukle(piyasa, strat.ZAMAN_DILIMI)
    _, kol = _df(v, strat)
    sinyaller = (_json_oku(os.path.join(SINYAL, f"kd_{piyasa}.json"), {}) or {}).get("sinyaller", [])
    sinir = _ay_bas_ts(d["dogrulama_aylar"][0]) if d["dogrulama_aylar"] else 2 ** 62
    spread = _k_bilgi(piyasa)[4]
    sonuc, acik_bitis, atlanan = [], -1, 0
    with contextlib.redirect_stdout(io.StringIO()):
        for sig in sorted(sinyaller, key=lambda x: x["T"]):
            if sig["T"] < acik_bitis:                      # bu piyasada açık pozisyon var
                atlanan += 1
                continue
            r = _islem(sig, strat, v, kol, sinir, ("kd", spread))
            if "atla" in r:
                continue
            r["grup"] = _k_bilgi(piyasa)[3]
            sonuc.append(r)
            acik_bitis = r["cikis_t"]
    _json_yaz(os.path.join(KD_SONUC, f"{piyasa}.json"), {"islemler": sonuc, "acik_varken_atlanan": atlanan})
    return True


# ---- İstatistik ----
def _k_ceyrek(ts):
    y, m = map(int, _ts_ay(ts).split("-"))
    return f"{y}-Ç{(m - 1) // 3 + 1}"


def _k_blok_ga(tr, aylar, seed=5):
    """Aylık blok bootstrap: ayları (içindeki tüm işlemlerle) yerine koyarak çek; işlem başına ortalama R'nin %95 aralığı."""
    if not tr:
        return None
    idx = {a: i for i, a in enumerate(aylar)}
    top = np.zeros(len(aylar))
    say = np.zeros(len(aylar))
    for x in tr:
        i = idx.get(_ts_ay(x["T"]))
        if i is not None:
            top[i] += x["r"]
            say[i] += 1
    rng = np.random.default_rng(seed)
    secim = rng.integers(0, len(aylar), (KD_BOOT, len(aylar)))
    t_, s_ = top[secim].sum(1), say[secim].sum(1)
    ort = t_[s_ > 0] / s_[s_ > 0]
    return float(np.percentile(ort, 2.5)), float(np.percentile(ort, 97.5))


def _k_ist(tr, aylar):
    s = {"n": len(tr)}
    if not tr:
        return s
    r = np.array([x["r"] for x in tr])
    s["ort_r"] = float(r.mean())
    s["toplam_r"] = float(r.sum())
    kay = r[r <= 0].sum()
    s["pf"] = float(r[r > 0].sum() / -kay) if kay < 0 else float("inf")
    s["kazanma"] = float((r > 0).mean())
    s["ga"] = _k_blok_ga(tr, aylar)
    kap = sorted(tr, key=lambda x: x["cikis_t"])
    s["dd_r"] = _dd([x["r"] for x in kap])
    cey = {}
    for a in aylar:
        cey.setdefault(_k_ceyrek(_ay_bas_ts(a)), 0.0)
    for x in tr:
        cey[_k_ceyrek(x["T"])] = cey.get(_k_ceyrek(x["T"]), 0.0) + x["r"]
    s["ceyrekler"] = cey
    s["karli_ceyrek"] = sum(1 for v_ in cey.values() if v_ > 0)
    s["ceyrek_say"] = len(cey)
    kaz = sorted((v_ for v_ in r if v_ > 0), reverse=True)
    s["top5"] = float(sum(kaz[:5]))
    s["top5_kazanc"] = s["top5"] / sum(kaz) if kaz else None
    s["top5_net"] = s["top5"] / s["toplam_r"] if s["toplam_r"] > 0 else None
    for yon, ad in (("AL", "long"), ("SAT", "short")):
        g = [x["r"] for x in tr if x["side"] == yon]
        s[f"{ad}_n"] = len(g)
        s[f"{ad}_ort"] = float(np.mean(g)) if g else None
        s[f"{ad}_top"] = float(np.sum(g)) if g else 0.0
    s["spread_r"] = float(np.mean([x.get("r_spread", 0.0) for x in tr]))
    s["swap_r"] = float(np.mean([x.get("r_swap", 0.0) for x in tr]))
    s["kom_r"] = float(np.mean([x.get("r_kom", 0.0) for x in tr]))
    s["brut_r"] = float(np.mean([x["r_brut"] for x in tr]))
    s["ort_gun"] = float(np.mean([x["dk"] for x in tr])) / 1440
    return s


def _k_kural(s):
    if not s["n"]:
        return [], False
    k = [
        ("Ortalama ≥ +0,10R", s["ort_r"] >= KARAR_ORT_R, _r(s["ort_r"])),
        ("Aylık blok bootstrap %95 GA alt sınırı > 0", s["ga"] is not None and s["ga"][0] > 0,
         _r(s["ga"][0]) if s["ga"] else "–"),
        (f"Kâr faktörü ≥ {_f2(KARAR_PF)}", s["pf"] >= KARAR_PF, _f2(s["pf"])),
        (f"Kârlı çeyrek ≥ %{KARAR_KARLI_CEYREK * 100:.0f}",
         s["karli_ceyrek"] >= math.ceil(KARAR_KARLI_CEYREK * s["ceyrek_say"] - 1e-9),
         f"{s['karli_ceyrek']}/{s['ceyrek_say']}"),
    ]
    return k, all(x[1] for x in k)


def _k_satir(ad, s):
    if not s["n"]:
        return [f"{ad}: işlem yok"]
    ga = f"{_r(s['ga'][0])} … {_r(s['ga'][1])}" if s["ga"] else "–"
    return [
        f"{ad}: {s['n']} işlem | {_r(s['ort_r'])}/işlem (aylık blok %95 GA {ga}) | PF {_f2(s['pf'])} | "
        f"kazanma {_pct(s['kazanma'])} | toplam {_r(s['toplam_r'])} ({_usd(s['toplam_r'] * KD_R_USDT)} USDT) | "
        f"en büyük düşüş {_f2(s['dd_r'])}R ({_usd(-s['dd_r'] * KD_R_USDT)} USDT) | kârlı çeyrek {s['karli_ceyrek']}/{s['ceyrek_say']}",
        f"   LONG {s['long_n']} işlem {_r(s['long_ort'])}/işlem · SHORT {s['short_n']} işlem {_r(s['short_ort'])}/işlem | "
        f"en büyük 5 kazanç {_r(s['top5'])} = kazançların {_pct(s['top5_kazanc'])}'i, net kârın "
        f"{_pct(s['top5_net']) if s['top5_net'] is not None else '– (net ≤ 0)'}'i",
        f"   maliyet payı: spread {_r(-s['spread_r'])}/işlem · swap {_r(-s['swap_r'])}/işlem"
        + (f" · komisyon {_r(-s['kom_r'])}/işlem" if s["kom_r"] else "")
        + f" | maliyetsiz {_r(s['brut_r'])}/işlem | ort. süre {_f2(s['ort_gun'])} gün",
    ]


def _k_kripto(kod, sinir_bas, sinir_son):
    """Kripto portföy işlemleri (geliştirme dönemi, portföyde alınanlar) – KD_KRIPTO_DOSYA'dan."""
    yol = os.path.join(CALISMA, KD_KRIPTO_DOSYA)
    if not os.path.exists(yol):
        return None, f"{KD_KRIPTO_DOSYA} bulunamadı"
    df = pd.read_csv(yol)
    for sutun in ("strateji", "mod"):                     # yeni CSV: strateji, eski (istek 7): mod
        if sutun in df.columns:
            df = df[df[sutun] == kod]
            break
    df = df[(df["donem"] == "gelistirme") & (df["portfoyde_alindi"] == 1)]
    tr = []
    for _, x in df.iterrows():
        T = int(pd.Timestamp(x["giris_zamani"]).tz_localize(rapor.TZ).timestamp())
        Tc = int(pd.Timestamp(x["cikis_zamani"]).tz_localize(rapor.TZ).timestamp())
        if not (sinir_bas <= T < sinir_son):
            continue
        r_kom = float(x["r_komisyonsuz"]) - float(x["r"])
        r_fund = float(x["r"]) - float(x["r_funding_dahil"])
        tr.append({"sym": x["sembol"], "grup": "kripto", "side": "AL" if x["yon"] == "LONG" else "SAT",
                   "T": T, "cikis_t": Tc, "cikis": x["cikis"], "r": float(x["r_funding_dahil"]),
                   "r_kom_dahil": float(x["r"]), "r_brut": float(x["r_komisyonsuz"]), "r_kom": r_kom,
                   "r_swap": r_fund, "r_spread": 0.0, "dk": int((Tc - T) // 60)})
    if not tr:
        return None, f"{KD_KRIPTO_DOSYA} içinde {kod} için geliştirme dönemi işlemi yok"
    return tr, None


def _k_rapor(d, b):
    aylar = d["ayar_aylar"]
    sinir = _ay_bas_ts(d["dogrulama_aylar"][0]) if d["dogrulama_aylar"] else 2 ** 62
    bas = _ay_bas_ts(aylar[0])
    dogr_acik = bool(d.get("dogrulama")) and bool(d["dogrulama_aylar"])
    hepsi, atl = [], {}
    for p_ in d["piyasalar"]:
        j = _json_oku(os.path.join(KD_SONUC, f"{p_}.json"), {}) or {}
        hepsi += j.get("islemler", [])
        atl[p_] = j.get("acik_varken_atlanan", 0)
    gel = [x for x in hepsi if x["T"] < sinir]
    dog = [x for x in hepsi if x["T"] >= sinir] if dogr_acik else []
    strat = _strateji(d)
    kripto, kripto_hata = _k_kripto(strat.KOD, bas, sinir)

    ozet = []                                      # (ad, istatistik) – CSV için de

    def ekle(ad, tr, ay=aylar):
        st = _k_ist(tr, ay)
        ozet.append((ad, st))
        return st

    s_top = ekle("KRİPTO DIŞI TOPLAM", gel)
    s_bir = ekle("BİRLEŞİK (kripto dışı + kripto)", gel + kripto) if kripto is not None else {"n": 0}
    k_top, ok_top = _k_kural(s_top)
    k_bir, ok_bir = _k_kural(s_bir) if kripto is not None else ([], False)

    satir = [
        f"🧪 KRİPTO DIŞI TREND BACKTEST (istek {d['istek']}) – {_ts_str(time.time())}",
        "🔧 Strateji: " + _strateji_ozeti(strat) + f" | Doğrulama: {'AÇIK' if dogr_acik else 'KAPALI'}",
        f"Veri: Dukascopy {'+'.join(d['zfs'])} bid, UTC, {KD_BASLANGIC} – {d['bitis']} | Geliştirme {aylar[0]} – {aylar[-1]} ({len(aylar)} ay)",
        "Maliyet: spread işlem başına bir kez (" + ", ".join(
            f"{p_[0]} {p_[4]:.5f}".rstrip("0").rstrip(".").replace(".", ",") for p_ in KD_PIYASALAR)
        + f"), swap gece başına yıllık %{KD_SWAP_YILLIK * 100:g}, komisyon yok | 1R = {KD_R_USDT:.0f} USDT | "
        "her piyasada en fazla 1 pozisyon",
        f"Kripto: {KD_KRIPTO_DOSYA} – {strat.KOD} portföyü (komisyon + funding tahmini dahil R)"
        + (f" – {len(kripto)} işlem" if kripto is not None else f" – YOK ({kripto_hata})"),
        "",
        "1) KARAR KURALLARI – geliştirme dönemi (tek piyasalar karar için kullanılmaz)",
        "(a) Kripto dışı toplam:",
        *[f"  {'✅' if ok else '❌'} {ad}: {deger}" for ad, ok, deger in k_top],
        "  SONUÇ: " + ("✅ GEÇTİ" if ok_top else "❌ KALDI"),
        "(b) Birleşik portföy (10 piyasa + kripto):",
    ]
    if kripto is None:
        satir.append(f"  hesaplanamadı: {kripto_hata}")
    else:
        satir += [f"  {'✅' if ok else '❌'} {ad}: {deger}" for ad, ok, deger in k_bir]
        satir.append("  SONUÇ: " + ("✅ GEÇTİ" if ok_bir else "❌ KALDI"))
        alt = [dict(x, r=x.get("r_kom_dahil", x["r"])) for x in kripto]
        s_alt = _k_ist(gel + alt, aylar)
        satir.append(f"  bilgi: kriptoda funding tahmini hariç (sadece komisyon) birleşik ortalama {_r(s_alt['ort_r'])}, "
                     f"PF {_f2(s_alt['pf'])}, GA alt {_r(s_alt['ga'][0]) if s_alt['ga'] else '–'}")

    satir += ["", "2) TOPLAMLAR"]
    satir += _k_satir("KRİPTO DIŞI TOPLAM", s_top)
    if kripto is not None:
        satir += _k_satir(f"KRİPTO ({KD_KRIPTO_DOSYA})", ekle("KRİPTO", kripto))
        satir += _k_satir("BİRLEŞİK", s_bir)

    satir += ["", "3) GRUPLAR"]
    for g in KD_GRUPLAR:
        satir += _k_satir(g.upper(), ekle(f"grup: {g}", [x for x in gel if x["grup"] == g]))

    satir += ["", "4) PİYASALAR (bilgi; karar için kullanılmaz)"]
    for p_ in d["piyasalar"]:
        satir += _k_satir(f"{p_} ({_k_bilgi(p_)[1]})", ekle(p_, [x for x in gel if x["sym"] == p_]))
        satir.append(f"   açık pozisyon varken atlanan sinyal: {atl[p_]}")

    satir += ["", "5) ÇEYREKLER (toplam R)"]
    ceyrekler = list(s_top.get("ceyrekler", {}))
    basl = ["kripto dışı"] + (["kripto", "birleşik"] if kripto is not None else [])
    seriler = [s_top] + ([next(st for ad, st in ozet if ad == "KRİPTO"), s_bir] if kripto is not None else [])
    satir.append("   çeyrek: " + " | ".join(basl))
    for c in ceyrekler:
        satir.append(f"   {c}: " + " | ".join(_r(st.get("ceyrekler", {}).get(c, 0.0)) for st in seriler))

    if kripto is not None:
        def aylik(tr):
            a = {x: 0.0 for x in aylar}
            for x in tr:
                if _ts_ay(x["T"]) in a:
                    a[_ts_ay(x["T"])] += x["r"]
            return np.array([a[x] for x in aylar])
        ak, ad_ = aylik(kripto), aylik(gel)
        kor = float(np.corrcoef(ak, ad_)[0, 1]) if ak.std() > 0 and ad_.std() > 0 else None
        sp = None
        if kor is not None:
            sp = float(np.corrcoef(pd.Series(ak).rank().to_numpy(), pd.Series(ad_).rank().to_numpy())[0, 1])
        satir += ["", f"6) KORELASYON – aylık toplam R, kripto ile kripto dışı ({len(aylar)} ay)",
                  f"   Pearson {_f2(kor)} · Spearman {_f2(sp)}",
                  f"   ikisinin de zararda olduğu ay: {int(((ak < 0) & (ad_ < 0)).sum())} · "
                  f"biri zararda diğeri kârda: {int(((ak < 0) != (ad_ < 0)).sum())}"]

    if dogr_acik:
        satir += ["", "7) DOĞRULAMA"]
        sd = _k_ist(dog, d["dogrulama_aylar"])
        satir += _k_satir("KRİPTO DIŞI TOPLAM (doğrulama)", sd)
    else:
        satir += ["", "Doğrulama KAPALI: geliştirme döneminden sonraki aylar hiç hesaplanmadı."]

    satir += ["", "Notlar:",
              "- Komisyon yok sayıldı (kripto dışı); spread sabit tahmin, gerçek spread haber/gece saatlerinde daha geniş olabilir.",
              "- Swap her iki yönde maliyet (yıllık %5, UTC gece yarısı başına 1/365); gerçek swap piyasaya ve brokera göre değişir, bazen alacak olur.",
              "- Dukascopy'nin piyasa kapalıyken ürettiği düz mumlar (hacim 0, high = low) atıldı; diğer mumlar olduğu gibi kullanıldı.",
              "- Stopun ötesinde açılan mumda (hafta sonu boşluğu) açılış fiyatından çıkıldı. Geliştirme işlemi dönem sonunda açıksa DÖNEM ile kapatıldı.",
              "- Kripto işlemlerinin R'si komisyon + funding tahmini dahil; kripto portföy sınırları o testteki gibi.",
              f"- Süre: {(time.time() - d['baslangic']) / 60:.0f} dk."]
    if d["hatalar"]:
        satir += ["", "Hatalar:"] + [f"- {h}" for h in d["hatalar"][:10]]
    metin = "\n".join(satir)

    ek = f"{d['istek']}_kd_{_dosya_adi(strat.KOD)}"
    rapor_yol = os.path.join(CALISMA, f"bt_rapor_{ek}.txt")
    with open(rapor_yol, "w") as f:
        f.write(metin)
    ozet_yol = os.path.join(CALISMA, f"bt_ozet_{ek}.csv")
    with open(ozet_yol, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["satir", "islem", "ort_r", "ga_alt", "ga_ust", "pf", "kazanma", "toplam_r", "toplam_usdt",
                    "dusus_r", "karli_ceyrek", "ceyrek_say", "top5_r", "top5_kazanc_payi", "top5_net_payi",
                    "long_n", "long_ort_r", "short_n", "short_ort_r", "spread_r", "swap_funding_r", "komisyon_r",
                    "maliyetsiz_r", "ort_gun"])
        for ad, st in ozet:
            if not st["n"]:
                w.writerow([ad, 0])
                continue
            ga_alt, ga_ust = st["ga"] or (None, None)
            w.writerow([ad, st["n"], round(st["ort_r"], 4),
                        None if ga_alt is None else round(ga_alt, 4), None if ga_ust is None else round(ga_ust, 4),
                        round(st["pf"], 3) if math.isfinite(st["pf"]) else "inf", round(st["kazanma"], 4),
                        round(st["toplam_r"], 3), round(st["toplam_r"] * KD_R_USDT, 2), round(st["dd_r"], 3),
                        st["karli_ceyrek"], st["ceyrek_say"], round(st["top5"], 3),
                        None if st["top5_kazanc"] is None else round(st["top5_kazanc"], 4),
                        None if st["top5_net"] is None else round(st["top5_net"], 4),
                        st["long_n"], None if st["long_ort"] is None else round(st["long_ort"], 4),
                        st["short_n"], None if st["short_ort"] is None else round(st["short_ort"], 4),
                        round(st["spread_r"], 4), round(st["swap_r"], 4), round(st["kom_r"], 4),
                        round(st["brut_r"], 4), round(st["ort_gun"], 2)])
    islem_yol = os.path.join(CALISMA, f"bt_islemler_{ek}.csv")
    with open(islem_yol, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["piyasa", "grup", "donem", "yon", "giris_zamani_utc", "cikis_zamani_utc", "cikis", "giris_fiyati",
                    "cikis_fiyati", "stop", "stop_pct", "r", "r_maliyetsiz", "r_spread", "r_swap", "geceler",
                    "net_usdt", "mfe_r", "sure_gun"])
        for x in sorted(gel + dog, key=lambda y: y["T"]):
            utc = lambda ts: datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
            w.writerow([x["sym"], x["grup"], "dogrulama" if x["T"] >= sinir else "gelistirme",
                        "LONG" if x["side"] == "AL" else "SHORT", utc(x["T"]), utc(x["cikis_t"]), x["cikis"],
                        x["giris_px"], x["cikis_px"], x["stop"], x["stop_pct"], x["r"], x["r_brut"], x["r_spread"],
                        x["r_swap"], x["geceler"], x["net"], x["mfe"], round(x["dk"] / 1440, 2)])

    notify = _NOTIFY[0]
    _gonder(notify, metin)
    _belge_gonder(rapor_yol, f"Kripto dışı backtest raporu (istek {d['istek']})")
    _belge_gonder(ozet_yol, "Özet tablo (toplam, kripto, birleşik, gruplar, piyasalar)")
    _belge_gonder(islem_yol, "Kripto dışı işlem listesi")
    return True


# =========================================================================
# VARYANT TESTİ (PLANLAR[kod]["tip"] == "varyant"; şimdilik sikisma_15m)
#   Plan önceden kayıtlı; strateji dosyasındaki hiçbir parametre değiştirilmez, tarama yok.
#   - Sinyaller: compute_indicators → strat.hazirla → her kapanmış mumda check_alert(strat, df, ust_trend)
#     (strat.VARYANT = "V0" / "V1"); ust_trend = sinyal anında kapanmış son 1h mumun market_state'i.
#   - V0: sinyal mumundan sonraki mumun açılışında piyasa emri. V1: V0 + 1h trend filtresi (dosyadaki kural).
#     V2: strat.retest_seviyesi seviyesine limit emir; sonraki en fazla 4 mumda fiyat seviyenin ÖTESİNE geçerse
#     dolar (değmek yetmez); dolmadan bir mum kutu ortasının ötesine kapanırsa ya da 4 mum dolarsa iptal.
#     Stop strat.stop() ile dolum fiyatına göre yeniden (risk %0,6–3 dışıysa işlem yok), TP = 3R.
#     Dolum mumunda sadece stop kontrol edilir (dolumdan önce mi sonra mı olduğu bilinmeyen TP sayılmaz).
#   - RASTGELE: V0 sinyal mumlarında aynı giriş, aynı stop mesafesi ve çıkış kuralları, yön yazı-tura;
#     sabit tohumla 200 tekrar.
#   - Maliyet: taker %0,05/taraf, maker %0,02 (V2 girişi); kayma piyasa emri başına BTC/ETH %0,02, diğerleri %0,05
#     (V2 girişi hariç tüm giriş/çıkışlar piyasa emri); funding 00/08/16 UTC'yi geçen pozisyonlarda %0,01.
#     R = giriş fiyatı (kaymasız) ile stop arası; maliyetler R'den düşülür.
#   - Portföy: açık en fazla MAX_OPEN_POSITIONS, aynı yönde MAX_SAME_DIRECTION, aynı coinde 1, günde (UTC) en fazla
#     8 işlem, gün içi gerçekleşen sonuç −6R'ye ulaşınca o gün yeni işlem yok. Sınırsız sonuç da raporlanır.
# =========================================================================
def _plan(d):
    return PLANLAR[d["strateji"]]


def _v_yeni_is(istek, strat, dogr, notify):
    plan = PLANLAR[strat.KOD]
    tf, ust = strat.ZAMAN_DILIMI, getattr(strat, "UST_ZAMAN_DILIMI", None)
    zfs = [tf] + ([ust] if ust and ust != tf else [])
    coinler = list(getattr(strat, "COINLER", None) or sb.SYMBOLS)
    if "BTC_USDT" not in coinler:
        coinler.append("BTC_USDT")
    gel = _aylar(*plan["gelistirme"])
    dog = _aylar(*plan["dogrulama"])
    d = {
        "istek": istek, "strateji": strat.KOD, "veri": "kripto", "dogrulama": dogr, "vt": True,
        "asama": "indir", "sira": 0, "alt": 0, "baslangic": time.time(), "zfs": zfs,
        "semboller": [[g, _bsym(g)] for g in coinler],
        "islem_sembolleri": [_bsym(g) for g in (getattr(strat, "COINLER", None) or sb.SYMBOLS)],
        "indir_aylar": _aylar(*plan["veri"]), "ayar_aylar": gel, "dogrulama_aylar": dog,
        "hatalar": [], "deneme": {}, "eksik": {}, "atlanan_coin": [],
    }
    notify("\n".join([
        f"🧪 Varyant testi başladı (istek {istek})",
        "Strateji: " + _strateji_ozeti(strat),
        f"Varyantlar: {', '.join(plan['varyantlar'])} + RASTGELE ({plan['rastgele_tekrar']} tekrar, sabit tohum)",
        f"Veri: Binance USDⓈ-M Futures {plan['veri'][0]} – {plan['veri'][1]}, {len(d['islem_sembolleri'])} coin, "
        f"mumlar {'+'.join(zfs)}; her coin ilk {plan['isinma_mum']} mum ısınma",
        f"Geliştirme: {gel[0]} – {gel[-1]} ({len(gel)} ay) | Doğrulama: "
        + (f"{dog[0]} – {dog[-1]} (sadece seçilen varyant, bir kez)" if dogr else "KAPALI"),
        "Maliyet: taker %0,05" + (", maker %0,02 (V2 girişi)" if "V2" in plan["varyantlar"] else "")
        + ", kayma BTC/ETH %0,02 · diğer %0,05, funding 8 saatte %0,01.",
        f"Portföy: {bt.MAX_OPEN_POSITIONS} açık / yönde {bt.MAX_SAME_DIRECTION} / coinde 1 / günde {plan['gunluk_islem']} işlem "
        f"/ günlük {plan['gunluk_zarar_r']:g}R'de dur.",
    ]))
    return d


def _v_birimler(d):
    ok = [b for _, b in d["semboller"] if b not in d["atlanan_coin"]]
    if d["asama"] == "indir":
        return [(b, tf) for b in ok for tf in d["zfs"]]
    if d["asama"] in ("sinyal", "simule"):
        return [(b,) for b in ok if b in d["islem_sembolleri"]]
    if d["asama"] == "rapor":
        return [("rapor",)]
    return []


def _v_sonraki_asama(d, notify):
    a = d["asama"]
    gecen = (time.time() - d["baslangic"]) / 60
    if a == "indir":
        coin = len([b for b in d["islem_sembolleri"] if b not in d["atlanan_coin"]])
        satir = [f"📥 Varyant testi: veri hazır ({coin}/{len(d['islem_sembolleri'])} coin, {gecen:.0f} dk)"]
        if d["eksik"]:
            satir.append("Eksik aylar (listelenmeden önceki aylar dahil): " + "; ".join(
                f"{k}: {v[0]}…{v[-1]} ({len(v)} ay)" for k, v in list(d["eksik"].items())[:12]))
        if d["atlanan_coin"]:
            satir.append("Dışarıda kalan coin: " + ", ".join(d["atlanan_coin"]))
        notify("\n".join(satir))
        d["asama"] = "sinyal"
    elif a == "sinyal":
        say = {"V0": 0, "V1": 0}
        for b in _v_birimler(d):
            for x in (_json_oku(os.path.join(SINYAL, f"vt_{b[0]}.json"), {}) or {}).get("sinyaller", []):
                say["V0"] += 1
                say["V1"] += bool(x["v1"])
        notify(f"📶 Varyant testi: sinyaller hesaplandı – V0/V2 {say['V0']}, V1 {say['V1']} sinyal, {gecen:.0f} dk. "
               "Şimdi işlemler işletiliyor.")
        d["asama"] = "simule"
    elif a == "simule":
        d["asama"] = "rapor"
    elif a == "rapor":
        d["asama"] = "bitti"
    d["sira"] = 0
    d["alt"] = 0


def _gunluk(bsym, tf_kaynak="1h"):
    """1h mumlarından UTC günlük mumlar (Binance 1d ile aynı sınırlar) + ortak indikatörler."""
    v = _yukle(bsym, tf_kaynak)
    gun = v["t"] // 86400 * 86400
    df = pd.DataFrame({"g": gun, "o": v["o"], "h": v["h"], "l": v["l"], "c": v["c"]})
    g = df.groupby("g", sort=True)
    gv = {"t": np.array(g.size().index, dtype=np.int64), "o": g["o"].first().to_numpy(), "h": g["h"].max().to_numpy(),
          "l": g["l"].min().to_numpy(), "c": g["c"].last().to_numpy()}
    _, kol = _df(gv)
    return gv["t"], kol


def _v_sinyal(d, b):
    bsym = b[0]
    os.makedirs(SINYAL, exist_ok=True)
    plan = _plan(d)
    strat = _strateji(d)
    tf, ust = strat.ZAMAN_DILIMI, getattr(strat, "UST_ZAMAN_DILIMI", None)
    sec = SEC[tf]
    v = _yukle(bsym, tf)
    _, kol = _df(v, strat)
    t = v["t"]
    n = len(t)
    # Aday: kırılım mumu kuralları (stratejinin kendi giris()'i, sıkışma ve deneme şartı geçici olarak kaldırılmış)
    kol_aday = dict(kol)
    kol_aday["sikisma"] = np.ones(n)
    kol_aday["deneme"] = np.zeros(n)
    ust_oku = _ust_okuyucu(bsym, ust)
    bt_gt, bt_gk = _gunluk("BTCUSDT") if os.path.exists(_veri_yolu("BTCUSDT", "1h")) else (None, None)
    gt, gk = _gunluk(bsym)

    def gunluk_bilgi(T):
        """Sinyal anında kapanmış son günlük mum: BTC trendi ve coinin günlük ATR%'si."""
        out = {"btc_gunluk": None, "gunluk_atr_pct": None}
        j = int(np.searchsorted(gt, T // 86400 * 86400)) - 1
        if j >= 0:
            a_, c_ = float(gk["atr"][j]), float(gk["close"][j])
            if math.isfinite(a_) and c_ > 0:
                out["gunluk_atr_pct"] = round(a_ / c_ * 100, 3)
        if bt_gt is not None:
            jb = int(np.searchsorted(bt_gt, T // 86400 * 86400)) - 1
            if jb >= 30:
                out["btc_gunluk"] = sb.market_state(_Pencere(bt_gk, max(0, jb - 198), jb + 2), jb - max(0, jb - 198))
        return out

    bas = _ay_bas_ts(d["ayar_aylar"][0])
    sinir = _ay_bas_ts(d["dogrulama_aylar"][0])
    son = _ay_bas_ts("%d-%02d" % _ay_kaydir(*map(int, d["dogrulama_aylar"][-1].split("-")), 1)) \
        if d.get("dogrulama") else sinir
    gecici = os.path.join(SINYAL, f"vt_{bsym}.yarim.json")
    kayit = (_json_oku(gecici, {}) or {}) if d["alt"] else {}
    liste = kayit.get("sinyaller", [])
    sebep = kayit.get("sebep", {})
    eski_varyant = getattr(strat, "VARYANT", None)
    kutu = int(getattr(strat, "KUTU", 16))
    i0 = max(plan["isinma_mum"], 199, d["alt"])
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            for i in range(i0, n - 1):
                if (i - i0) % 2000 == 0 and i > i0 and _kalan() < 3:
                    d["alt"] = i
                    _json_yaz(gecici, {"sinyaller": liste, "sebep": sebep})
                    return False
                T = int(t[i]) + sec
                if T < bas or T >= son or int(t[i + 1]) != T:
                    continue
                a = i - 198
                strat.VARYANT = "V0"
                aday = strat.giris(_Pencere(kol_aday, a, i + 2), 198, None)
                if not aday:
                    continue
                egitim = T < sinir

                def say(nd):
                    if egitim:
                        sebep[nd] = sebep.get(nd, 0) + 1
                if float(kol["sikisma"][i]) != 1:
                    say("sıkışma yok")
                    continue
                if float(np.nanmax(kol["deneme"][i - kutu:i])) == 1:
                    say("kutu tükenmiş")
                    continue
                pen = _Pencere(kol, a, i + 2)
                ust_t, ust_g = ust_oku(T) if ust else (None, None)
                strat.VARYANT = "V0"
                a0 = sb.check_alert(strat, pen, ust_t)
                if not a0:
                    say("risk dışı")
                    continue
                strat.VARYANT = "V1"
                a1 = sb.check_alert(strat, pen, ust_t)
                if not a1:
                    say("filtre (V1)")
                seviye, orta = (strat.retest_seviyesi(pen, 198, a0["side"]) if "V2" in plan["varyantlar"]
                                 else (None, None))
                sig = {"T": T, "i": i, "tf": tf, "sym": bsym, "side": a0["side"], "entry": float(a0["entry"]),
                       "stop": float(a0["stop"]), "target": None if a0.get("target") is None else float(a0["target"]),
                       "src": "+".join(a0.get("sources") or [strat.KOD]), "v1": bool(a1),
                       "seviye": None if seviye is None else float(seviye),
                       "orta": None if orta is None else float(orta), "atr": float(kol["atr"][i]),
                       "kh": float(kol["kh"][i]), "kl": float(kol["kl"][i])}
                sig.update(_baglam(kol, i, a0["side"], sig["entry"], sig["stop"], ust_t, ust_g, None, None))
                sig.update(gunluk_bilgi(T))
                liste.append(sig)
    finally:
        strat.VARYANT = eski_varyant
    _json_yaz(os.path.join(SINYAL, f"vt_{bsym}.json"), {"strateji": strat.KOD, "sinyaller": liste, "sebep": sebep})
    if os.path.exists(gecici):
        os.remove(gecici)
    return True


def _v_yol(v, k_bas, side, giris, stop, tp, sure, dolum_mumu, kesim, sec):
    """
    Bir işlemin yolu (maliyetsiz). k_bas: girişin olduğu mum. dolum_mumu: True ise giriş mum içinde (V2 limit)
    ve o mumda sadece stop kontrol edilir. Dönüş: (çıkış fiyatı, çıkış türü, çıkış zamanı, mfe, mae, piyasa_cikisi).
    """
    t, O, H, L, C = v["t"], v["o"], v["h"], v["l"], v["c"]
    s = 1 if side == "AL" else -1
    dist = abs(giris - stop)
    mfe = mae = 0.0
    k = k_bas
    while True:
        tk = int(t[k])
        if kesim is not None and tk >= kesim:
            return float(O[k]), "DÖNEM", tk + 10, mfe, mae
        if sure and k - k_bas >= sure:
            return float(O[k]), "SÜRE", tk + 10, mfe, mae
        op, hi, lo, cl = float(O[k]), float(H[k]), float(L[k]), float(C[k])
        ilk = dolum_mumu and k == k_bas
        if (s == 1 and lo <= stop) or (s == -1 and hi >= stop):
            bosluk = (not ilk) and s * (op - stop) <= 0
            px = op if bosluk else stop
            return px, "SL", tk + (10 if bosluk else sec // 2), mfe, max(mae, abs(px - giris) / dist)
        if ilk:
            mfe = max(mfe, s * (cl - giris) / dist)
            mae = max(mae, (-s) * (cl - giris) / dist, 0.0)
        else:
            mfe = max(mfe, ((hi - giris) if s == 1 else (giris - lo)) / dist)
            mae = max(mae, ((giris - lo) if s == 1 else (hi - giris)) / dist)
            if tp is not None and ((s == 1 and hi >= tp) or (s == -1 and lo <= tp)):
                return (op if s * (op - tp) >= 0 else tp), "TP", tk + sec // 2, mfe, mae
        if k >= len(t) - 1:
            return cl, "SON", tk + sec + 10, mfe, mae
        k += 1


def _v_maliyet(plan, bsym, side, giris, px, dist, t_giris, t_cikis, giris_limit):
    s = 1 if side == "AL" else -1
    kayma = plan["kayma"].get(bsym, plan["kayma"]["diger"])
    r_brut = s * (px - giris) / dist
    kayma_r = ((0 if giris_limit else giris * kayma) + px * kayma) / dist
    kom_r = (giris * (plan["maker"] if giris_limit else plan["taker"]) + px * plan["taker"]) / dist
    n_fund = int(t_cikis // 28800 - (t_giris + 20) // 28800)          # geçilen 00/08/16 UTC sayısı
    fund_r = max(0, n_fund) * plan["funding_8s"] * giris / dist
    r = r_brut - kayma_r - kom_r - fund_r
    return {"r": round(r, 4), "r_brut": round(r_brut, 4), "kayma_r": round(kayma_r, 4), "kom_r": round(kom_r, 4),
            "fund_r": round(fund_r, 4), "n_fund": max(0, n_fund)}


def _v_simule(d, b):
    bsym = b[0]
    os.makedirs(SONUC, exist_ok=True)
    plan = _plan(d)
    strat = _strateji(d)
    tf = strat.ZAMAN_DILIMI
    sec = SEC[tf]
    j_ = _json_oku(os.path.join(SINYAL, f"vt_{bsym}.json"), {}) or {}
    sinyaller = j_.get("sinyaller", [])
    sinir = _ay_bas_ts(d["dogrulama_aylar"][0])
    v = _yukle(bsym, tf)
    _, kol = _df(v, strat)
    t, O, H, L, C = v["t"], v["o"], v["h"], v["l"], v["c"]
    sure = strat.CIKIS.get("sure_mum")
    tp_r = strat.CIKIS.get("tp_r")
    kayitlar = []
    # Yazı-tura: her sinyal mumu için kendi sabit tohumu (coin + mum) → dönem/ayar değişse de aynı mumun yönleri aynı
    rast = np.array([np.random.default_rng([plan["rastgele_tohum"], sum(map(ord, bsym)), int(sg["i"])])
                     .integers(0, 2, plan["rastgele_tekrar"]) for sg in sinyaller], dtype=np.int8).T \
        if sinyaller else np.zeros((plan["rastgele_tekrar"], 0), dtype=np.int8)   # 1 = sinyal yönü, 0 = ters
    for j, sig in enumerate(sinyaller):
        i = sig["i"]
        k0 = i + 1
        s = 1 if sig["side"] == "AL" else -1
        kesim = sinir if sig["T"] < sinir else None
        x = {k: sig.get(k) for k in ("T", "i", "sym", "tf", "side", "src", "v1", "btc_gunluk", "gunluk_atr_pct")
             + tuple(BAGLAM)}
        # Sahte kırılım: sonraki 4 mumda kutu ortasının ötesine kapanış
        son_k = min(len(t), k0 + plan["sahte_kirilim_mum"])
        orta_k = sig["orta"] if sig.get("orta") is not None else (sig["kh"] + sig["kl"]) / 2
        x["sahte"] = bool(any(s * (C[k] - orta_k) < 0 for k in range(k0, son_k)))
        # --- V0 / V1 / RASTGELE: sonraki mumun açılışında piyasa emri
        giris = float(O[k0])
        dist = abs(giris - sig["stop"])
        sonuc = {}
        if s * (giris - sig["stop"]) <= 0 or (sig["target"] is not None and s * (sig["target"] - giris) <= 0):
            x["v0"] = {"atla": "fiyat stop/TP'nin ötesinde açıldı"}
            x["ters"] = None
        else:
            tp_mesafe = abs(sig["target"] - giris) if sig["target"] is not None else None
            for yon_k, side in (("v0", sig["side"]), ("ters", "SAT" if sig["side"] == "AL" else "AL")):
                ss = 1 if side == "AL" else -1
                st_ = giris - ss * dist
                tp_ = giris + ss * tp_mesafe if tp_mesafe is not None else None
                px, cik, tc, mfe, mae = _v_yol(v, k0, side, giris, st_, tp_, sure, False, kesim, sec)
                r = {"side": side, "giris_t": sig["T"] + 20, "cikis_t": tc, "cikis": cik, "giris_px": giris,
                     "cikis_px": px, "stop": st_, "mfe": round(mfe, 3), "mae": round(mae, 3)}
                r.update(_v_maliyet(plan, bsym, side, giris, px, dist, sig["T"], tc, False))
                sonuc[yon_k] = r
            x["v0"], x["ters"] = sonuc["v0"], sonuc["ters"]
        # --- V2: limit emir (retest) – sadece planda V2 varsa
        lvl, orta = sig["seviye"], sig["orta"]
        x["v2"] = {"atla": "retest dolmadı"} if "V2" in plan["varyantlar"] else None
        for k in (range(k0, min(len(t), k0 + plan["retest_mum"])) if "V2" in plan["varyantlar"] else ()):
            op, hi, lo, cl = float(O[k]), float(H[k]), float(L[k]), float(C[k])
            if (s == 1 and lo < lvl) or (s == -1 and hi > lvl):            # seviyenin ötesine geçti → doldu
                fill = min(op, lvl) if s == 1 else max(op, lvl)
                pen = _Pencere(kol, i - 198, i + 2)
                st2 = strat.stop(pen, 198, sig["side"], fill, sig["atr"])
                if st2 is None or s * (fill - st2) <= 0:
                    x["v2"] = {"atla": "retest risk dışı"}
                    break
                d2 = abs(fill - st2)
                tp2 = fill + s * tp_r * d2 if tp_r else None
                px, cik, tc, mfe, mae = _v_yol(v, k, sig["side"], fill, st2, tp2, sure, True, kesim, sec)
                tg = int(t[k]) + sec // 2
                r = {"side": sig["side"], "giris_t": tg, "cikis_t": tc, "cikis": cik, "giris_px": fill, "cikis_px": px,
                     "stop": st2, "mfe": round(mfe, 3), "mae": round(mae, 3), "bekleme_mum": k - k0 + 1}
                r.update(_v_maliyet(plan, bsym, sig["side"], fill, px, d2, tg, tc, True))
                x["v2"] = r
                break
            if s * (cl - orta) < 0:                                        # dolmadan kutu ortasının ötesine kapandı
                x["v2"] = {"atla": "retest iptal (kutu ortası)"}
                break
        kayitlar.append(x)
    _json_yaz(os.path.join(SONUC, f"vt_{bsym}.json"),
              {"kayitlar": kayitlar, "rastgele": rast.tolist(), "sebep": j_.get("sebep", {})})
    return True


# ---- Portföy, istatistik ----
def _v_portfoy(adaylar, plan):
    """adaylar: dict listesi (sym, side, giris_t, cikis_t, r). Dönüş: (alınanlar, sebep sayıları)."""
    sirali = sorted(adaylar, key=lambda x: (x["giris_t"], x["sym"]))
    acik, alinan = [], []
    gun_r, gun_say = {}, {}
    neden = {"aynı coin": 0, "pozisyon sınırı": 0, "yön sınırı": 0, "günlük işlem sınırı": 0, "günlük zarar sınırı": 0}
    for x in sirali:
        te = x["giris_t"]
        kalan = []
        for a in acik:
            if a["cikis_t"] <= te:
                g = a["cikis_t"] // 86400
                gun_r[g] = gun_r.get(g, 0.0) + a["r"]
            else:
                kalan.append(a)
        acik = kalan
        g = te // 86400
        if any(a["sym"] == x["sym"] for a in acik):
            neden["aynı coin"] += 1
        elif len(acik) >= bt.MAX_OPEN_POSITIONS:
            neden["pozisyon sınırı"] += 1
        elif bt.MAX_SAME_DIRECTION and sum(a["side"] == x["side"] for a in acik) >= bt.MAX_SAME_DIRECTION:
            neden["yön sınırı"] += 1
        elif gun_say.get(g, 0) >= plan["gunluk_islem"]:
            neden["günlük işlem sınırı"] += 1
        elif gun_r.get(g, 0.0) <= plan["gunluk_zarar_r"]:
            neden["günlük zarar sınırı"] += 1
        else:
            acik.append(x)
            alinan.append(x)
            gun_say[g] = gun_say.get(g, 0) + 1
    return alinan, neden


def _v_ist(tr, aylar):
    s = {"n": len(tr)}
    if not tr:
        return s
    r = np.array([x["r"] for x in tr])
    rb = np.array([x["r_brut"] for x in tr])
    s["ort_r"], s["ort_brut"] = float(r.mean()), float(rb.mean())
    s["toplam_r"] = float(r.sum())
    kay = r[r <= 0].sum()
    s["pf"] = float(r[r > 0].sum() / -kay) if kay < 0 else float("inf")
    kb = rb[rb <= 0].sum()
    s["pf_brut"] = float(rb[rb > 0].sum() / -kb) if kb < 0 else float("inf")
    s["kazanma"] = float((r > 0).mean())
    s["ort_kazanc"] = float(r[r > 0].mean()) if (r > 0).any() else None
    s["ort_kayip"] = float(r[r <= 0].mean()) if (r <= 0).any() else None
    s["ga"] = _k_blok_ga([{"T": x["T"], "r": x["r"]} for x in tr], aylar)
    s["ga_brut"] = _k_blok_ga([{"T": x["T"], "r": x["r_brut"]} for x in tr], aylar)
    kap = sorted(tr, key=lambda x: x["cikis_t"])
    s["dd_r"] = _dd([x["r"] for x in kap])
    seri = en = 0
    for x in kap:
        seri = seri + 1 if x["r"] <= 0 else 0
        en = max(en, seri)
    s["seri"] = en
    cey = {}
    for a in aylar:
        cey.setdefault(_k_ceyrek(_ay_bas_ts(a)), [])
    for x in tr:
        cey.setdefault(_k_ceyrek(x["T"]), []).append(x["r"])
    s["ceyrekler"] = {c: (len(v_), float(sum(v_))) for c, v_ in cey.items()}
    s["karli_ceyrek"] = sum(1 for n_, t_ in s["ceyrekler"].values() if t_ > 0)
    s["ceyrek_say"] = len(cey)
    for k in ("kayma_r", "kom_r", "fund_r"):
        s[k] = float(np.mean([x[k] for x in tr]))
    cik = {}
    for x in tr:
        cik[x["cikis"]] = cik.get(x["cikis"], 0) + 1
    s["cikis"] = cik
    s["sahte"] = float(np.mean([x["sahte"] for x in tr]))
    s["ort_dk"] = float(np.mean([(x["cikis_t"] - x["giris_t"]) / 60 for x in tr]))
    return s


def _v_ozet(ad, s):
    if not s["n"]:
        return [f"{ad}: işlem yok"]
    ga = f"{_r(s['ga'][0])} … {_r(s['ga'][1])}" if s.get("ga") else "–"
    gab = f"{_r(s['ga_brut'][0])} … {_r(s['ga_brut'][1])}" if s.get("ga_brut") else "–"
    return [
        f"{ad}: {s['n']} işlem | {_r(s['ort_r'])}/işlem (maliyetsiz {_r(s['ort_brut'])}) | aylık blok %95 GA {ga} "
        f"(maliyetsiz {gab}) | PF {_f2(s['pf'])} (maliyetsiz {_f2(s['pf_brut'])}) | kazanma {_pct(s['kazanma'])}",
        f"   net {_r(s['toplam_r'])} ({_usd(s['toplam_r'] * bt.MARGIN_USDT * bt.LOSS_PCT_OF_MARGIN)} USDT) | "
        f"en büyük düşüş {_f2(s['dd_r'])}R | en uzun kayıp serisi {s['seri']} | ort. kazanç {_r(s['ort_kazanc'])} · "
        f"ort. kayıp {_r(s['ort_kayip'])} | kârlı çeyrek {s['karli_ceyrek']}/{s['ceyrek_say']}",
        f"   maliyet/işlem: komisyon {_r(-s['kom_r'])} · kayma {_r(-s['kayma_r'])} · funding {_r(-s['fund_r'])} | "
        "çıkış: " + " · ".join(f"{k} {v_}" for k, v_ in sorted(s["cikis"].items(), key=lambda kv: -kv[1]))
        + f" | sahte kırılım {_pct(s['sahte'])} | ort. süre {_f2(s['ort_dk'] / 60)} saat",
    ]


def _v_kisa(ad, tr):
    if not tr:
        return f"   {ad}: işlem yok"
    r = np.array([x["r"] for x in tr])
    kay = r[r <= 0].sum()
    pf = r[r > 0].sum() / -kay if kay < 0 else float("inf")
    return (f"   {ad}: {len(tr)} işlem | {_r(r.mean())}/işlem (maliyetsiz {_r(np.mean([x['r_brut'] for x in tr]))}) | "
            f"PF {_f2(pf)} | kazanma {_pct((r > 0).mean())} | toplam {_r(r.sum())}")


def _v_tstat(tr, rast_ort):
    """Varyant işlemleri ile aynı sinyal mumlarındaki RASTGELE ortalaması arasındaki farkın (eşleştirilmiş) t değeri."""
    fark = [x["r"] - rast_ort[(x["sym"], x["i"])] for x in tr if (x["sym"], x["i"]) in rast_ort]
    if len(fark) < 3:
        return None, len(fark), None
    f = np.array(fark)
    sd = f.std(ddof=1)
    return (float(f.mean() / (sd / math.sqrt(len(f)))) if sd > 0 else None), len(f), float(f.mean())


def _v_rapor(d, b):
    plan = _plan(d)
    strat = _strateji(d)
    sinir = _ay_bas_ts(d["dogrulama_aylar"][0])
    gel_aylar, dog_aylar = d["ayar_aylar"], d["dogrulama_aylar"]
    dogr_acik = bool(d.get("dogrulama"))
    kayit_hepsi, rast_hepsi, sebep = [], [], {}
    for b_ in _v_birimler(dict(d, asama="simule")):
        j_ = _json_oku(os.path.join(SONUC, f"vt_{b_[0]}.json"), {}) or {}
        ks = j_.get("kayitlar", [])
        if not dogr_acik:
            ks = [x for x in ks if x["T"] < sinir]
        kayit_hepsi += ks
        rr = np.array(j_.get("rastgele", []), dtype=np.int8).reshape(-1, len(j_.get("kayitlar", []))) \
            if j_.get("kayitlar") else np.zeros((plan["rastgele_tekrar"], 0), dtype=np.int8)
        rast_hepsi.append((ks, rr[:, :len(ks)]))
        for k, v_ in j_.get("sebep", {}).items():
            sebep[k] = sebep.get(k, 0) + v_

    def islem(x, anahtar):
        y = x.get(anahtar)
        if not y or "atla" in y:
            return None
        z = dict(y)
        z.update({k: x.get(k) for k in ("T", "i", "sym", "tf", "src", "sahte", "btc_gunluk", "gunluk_atr_pct")
                  + tuple(BAGLAM)})
        return z

    gel_k = [x for x in kayit_hepsi if x["T"] < sinir]
    adaylar = {
        "V0": [islem(x, "v0") for x in gel_k],
        "V1": [islem(x, "v0") for x in gel_k if x["v1"]],
        "V2": [islem(x, "v2") for x in gel_k],
    }
    adaylar = {k: [y for y in v_ if y] for k, v_ in adaylar.items()}
    atla = {"V0": {}, "V1": {}, "V2": {}}
    for x in gel_k:
        for var, ana in (("V0", "v0"), ("V1", "v0"), ("V2", "v2")):
            if var == "V1" and not x["v1"]:
                continue
            y = x.get(ana) or {}
            if "atla" in y:
                atla[var][y["atla"]] = atla[var].get(y["atla"], 0) + 1

    # RASTGELE: her sinyal mumunda iki yönün sonucu hazır; tekrarlar yönü seçer
    rast_ort = {}
    rast_tekrar_sinirli, rast_tekrar_sinirsiz = [], []
    cift = []                                         # (x, v0 sonucu, ters sonucu, yön seçimleri)
    for ks, rr in rast_hepsi:
        for j, x in enumerate(ks):
            if x["T"] >= sinir or not x.get("ters"):
                continue
            a0, a1 = islem(x, "v0"), islem(x, "ters")
            cift.append((a0, a1, rr[:, j]))
            rast_ort[(x["sym"], x["i"])] = float(np.mean(np.where(rr[:, j] == 1, a0["r"], a1["r"])))
    for k in range(plan["rastgele_tekrar"]):
        tr = [(a0 if sec_[k] == 1 else a1) for a0, a1, sec_ in cift]
        rast_tekrar_sinirsiz.append(float(np.mean([x["r"] for x in tr])) if tr else 0.0)
        al, _ = _v_portfoy(tr, plan)
        rast_tekrar_sinirli.append(float(np.mean([x["r"] for x in al])) if al else 0.0)

    sonuc = {}
    for var in plan["varyantlar"]:
        al, neden = _v_portfoy(adaylar[var], plan)
        st = _v_ist(al, gel_aylar)
        tval, tn, tfark = _v_tstat(al, rast_ort)
        kural = []
        if st["n"]:
            kural = [
                ("Ortalama ≥ +0,10R", st["ort_r"] >= KARAR_ORT_R, _r(st["ort_r"])),
                ("Aylık blok bootstrap %95 GA alt sınırı > 0", st["ga"] is not None and st["ga"][0] > 0,
                 _r(st["ga"][0]) if st["ga"] else "–"),
                (f"Kâr faktörü ≥ {_f2(KARAR_PF)}", st["pf"] >= KARAR_PF, _f2(st["pf"])),
                (f"Kârlı çeyrek ≥ %{KARAR_KARLI_CEYREK * 100:.0f}",
                 st["karli_ceyrek"] >= math.ceil(KARAR_KARLI_CEYREK * st["ceyrek_say"] - 1e-9),
                 f"{st['karli_ceyrek']}/{st['ceyrek_say']}"),
                ("RASTGELE'den fark t ≥ 2", tval is not None and tval >= 2,
                 f"t = {_f2(tval)} (fark {_r(tfark)}, {tn} işlem)"),
            ]
        sonuc[var] = {"al": al, "neden": neden, "st": st, "kural": kural,
                      "gecti": bool(kural) and all(k[1] for k in kural),
                      "sinirsiz": _v_ist(adaylar[var], gel_aylar)}
    gecen = [v_ for v_ in plan["varyantlar"] if sonuc[v_]["gecti"]]
    secilen = max(gecen, key=lambda v_: sonuc[v_]["st"]["ort_r"]) if gecen else None

    kd = _json_oku(DOGRULAMA_KAYDI, {}) or {}
    kayitli = kd.get("modlar", {})
    anahtar_k = f"{strat.KOD}:{secilen}" if secilen else None
    onceki = kayitli.get(anahtar_k) if anahtar_k else None

    R_USDT = bt.MARGIN_USDT * bt.LOSS_PCT_OF_MARGIN
    satir = [
        f"🧪 VARYANT TESTİ (istek {d['istek']}) – {_ts_str(time.time())}",
        "🔧 Strateji: " + _strateji_ozeti(strat),
        f"Doğrulama: {'AÇIK (sadece seçilen varyant)' if dogr_acik else 'KAPALI'} | Geliştirme {gel_aylar[0]} – "
        f"{gel_aylar[-1]} ({len(gel_aylar)} ay) | {len(d['islem_sembolleri'])} coin | 1R = {R_USDT:.0f} USDT",
        "Maliyet dahil: taker %0,05/taraf" + (", maker %0,02 (V2 girişi)" if "V2" in plan["varyantlar"] else "")
        + ", kayma BTC/ETH %0,02 · diğer %0,05 (piyasa emri başına), funding 00/08/16 UTC başına %0,01.",
        f"Portföy: {bt.MAX_OPEN_POSITIONS} açık / yönde {bt.MAX_SAME_DIRECTION} / coinde 1 / günde {plan['gunluk_islem']} işlem / "
        f"gün içi {plan['gunluk_zarar_r']:g}R'de yeni işlem yok.",
        "",
        "1) KARAR KURALLARI – geliştirme dönemi, maliyet ve portföy sınırları dahil, her varyant ayrı",
    ]
    for var in plan["varyantlar"]:
        satir.append(f"{var}: " + ("✅ GEÇTİ" if sonuc[var]["gecti"] else "❌ KALDI"))
        satir += [f"  {'✅' if ok else '❌'} {ad}: {deger}" for ad, ok, deger in sonuc[var]["kural"]] or ["  işlem yok"]
    satir.append("➡️ " + (f"Seçilen: {secilen} (geçenler arasında ortalama R'si en yüksek). Doğrulama sadece bunun için bir kez açılır."
                          if secilen else "Hiçbir varyant geçmedi. Plan gereği sonuca göre parametre, saat ya da coin filtresi önerilmez."))

    satir += ["", "2) SONUÇLAR – portföy sınırlarıyla"]
    for var in plan["varyantlar"]:
        satir += _v_ozet(var, sonuc[var]["st"])
        satir.append("   portföy nedeniyle alınmayan: " + " · ".join(f"{k} {v_}" for k, v_ in sonuc[var]["neden"].items()))
    rs, rf = np.array(rast_tekrar_sinirli), np.array(rast_tekrar_sinirsiz)
    satir.append(f"RASTGELE ({plan['rastgele_tekrar']} tekrar, V0 sinyal mumları, yön yazı-tura): portföylü ortalama "
                 f"{_r(float(rs.mean()))}/işlem (tekrarların %2,5–97,5'i {_r(float(np.percentile(rs, 2.5)))} … "
                 f"{_r(float(np.percentile(rs, 97.5)))}); sınırsız {_r(float(rf.mean()))} "
                 f"({_r(float(np.percentile(rf, 2.5)))} … {_r(float(np.percentile(rf, 97.5)))})")
    for var in plan["varyantlar"]:
        st = sonuc[var]["st"]
        if st["n"]:
            yuzde = float((rs >= st["ort_r"]).mean())
            satir.append(f"   {var} ortalaması RASTGELE tekrarlarının %{(1 - yuzde) * 100:.1f}'inden yüksek".replace(".", ","))

    satir += ["", "3) SINIRSIZ – her sinyal bağımsız (portföy sınırı yok)"]
    for var in plan["varyantlar"]:
        satir += _v_ozet(f"{var} (sınırsız)", sonuc[var]["sinirsiz"])

    satir += ["", "4) NO TRADE SEBEPLERİ – geliştirme dönemi (aday: kırılım mumu kurallarını geçen mum)"]
    v0_sinyal = len(gel_k)
    aday = sebep.get("sıkışma yok", 0) + sebep.get("kutu tükenmiş", 0) + sebep.get("risk dışı", 0) + v0_sinyal
    satir.append(f"  aday {aday}: sıkışma yok {sebep.get('sıkışma yok', 0)} · "
                 f"kutu tükenmiş {sebep.get('kutu tükenmiş', 0)} · risk dışı {sebep.get('risk dışı', 0)} · V0 sinyali {v0_sinyal}")
    for var in plan["varyantlar"]:
        parca = []
        if var == "V1":
            parca.append(f"filtre {sebep.get('filtre (V1)', 0)}")
        parca += [f"{k} {v_}" for k, v_ in atla[var].items()]
        parca.append(f"portföy {sum(sonuc[var]['neden'].values())}")
        satir.append(f"  {var}: " + " · ".join(parca) + f" → işlem {sonuc[var]['st']['n']}")

    for var in plan["varyantlar"]:
        al = sonuc[var]["al"]
        if not al:
            continue
        satir += ["", f"5) DAĞILIMLAR – {var} (portföy sınırlarıyla)"]
        satir.append(_v_kisa("LONG", [x for x in al if x["side"] == "AL"]))
        satir.append(_v_kisa("SHORT", [x for x in al if x["side"] == "SAT"]))
        mf, ma = np.array([x["mfe"] for x in al]), np.array([x["mae"] for x in al])
        satir.append(f"  MFE (R): medyan {_f2(float(np.median(mf)))} · ≥1R {_pct(float((mf >= 1).mean()))} · "
                     f"≥2R {_pct(float((mf >= 2).mean()))} · ≥3R {_pct(float((mf >= 3).mean()))} | MAE (R): medyan "
                     f"{_f2(float(np.median(ma)))} · ≥0,5R {_pct(float((ma >= 0.5).mean()))}")
        tp_ = [x for x in al if x["cikis"] == "TP"]
        sr = [x for x in al if x["cikis"] == "SÜRE"]
        if sr:
            satir.append(f"  SÜRE çıkışları: ort. {_r(float(np.mean([x['r'] for x in sr])))}, medyan MFE "
                         f"{_f2(float(np.median([x['mfe'] for x in sr])))}R" + (f" | TP {len(tp_)}" if tp_ else ""))
        satir.append("  Coin:")
        for c in d["islem_sembolleri"]:
            g = [x for x in al if x["sym"] == c]
            if g:
                satir.append(_v_kisa(c, g))
        satir.append("  Yıllık:")
        for y in sorted({_ts_ay(x["T"])[:4] for x in al}):
            satir.append(_v_kisa(y, [x for x in al if _ts_ay(x["T"])[:4] == y]))
        satir.append("  Çeyreklik: " + " | ".join(
            f"{c} {n_}/{_r(t_)}" for c, (n_, t_) in sonuc[var]["st"]["ceyrekler"].items()))
        satir.append("  BTC günlük trendi:")
        for k, ad in (("YÜKSELİŞ", "yükseliş"), ("DÜŞÜŞ", "düşüş"), ("YATAY", "yatay")):
            satir.append(_v_kisa(ad, [x for x in al if x.get("btc_gunluk") == k]))
        satir.append("  Oynaklık (coinin günlük ATR%'sinin geliştirme dönemindeki üçte biri):")
        esik = {}
        for c in d["islem_sembolleri"]:
            vals = [x["gunluk_atr_pct"] for x in kayit_hepsi if x["sym"] == c and x["T"] < sinir
                    and x.get("gunluk_atr_pct") is not None]
            if len(vals) >= 3:
                esik[c] = np.percentile(vals, [33.3, 66.7])
        for ad, f_ in (("düşük", lambda x, e: x < e[0]), ("orta", lambda x, e: e[0] <= x < e[1]),
                       ("yüksek", lambda x, e: x >= e[1])):
            satir.append(_v_kisa(ad, [x for x in al if x["sym"] in esik and x.get("gunluk_atr_pct") is not None
                                      and f_(x["gunluk_atr_pct"], esik[x["sym"]])]))
        satir.append("  UTC saat (sadece bilgi):")
        for a_, b2 in ((0, 8), (8, 16), (16, 24)):
            satir.append(_v_kisa(f"{a_:02d}–{b2:02d}", [x for x in al if a_ <= (x["giris_t"] % 86400) // 3600 < b2]))

    if secilen:
        al = sonuc[secilen]["al"]
        ay_basina = max(1, round(len(al) / len(gel_aylar)))
        satir += ["", f"6) CANLI İZLEME EŞİKLERİ – {secilen}, Monte Carlo %95 (DURMA için)"]
        for u_, dd95, seri95 in _monte_carlo([dict(x, net=x["r"] * R_USDT) for x in al],
                                             sorted({30, 60, 100, ay_basina * 3})):
            satir.append(f"  {u_} işlem: düşüş {_usd(-dd95)} USDT ({_f2(dd95 / R_USDT)}R) | kayıp serisi {seri95}")

    gecti_d = None
    if dogr_acik and secilen:
        dk = [x for x in kayit_hepsi if x["T"] >= sinir]
        ana = "v2" if secilen == "V2" else "v0"
        ad_ = [islem(x, ana) for x in dk if secilen != "V1" or x["v1"]]
        al_d, _ = _v_portfoy([y for y in ad_ if y], plan)
        sd = _v_ist(al_d, dog_aylar)
        satir += ["", f"7) DOĞRULAMA – {secilen}, tek sefer ({dog_aylar[0]} – {dog_aylar[-1]})"]
        if onceki:
            satir.append(f"⚠️ Bu varyantın doğrulaması ilk kez {onceki.get('tarih')} tarihinde görüldü; bu tekrar bakıştır.")
        satir += _v_ozet(secilen, sd)
        if sd["n"]:
            kural = [("Ortalama ≥ +0,10R", sd["ort_r"] >= KARAR_ORT_R, _r(sd["ort_r"])),
                     ("Aylık blok %95 GA alt sınırı > 0", sd["ga"] is not None and sd["ga"][0] > 0,
                      _r(sd["ga"][0]) if sd["ga"] else "–"),
                     (f"Kâr faktörü ≥ {_f2(KARAR_PF)}", sd["pf"] >= KARAR_PF, _f2(sd["pf"])),
                     (f"Kârlı çeyrek ≥ %{KARAR_KARLI_CEYREK * 100:.0f}",
                      sd["karli_ceyrek"] >= math.ceil(KARAR_KARLI_CEYREK * sd["ceyrek_say"] - 1e-9),
                      f"{sd['karli_ceyrek']}/{sd['ceyrek_say']}")]
            gecti_d = all(k[1] for k in kural)
            satir += [f"  {'✅' if ok else '❌'} {ad}: {deger}" for ad, ok, deger in kural]
            satir.append("SONUÇ: " + ("✅ GEÇTİ" if gecti_d else "❌ KALDI"))
        if not onceki:
            kd.setdefault("modlar", {})[anahtar_k] = {"tarih": _ts_str(time.time()), "istek": d["istek"],
                                                      "gecti": gecti_d, "n": sd["n"]}
            _json_yaz(DOGRULAMA_KAYDI, kd)
    elif dogr_acik:
        satir += ["", "7) DOĞRULAMA – hiçbir varyant geçmediği için açılmadı."]

    satir += ["", "Notlar:",
              f"- Çıkışlar {strat.ZAMAN_DILIMI} mumlarla işletildi; aynı mumda stop ve TP varsa önce stop; "
              "mum stopun ötesinde açıldıysa açılıştan çıkış.",
              "- 'maliyetsiz' = komisyon, kayma ve funding düşülmeden önceki sonuç; GA'lar aylık blok bootstrap (%95)."]
    if "V2" in plan["varyantlar"]:
        satir.append("- V2'de dolum mumunda sadece stop kontrol edildi (TP'nin dolumdan önce mi sonra mı geldiği bilinmiyor).")
    satir += [
              "- Sahte kırılım: sinyalden sonraki 4 mumda kutu ortasının ötesine kapanış (işlemden bağımsız ölçülür).",
              "- t değeri: varyantın portföyde alınan her işlemi ile aynı sinyal mumundaki 200 RASTGELE sonucun ortalaması "
              "arasındaki farklardan (eşleştirilmiş); RASTGELE V0 girişiyle (sonraki mumun açılışı) hesaplanır.",
              "- NO TRADE 'aday': stratejinin giris() kuralları sıkışma ve deneme şartları geçici olarak kaldırılarak "
              "uygulandığında kırılım veren mum.",
              f"- Süre: {(time.time() - d['baslangic']) / 60:.0f} dk."]
    if d["hatalar"]:
        satir += ["", "Hatalar:"] + [f"- {h}" for h in d["hatalar"][:10]]
    metin = "\n".join(satir)

    ek = f"{d['istek']}_{_dosya_adi(strat.KOD)}"
    rapor_yol = os.path.join(CALISMA, f"bt_rapor_{ek}.txt")
    with open(rapor_yol, "w") as f:
        f.write(metin)
    csv_yol = os.path.join(CALISMA, f"bt_islemler_{ek}.csv")
    alinan_k = {var: {(x["sym"], x["i"]) for x in sonuc[var]["al"]} for var in plan["varyantlar"]}
    with open(csv_yol, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["varyant", "portfoyde_alindi", "sembol", "yon", "sinyal_zamani", "giris_zamani", "cikis_zamani",
                    "cikis", "giris_fiyati", "cikis_fiyati", "stop", "r", "r_maliyetsiz", "komisyon_r", "kayma_r",
                    "funding_r", "mfe_r", "mae_r", "sahte_kirilim", "btc_gunluk", "gunluk_atr_pct", "rastgele_ort_r"]
                   + BAGLAM)
        for var in plan["varyantlar"]:
            for x in sorted(adaylar[var], key=lambda y: y["giris_t"]):
                w.writerow([var, int((x["sym"], x["i"]) in alinan_k[var]), x["sym"],
                            "LONG" if x["side"] == "AL" else "SHORT", _ts_str(x["T"]), _ts_str(x["giris_t"]),
                            _ts_str(x["cikis_t"]), x["cikis"], x["giris_px"], x["cikis_px"], x["stop"], x["r"],
                            x["r_brut"], x["kom_r"], x["kayma_r"], x["fund_r"], x["mfe"], x["mae"], int(x["sahte"]),
                            x.get("btc_gunluk"), x.get("gunluk_atr_pct"),
                            None if (x["sym"], x["i"]) not in rast_ort else round(rast_ort[(x["sym"], x["i"])], 4)]
                           + [x.get(k) for k in BAGLAM])

    notify = _NOTIFY[0]
    _gonder(notify, metin)
    _belge_gonder(rapor_yol, f"Varyant testi raporu (istek {d['istek']}, {strat.KOD})")
    _belge_gonder(csv_yol, "İşlem listesi – V0/V1/V2, tüm sinyaller; portfoyde_alindi = portföy sınırlarıyla alınan")
    return True
