"""
Backtest: sinyal botunun kurallarını son 12 ayın Binance USDⓈ-M Futures geçmiş verisinde işletir.

BAŞLATMA (SSH gerekmez): repodaki ayarlar.env'de BACKTEST_ISTEGI değerini değiştir
  (ör. BACKTEST_ISTEGI=1; tekrar çalıştırmak için 2, 3 ...).
Canlı bot her dakika kendi işini bitirdikten sonra, bir sonraki dakikaya kadar kalan
birkaç saniyede backtest bir adım ilerler; canlı bot hiç beklemez. Bitince rapor ve
dosyalar (rapor .txt + işlemler .csv) Telegram'a gelir.

Birebir aynı olanlar:
  - Sinyal / trend / üst zaman dilimi / ATR stop: signal_bot.py'nin kendi fonksiyonları
    (compute_indicators, compute_signals, market_state, check_alert), 200 mumluk pencereyle.
  - Pozisyon büyüklüğü, kaldıraç sınırı, yarı kâr, SL'yi girişe çekme, süre sınırı,
    yapı (EMA26) çıkışı, pozisyon sınırları: binance_trader.py'nin ayarları ve sırası.
Varsayımlar:
  - Giriş: sinyal mumundan sonraki mumun açılışı. Çıkışlar 1 dakikalık mumlarla işletilir.
  - Aynı dakikada SL ve hedef (yarı kâr / TP) birlikteyse önce SL sayılır.
  - Botun dakikalık yönetim işleri (SL'yi girişe çekme, yapı ve süre çıkışı) bir sonraki
    dakikanın açılışında yapılır. Komisyon taker %0,05 (her dolumda). Funding ve kayma yok.

ayarlar.env:
  BACKTEST_ISTEGI   – değişince yeni backtest başlar
  DOGRULAMA_GOSTER  – false/tanımsız: son 4 ay hiç hesaplanmaz, sadece ayar dönemi; true: doğrulama da yapılır
  GIRIS_MODU        – signal_bot.py'deki giriş fikri; raporun başına yazılır
  GIRIS_MODU ya da DOGRULAMA_GOSTER değişince de yeni backtest başlar.

Çalışma klasörü: /opt/sinyalbot/bt_calisma (repo dışında; veri ~0,5 GB).
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

TEST_AY = 12                  # toplam test dönemi
AYAR_AY = 8                   # ilk 8 ay: ayar / karşılaştırma; kalan 4 ay: tek seferlik doğrulama
ISINMA_AY = 2                 # indikatörler için öncesinden fazladan indirilen ay
KOMISYON = 0.0005             # taker %0,05
MAX_DAKIKA = 30 * 1440        # bir işlem en fazla 30 gün izlenir (sonra "SON" ile kapanır)
UFUK_MUM = 16                 # çıkıştan bağımsız MFE/MAE: girişten sonraki 16 mum (işlemin zaman diliminde)
# Giriş anındaki bağlam bilgileri (işlem kayıtlarına ve CSV'ye yazılır)
BAGLAM = ["risk_atr", "stop_pct", "trend_guc", "ema26_uzaklik", "rsi", "atr_pct",
          "htf_trend", "htf_guc", "btc_trend", "btc_htf_trend"]

SURE_SONU = 48                # süreç başladıktan bu kadar saniye sonra backtest bu dakikalık işini bırakır
MIN_KALAN = 10                # yeni bir iş birimine başlamak için gereken en az süre (sn)
INDIRME_ESZAMANLI = 6
VERI_URL = "https://data.binance.vision/data/futures/um/monthly/klines"
ONEK = {"PEPE": "1000PEPE", "SHIB": "1000SHIB", "BONK": "1000BONK", "FLOKI": "1000FLOKI"}

# Önceden kabul edilen karar kuralları (doğrulama dönemi)
KARAR_ORT_R = 0.10
KARAR_PF = 1.2
KARAR_KARLI_AY = 0.60
KARAR_MAX_DD_USDT = rapor.STOP_DRAWDOWN_USDT     # kabul edilebilir en büyük düşüş (1250 USDT)

# Önceden belirlenmiş varyantlar (başka varyant eklenmez)
VARYANTLAR = [
    {"kod": "A", "ad": "Mevcut kurallar", "tp_r": sb.TP_R, "sure": True, "yapi": bt.STRUCTURE_EXIT},
    {"kod": "B", "ad": "Süre sınırı yok", "tp_r": sb.TP_R, "sure": False, "yapi": bt.STRUCTURE_EXIT},
    {"kod": "C", "ad": "TP 2R", "tp_r": 2.0, "sure": True, "yapi": bt.STRUCTURE_EXIT},
    {"kod": "D", "ad": "Yapı çıkışı yok", "tp_r": sb.TP_R, "sure": True, "yapi": False},
]
SECIM_KURALI = "ayar döneminde işlem başına ortalama R'si en yüksek varyant (eşitlikte A)"

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


def _ay_listesi(son, adet):
    return [f"{a[0]}-{a[1]:02d}" for a in (_ay_kaydir(son[0], son[1], -k) for k in range(adet - 1, -1, -1))]


def _ay_bas_ts(ay):
    y, m = map(int, ay.split("-"))
    return int(datetime.datetime(y, m, 1, tzinfo=datetime.timezone.utc).timestamp())


def _ay_son_ts(ay):
    y, m = map(int, ay.split("-"))
    y2, m2 = _ay_kaydir(y, m, 1)
    return int(datetime.datetime(y2, m2, 1, tzinfo=datetime.timezone.utc).timestamp())


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
# Ana giriş: signal_bot.main() her dakika sonunda çağırır
# =========================================================================
_NOTIFY = [print]          # rapor aşaması Telegram için bunu kullanır


def _giris_modu():
    """signal_bot.py'deki GIRIS_MODU (yoksa ayarlar.env'deki değer)."""
    m = getattr(sb, "GIRIS_MODU", None) or os.environ.get("GIRIS_MODU", "")
    m = str(m).strip().strip('"').strip("'").strip()
    return m or "tanımsız"


def _dogrulama_ayari():
    """DOGRULAMA_GOSTER=true ise son 4 ay hesaplanır ve gösterilir. Tanımsızsa KAPALI (yanlışlıkla bakılmasın)."""
    return sb._flag("DOGRULAMA_GOSTER")


def _dosya_adi(x):
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in x)


def adim(notify):
    _NOTIFY[0] = notify
    istek = os.environ.get(ISTEK_DEGISKENI, "").strip().strip('"').strip("'")
    if not istek or _kalan() < MIN_KALAN:
        return
    os.makedirs(CALISMA, exist_ok=True)
    d = _json_oku(DURUM, {}) or {}
    mod, dogr = _giris_modu(), _dogrulama_ayari()
    if (d.get("istek") != istek or d.get("giris_modu", mod) != mod
            or d.get("dogrulama", dogr) != dogr):
        d = _yeni_is(istek, notify)
        _json_yaz(DURUM, d)
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


def _yeni_is(istek, notify):
    simdi = datetime.datetime.now(datetime.timezone.utc)
    son = None
    for geri in (1, 2, 3):
        y, m = _ay_kaydir(simdi.year, simdi.month, -geri)
        if _var_mi(f"{VERI_URL}/BTCUSDT/1m/BTCUSDT-1m-{y}-{m:02d}.zip"):
            son = (y, m)
            break
    if not son:
        notify(f"❌ Backtest ({istek}) başlatılamadı: data.binance.vision'a ulaşılamadı. "
               f"Tekrar denemek için {ISTEK_DEGISKENI} değerini değiştir.")
        return {"istek": istek, "asama": "bitti"}
    test = _ay_listesi(son, TEST_AY)
    zfs = list(dict.fromkeys(["1m"] + sb.TIMEFRAMES + [sb.HIGHER_TF[t] for t in sb.TIMEFRAMES if t in sb.HIGHER_TF]))
    d = {
        "istek": istek, "asama": "indir", "sira": 0, "alt": 0, "baslangic": time.time(),
        "semboller": [[g, _bsym(g)] for g in sb.SYMBOLS], "zfs": zfs,
        "indir_aylar": _ay_listesi(son, TEST_AY + ISINMA_AY), "test_aylar": test,
        "ayar_aylar": test[:AYAR_AY], "dogrulama_aylar": test[AYAR_AY:],
        "hatalar": [], "deneme": {}, "eksik": {}, "atlanan_coin": [],
        "giris_modu": _giris_modu(), "dogrulama": _dogrulama_ayari(),
    }
    notify("\n".join([
        f"🧪 Backtest başladı (istek {istek}) – GIRIS_MODU: {d['giris_modu']}",
        f"Veri: Binance USDⓈ-M Futures, {len(d['semboller'])} coin, sinyal {'+'.join(sb.TIMEFRAMES)}, çıkışlar 1m",
        f"Ayar/karşılaştırma: {test[0]} – {test[AYAR_AY - 1]} ({AYAR_AY} ay)",
        (f"Doğrulama (tek sefer): {test[AYAR_AY]} – {test[-1]} ({TEST_AY - AYAR_AY} ay)" if d["dogrulama"] else
         f"Doğrulama: KAPALI (DOGRULAMA_GOSTER=false) – {test[AYAR_AY]} – {test[-1]} hiç hesaplanmayacak"),
        "Varyantlar: " + " · ".join(f"{v['kod']} {v['ad']}" for v in VARYANTLAR),
        f"Seçim: {SECIM_KURALI}",
        "Canlı bot her dakikadan sonra kalan sürede ilerler; birkaç saat sürebilir. Aşama bitince haber veririm.",
    ]))
    return d


def _birimler(d):
    ok = [s for s in d["semboller"] if s[1] not in d["atlanan_coin"]]
    if d["asama"] == "indir":
        return [(b, tf) for _, b in ok for tf in d["zfs"]]
    if d["asama"] == "sinyal":
        return [(b, tf) for _, b in ok for tf in sb.TIMEFRAMES]
    if d["asama"] == "simule":
        return [(b,) for _, b in ok]
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
        bitti = {"indir": _b_indir, "sinyal": _b_sinyal, "simule": _b_simule, "rapor": _b_rapor}[d["asama"]](d, b)
    except Exception as e:
        n = d["deneme"].get(anahtar, 0) + 1
        d["deneme"][anahtar] = n
        print(f"Backtest hata ({anahtar}, deneme {n}): {e}")
        if n >= 3:
            d["hatalar"].append(f"{anahtar}: {e}"[:300])
            if d["asama"] == "indir" and b[1] == "1m":
                d["atlanan_coin"].append(b[0])          # 1m verisi yoksa coin tamamen dışarıda kalır
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
    a = d["asama"]
    gecen = (time.time() - d["baslangic"]) / 60
    if a == "indir":
        coin = len(d["semboller"]) - len(d["atlanan_coin"])
        satir = [f"📥 Backtest: veri indirildi ({coin}/{len(d['semboller'])} coin, {gecen:.0f} dk)"]
        if d["eksik"]:
            satir.append("Eksik aylar: " + "; ".join(f"{k}: {','.join(v)}" for k, v in list(d["eksik"].items())[:8]))
        if d["atlanan_coin"]:
            satir.append("Dışarıda kalan coin: " + ", ".join(d["atlanan_coin"]))
        notify("\n".join(satir))
        d["asama"] = "sinyal"
    elif a == "sinyal":
        say, tfsay = 0, {}
        for _, b in d["semboller"]:
            for tf in sb.TIMEFRAMES:
                n = len((_json_oku(os.path.join(SINYAL, f"{b}_{tf}.json"), {}) or {}).get("sinyaller", []))
                say += n
                tfsay[tf] = tfsay.get(tf, 0) + n
        notify(f"📶 Backtest: sinyaller hesaplandı – {say} sinyal (" +
               " · ".join(f"{k} {v}" for k, v in tfsay.items()) + f"), {gecen:.0f} dk. Şimdi çıkışlar işletiliyor.")
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


def _veri_yolu(bsym, tf):
    return os.path.join(VERI, f"{bsym}_{tf}.npz")


def _b_indir(d, b):
    bsym, tf = b
    os.makedirs(VERI, exist_ok=True)
    yol = _veri_yolu(bsym, tf)
    anahtar = ",".join(d["indir_aylar"])
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
    with concurrent.futures.ThreadPoolExecutor(INDIRME_ESZAMANLI) as ex:
        sonuc = list(ex.map(lambda ay: _indir_ay(bsym, tf, ay), aylar))
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


def _indikatorlu(bsym, tf):
    v = _yukle(bsym, tf)
    df = pd.DataFrame({"open": v["o"], "high": v["h"], "low": v["l"], "close": v["c"]})
    sb.compute_indicators(df)                               # botun kendi indikatörleri
    kol = {k: pd.to_numeric(df[k]).to_numpy(float) for k in df.columns}
    kol["datetime"] = v["t"]
    return v["t"], df, kol


# =========================================================================
# 2) Sinyaller (botun check_alert'i, 200 mumluk pencere)
# =========================================================================
def _trend_okuyucu(bsym, tf):
    """T anında son KAPANMIŞ mumun trendi (botun market_state'i) ve gücü |EMA12-EMA26|/ATR."""
    if not tf:
        return lambda T: (None, None)
    try:
        t, _, kol = _indikatorlu(bsym, tf)
    except Exception:
        return lambda T: ("VERİ YOK", None)
    sec = SEC[tf]
    tam = _Pencere(kol, 0, len(t))
    onbellek = {}

    def oku(T):
        j_open = T // sec * sec - sec
        j = int(np.searchsorted(t, j_open))
        if j >= len(t) or t[j] != j_open or j < 199:
            return "VERİ YOK", None                        # filtre uygulanamıyorsa sinyal geçmez
        if j not in onbellek:
            a = kol["atr"][j]
            guc = float(abs(kol["ema_12"][j] - kol["ema_26"][j]) / a) if a > 0 else None
            onbellek[j] = (sb.market_state(tam, j)[0], guc)
        return onbellek[j]
    return oku


def _sayi(x, basamak=3):
    x = float(x)
    return round(x, basamak) if math.isfinite(x) else None


def _b_sinyal(d, b):
    bsym, tf = b
    os.makedirs(SINYAL, exist_ok=True)
    sec = SEC[tf]
    t, df, kol = _indikatorlu(bsym, tf)
    buy, sell = sb.compute_signals(df)
    n = len(t)

    htf = sb.HIGHER_TF.get(tf)
    htf_oku = _trend_okuyucu(bsym, htf)
    btc = _bsym("BTC_USDT")
    btc_oku = _trend_okuyucu(btc, tf)
    btc_htf_oku = _trend_okuyucu(btc, htf)

    bas = _ay_bas_ts(d["test_aylar"][0])
    son = _ay_son_ts(d["test_aylar"][-1])
    sinir = _ay_bas_ts(d["dogrulama_aylar"][0])
    if not d.get("dogrulama", True):
        son = sinir                                         # doğrulama dönemi hiç hesaplanmaz
    gecici = os.path.join(SINYAL, f"{bsym}_{tf}.yarim.json")
    kayit = (_json_oku(gecici, {}) or {}) if d["alt"] else {}
    liste = kayit.get("sinyaller", [])
    ham = kayit.get("ham", {})                              # sadece ayar dönemi: kaynak-yön → eleme nedeni → sayı

    def say(anahtar, neden):
        ham.setdefault(anahtar, {})
        ham[anahtar][neden] = ham[anahtar].get(neden, 0) + 1

    i0 = max(199, d["alt"])
    with contextlib.redirect_stdout(io.StringIO()):         # check_alert'in "stop çok uzak" yazıları
        for i in range(i0, n - 1):
            if (i - i0) % 400 == 0 and i > i0 and _kalan() < 3:
                d["alt"] = i
                _json_yaz(gecici, {"sinyaller": liste, "ham": ham})
                return False
            al, sat = buy[i], sell[i]
            if not al and not sat:
                continue
            T = int(t[i]) + sec                            # sinyal mumunun kapanışı = giriş anı
            if T < bas or T >= son or int(t[i + 1]) != T:
                continue
            egitim = T < sinir
            if al and sat:
                if egitim:
                    for k in al:
                        say(f"{k} AL", "çelişkili")
                    for k in sat:
                        say(f"{k} SAT", "çelişkili")
                continue
            side = "AL" if al else "SAT"
            a = i - 198                                     # canlıdaki gibi 200 mum: son kapanmış = n-2
            pencere = _Pencere(kol, a, i + 2)
            htf_t = htf_oku(T)[0] if htf else None
            alert = sb.check_alert(pencere, buy[a:i + 2], sell[a:i + 2], htf_t)
            if egitim:
                # Eleme nedeni: karar check_alert'in; neden, onun sırasıyla (trend → üst ZD → stop) belirlenir
                if alert:
                    neden = "GEÇTİ"
                else:
                    istenen = "YÜKSELİŞ" if side == "AL" else "DÜŞÜŞ"
                    trend = sb.market_state(pencere, 198)[0]
                    if trend == "YATAY":
                        neden = "trend yatay"
                    elif trend != istenen:
                        neden = "trend ters"
                    elif htf_t == "VERİ YOK":
                        neden = "üst ZD veri yok"
                    elif htf_t is not None and htf_t != istenen:
                        neden = "üst ZD yatay" if htf_t == "YATAY" else "üst ZD ters"
                    else:
                        neden = "stop >4 ATR"
                for k in (al or sat):
                    say(f"{k} {side}", neden)
            if alert:
                s = 1 if side == "AL" else -1
                c, atr_ = float(kol["close"][i]), float(kol["atr"][i])
                e12, e26, rsi_ = float(kol["ema_12"][i]), float(kol["ema_26"][i]), float(kol["rsi"][i])
                entry, stop = float(alert["entry"]), float(alert["stop"])
                liste.append({
                    "T": T, "tf": tf, "sym": bsym, "side": side, "entry": entry, "stop": stop,
                    "src": "+".join(alert["sources"]), "risk_atr": _sayi(alert["risk_atr"], 2),
                    "stop_pct": _sayi(abs(entry - stop) / entry * 100),
                    "trend_guc": _sayi(abs(e12 - e26) / atr_),
                    "ema26_uzaklik": _sayi(s * (c - e26) / atr_),      # + = trend yönünde EMA26'dan uzakta
                    "rsi": _sayi(rsi_, 1),
                    "atr_pct": _sayi(atr_ / c * 100),
                    "htf_trend": htf_t, "htf_guc": _sayi(htf_oku(T)[1]) if htf and htf_oku(T)[1] is not None else None,
                    "btc_trend": btc_oku(T)[0], "btc_htf_trend": btc_htf_oku(T)[0] if htf else None,
                })
    _json_yaz(os.path.join(SINYAL, f"{bsym}_{tf}.json"), {"sinyaller": liste, "ham": ham})
    if os.path.exists(gecici):
        os.remove(gecici)
    return True


# =========================================================================
# 3) Çıkışları 1 dakikalık mumlarla işletme
# =========================================================================
def _islem(sig, v, T1, O, H, L, C, yapi):
    """Tek sinyal, tek varyant. Portföy sınırları burada değil, _portfoy'de uygulanır."""
    s = 1 if sig["side"] == "AL" else -1
    T = sig["T"]
    tf = sig["tf"]
    sec = SEC[tf]
    k0 = int(np.searchsorted(T1, T))
    if k0 >= len(T1) or T1[k0] != T:
        return {"atla": "1m veri yok"}
    sl0 = sig["stop"]
    risk_sig = abs(sig["entry"] - sl0)
    tp = sig["entry"] + s * v["tp_r"] * risk_sig            # hedef sinyal fiyatından (check_alert gibi)
    fill = O[k0]
    if s * (fill - sl0) <= 0 or s * (tp - fill) <= 0:
        return {"atla": "fiyat SL/TP aralığının dışında"}

    # Büyüklük: open_trade ile aynı
    dist = abs(fill - sl0)
    hedef_zarar = bt.MARGIN_USDT * bt.LOSS_PCT_OF_MARGIN
    notional = hedef_zarar / (dist / fill)
    lev = max(1, math.ceil(notional / bt.MARGIN_USDT))
    if lev > bt.MAX_LEVERAGE:
        lev = bt.MAX_LEVERAGE
        notional = bt.MARGIN_USDT * lev
    qty = notional / fill
    risk_usdt = qty * dist
    half = qty * bt.PARTIAL_SIZE
    mid = fill + (tp - fill) * bt.PARTIAL_AT

    pnl = 0.0
    ucret = fill * qty * KOMISYON
    kalan = qty
    sl = sl0
    kismi = False            # yarı kâr emri borsada doldu
    be_bekliyor = False      # bot bir sonraki dakikada SL'yi girişe çekecek
    be_tamam = False         # botun be_done'ı
    mum_sayisi = bt.MAX_HOLD_CANDLES.get(tf, bt.DEFAULT_HOLD_CANDLES)
    mfe = 0.0
    son_k = min(len(T1) - 1, k0 + MAX_DAKIKA)
    k = k0
    cikis = None
    cikis_t = None

    def kapat(px, ne):
        nonlocal pnl, ucret, kalan
        pnl += s * (px - fill) * ne
        ucret += px * ne * KOMISYON
        kalan -= ne

    while True:
        tk = int(T1[k])
        # --- Botun dakikalık yönetimi (manage_positions sırası), dakikanın açılışında
        if k > k0:
            p = O[k]
            if be_bekliyor:
                be_bekliyor = False
                be_tamam = True
                if s * (p - fill) <= 0:                     # SL girişe konamaz → kalan piyasadan kapanır
                    kapat(p, kalan)
                    cikis, cikis_t = "GİRİŞ", tk + 20
                    break
                sl = fill
            else:
                if v["yapi"] and not be_tamam and tk % sec == 0 and tk > T:
                    y = yapi.get(tk)                        # bu dakikada kapanan işlem-zaman dilimi mumu
                    if y and ((s == 1 and y[0] < y[1]) or (s == -1 and y[0] > y[1])):
                        kapat(p, kalan)
                        cikis, cikis_t = "YAPI", tk + 20
                        break
                if v["sure"] and mum_sayisi and not (be_tamam and not bt.AFTER_PARTIAL_MULT):
                    sinir = mum_sayisi * sec * (bt.AFTER_PARTIAL_MULT if be_tamam else 1)
                    if tk - T >= sinir:
                        kapat(p, kalan)
                        cikis, cikis_t = "SÜRE", tk + 20
                        break
        # --- Borsadaki emirler (SL önce)
        op, hi, lo = O[k], H[k], L[k]
        if (s == 1 and lo <= sl) or (s == -1 and hi >= sl):
            kapat(op if s * (op - sl) <= 0 else sl, kalan)
            cikis = "GİRİŞ" if be_tamam and sl == fill else "SL"
            cikis_t = tk + 30
            break
        fav = (hi - fill) if s == 1 else (fill - lo)
        if fav / dist > mfe:
            mfe = fav / dist
        if not kismi and ((s == 1 and hi >= mid) or (s == -1 and lo <= mid)):
            kapat(op if s * (op - mid) >= 0 else mid, half)
            kismi = True
            be_bekliyor = True
        if (s == 1 and hi >= tp) or (s == -1 and lo <= tp):
            kapat(op if s * (op - tp) >= 0 else tp, kalan)
            cikis, cikis_t = "TP", tk + 30
            break
        if k >= son_k:
            kapat(C[k], kalan)
            cikis, cikis_t = "SON", tk + 60
            break
        k += 1

    net = pnl - ucret
    return {"sym": sig["sym"], "tf": tf, "side": sig["side"], "T": T, "cikis_t": cikis_t, "cikis": cikis,
            "kismi": kismi, "net": round(net, 2), "risk": round(risk_usdt, 2), "r": round(net / risk_usdt, 4),
            "brut": round(pnl, 2), "kom": round(ucret, 2), "r_brut": round(pnl / risk_usdt, 4),
            "kom_r": round(ucret / risk_usdt, 4),
            "mfe": round(mfe, 3), "lev": lev, "dk": (cikis_t - T) // 60, "src": sig["src"]}


def _sabit_ufuk(sig, T1, On, Hn, Ln):
    """
    Çıkış kuralından bağımsız: girişten sonraki UFUK_MUM mum (işlemin zaman diliminde) boyunca
    fiyatın lehimize (MFE) ve aleyhimize (MAE) en fazla ne kadar gittiği, R cinsinden
    (1R = giriş fiyatı ile SL arası). ilk1r: +1R'ye mi -1R'ye (SL) mi önce değdi; aynı dakikada ise zarar.
    """
    k0 = int(np.searchsorted(T1, sig["T"]))
    if k0 >= len(T1) or T1[k0] != sig["T"]:
        return {}
    fill = float(On[k0])
    dist = abs(fill - sig["stop"])
    if dist <= 0:
        return {}
    k1 = k0 + UFUK_MUM * SEC[sig["tf"]] // 60
    h, l = Hn[k0:k1], Ln[k0:k1]
    if sig["side"] == "AL":
        lehte, aleyhte = (h - fill) / dist, (fill - l) / dist
    else:
        lehte, aleyhte = (fill - l) / dist, (h - fill) / dist
    kar = np.flatnonzero(lehte >= 1)
    zarar = np.flatnonzero(aleyhte >= 1)
    if not len(kar) and not len(zarar):
        ilk = "yok"
    elif not len(zarar) or (len(kar) and kar[0] < zarar[0]):
        ilk = "kâr"
    else:
        ilk = "zarar"
    return {"mfe16": round(float(lehte.max()), 3), "mae16": round(float(aleyhte.max()), 3), "ilk1r": ilk}


def _b_simule(d, b):
    bsym = b[0]
    os.makedirs(SONUC, exist_ok=True)
    sinyaller = []
    for tf in sb.TIMEFRAMES:
        sinyaller += (_json_oku(os.path.join(SINYAL, f"{bsym}_{tf}.json"), {}) or {}).get("sinyaller", [])
    hedef = os.path.join(SONUC, f"{bsym}.json")
    gecici = hedef + ".yarim"
    sonuc = (_json_oku(gecici, []) or []) if d["alt"] else []
    if sinyaller:
        m = _yukle(bsym, "1m")
        T1 = m["t"]
        O, H, L, C = m["o"].tolist(), m["h"].tolist(), m["l"].tolist(), m["c"].tolist()
        On, Hn, Ln = m["o"], m["h"], m["l"]
        yapi = {}
        for tf in {s_["tf"] for s_ in sinyaller}:
            v = _yukle(bsym, tf)
            kap = v["c"].tolist()
            e26 = sb.ema(kap, 26)                           # compute_indicators'taki ema_26 ile aynı
            yapi[tf] = {int(t_) + SEC[tf]: (c_, e_) for t_, c_, e_ in zip(v["t"].tolist(), kap, e26)}
        for j in range(d["alt"], len(sinyaller)):
            if j > d["alt"] and j % 10 == 0 and _kalan() < 3:
                d["alt"] = j
                _json_yaz(gecici, sonuc)
                return False
            sig = sinyaller[j]
            ek = {k: sig.get(k) for k in BAGLAM}
            ek.update(_sabit_ufuk(sig, T1, On, Hn, Ln))
            for v in VARYANTLAR:
                r = _islem(sig, v, T1, O, H, L, C, yapi[sig["tf"]])
                r["var"] = v["kod"]
                if "atla" in r:
                    r.update({"sym": bsym, "tf": sig["tf"], "T": sig["T"], "side": sig["side"], "src": sig["src"]})
                r.update(ek)
                sonuc.append(r)
    _json_yaz(hedef, sonuc)
    if os.path.exists(gecici):
        os.remove(gecici)
    return True


# =========================================================================
# 4) Portföy sınırları, istatistik ve rapor
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
    return s


def _ozet(ad, s):
    if not s["n"]:
        return f"{ad}: işlem yok"
    return (f"{ad}: {s['n']} işlem | {_r(s['ort_r'])}/işlem (%95 GA {_r(s['ga'][0])} … {_r(s['ga'][1])}) | "
            f"PF {_f2(s['pf'])} | kazanma {_pct(s['kazanma'])} | net {_usd(s['net'])} USDT | "
            f"en büyük düşüş {_usd(-s['dd'])} USDT ({_f2(s['dd_r'])}R) | kârlı ay {s['karli_ay']}/{s['ay_say']}"
            f"\n   komisyonsuz: {_r(s['r_brut'])}/işlem | PF {_f2(s['pf_brut'])} | kazanma {_pct(s['kazanma_brut'])}"
            f" · komisyon payı {_r(-s['kom_r'])}/işlem (toplam {_usd(-s['kom_usdt'])} USDT)")


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


def _dokum(tr, s):
    satir = [_kisa(tf, [x for x in tr if x["tf"] == tf]) for tf in sb.TIMEFRAMES]
    satir += [_kisa("LONG", [x for x in tr if x["side"] == "AL"]), _kisa("SHORT", [x for x in tr if x["side"] == "SAT"])]
    for tf in sb.TIMEFRAMES:
        for yon, ad in (("AL", "LONG"), ("SAT", "SHORT")):
            satir.append(_kisa(f"{tf} {ad}", [x for x in tr if x["tf"] == tf and x["side"] == yon]))
    if s["n"]:
        satir.append("  Çıkışlar: " + " · ".join(f"{k} {v}" for k, v in sorted(s["cikis"].items(), key=lambda kv: -kv[1]))
                     + f" | yarı kâra ulaşma {_pct(s['kismi'])} | ort. süre {_f2(s['ort_dk'] / 60)} saat"
                     + f" | en uzun kayıp serisi {s['seri']}")
        satir.append("  Aylık:")
        for a, n_, net_, rr in s["aylar"]:
            satir.append(f"   {a}: {n_} işlem | net {_usd(net_)} USDT | {_r(rr)}/işlem" + (" ✅" if net_ > 0 else ""))
    return satir


def _mfe(tr, tp_r):
    if not tr:
        return ["  işlem yok"]
    m = np.array([x["mfe"] for x in tr])
    kovalar = [(0, 0.25), (0.25, 0.5), (0.5, 0.75), (0.75, 1.0), (1.0, 1.5), (1.5, 2.0), (2.0, 3.0), (3.0, float("inf"))]
    satir = ["  Dağılım (kapanmadan önce ulaşılan en yüksek kâr, R):"]
    for a, b in kovalar:
        oran = float(((m >= a) & (m < b)).mean())
        etiket = f"{_f2(a)}–{_f2(b)}R" if math.isfinite(b) else f"≥{_f2(a)}R"
        satir.append(f"   {etiket}: {_pct(oran)} {'█' * int(round(oran * 40))}")
    esikler = sorted({0.5, 0.75, 1.0, 1.5, 2.0, round(tp_r * bt.PARTIAL_AT, 4)})   # yarı kâr seviyesi de dahil
    satir.append("  Ulaşma: " + " · ".join(f"≥{_f2(e)}R {_pct(float((m >= e).mean()))}" for e in esikler))
    sl = np.array([x["mfe"] for x in tr if x["cikis"] == "SL"])
    if len(sl):
        satir.append(f"  SL ile kapananlar ({len(sl)}): önce ≥0,5R'ye ulaşan {_pct(float((sl >= 0.5).mean()))}, "
                     f"medyan MFE {_f2(float(np.median(sl)))}R")
    for c in ("SÜRE", "YAPI"):
        z = np.array([x["mfe"] for x in tr if x["cikis"] == c])
        if len(z):
            satir.append(f"  {c} ile kapananlar ({len(z)}): medyan MFE {_f2(float(np.median(z)))}R, "
                         f"≥1R'ye ulaşan {_pct(float((z >= 1).mean()))}")
    return satir


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


# ---- Giriş analizi (sadece ayar dönemi, sınırsız: her sinyal bağımsız işlem) ----
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


def _giris_analizi(d, hepsi, sinir, port):
    ayar_bag = {v["kod"]: [x for x in hepsi if x["var"] == v["kod"] and "atla" not in x and x["T"] < sinir]
                for v in VARYANTLAR}
    a_tr = ayar_bag["A"]
    satir = ["", "6) GİRİŞ ANALİZİ – sadece ayar dönemi, pozisyon sınırı YOK (her sinyal bağımsız işlem)",
             "   (doğrulama dönemine bilerek bakılmıyor; girişler burada sorgulanıp değiştirilecek)"]

    # 6a: sınırsız sonuçlar
    satir.append("6a) Varyantlar, tüm sinyaller bağımsız:")
    for v in VARYANTLAR:
        satir.append(_ozet(f"{v['kod']} {v['ad']}", _ist(ayar_bag[v["kod"]], d["ayar_aylar"])))
    alinan_a = {(x["sym"], x["tf"], x["T"]) for x in port["A"][0]}
    ali = [x for x in a_tr if (x["sym"], x["tf"], x["T"]) in alinan_a]
    atl = [x for x in a_tr if (x["sym"], x["tf"], x["T"]) not in alinan_a]
    satir.append("  A – portföyde alınanlar ile sınır yüzünden atlananlar:")
    satir.append(_kisa("alınan", ali))
    satir.append(_kisa("atlanan", atl))
    if not a_tr:
        return satir

    # 6b: çıkıştan bağımsız MFE / MAE
    u = [x for x in a_tr if x.get("ilk1r")]
    satir += ["", f"6b) Çıkıştan bağımsız: girişten sonraki {UFUK_MUM} mum (işlemin zaman diliminde), 1R = giriş–SL mesafesi"]
    if u:
        kar = [x for x in u if x["ilk1r"] == "kâr"]
        zar = [x for x in u if x["ilk1r"] == "zarar"]
        satir.append(f"  Önce +1R'ye değen {_pct(len(kar) / len(u))} · önce SL'ye (−1R) değen {_pct(len(zar) / len(u))} · "
                     f"ikisine de değmeyen {_pct(1 - (len(kar) + len(zar)) / len(u))}")
        satir += _dagilim(f"MFE{UFUK_MUM} (en yüksek lehte hareket)", [x["mfe16"] for x in u])
        satir += _dagilim(f"MAE{UFUK_MUM} (en derin ters hareket)", [x["mae16"] for x in u])
        if kar:
            yuz = np.percentile([x["mae16"] for x in kar], [50, 75, 90])
            satir.append(f"  Önce +1R'ye değenlerin ters hareketi (MAE): %50'si ≤{_f2(yuz[0])}R, %75'i ≤{_f2(yuz[1])}R, "
                         f"%90'ı ≤{_f2(yuz[2])}R")
        if zar:
            sonra = sum(x["mfe16"] >= 1 for x in zar) / len(zar)
            satir.append(f"  Önce SL'ye değenlerin {_pct(sonra)}'i aynı {UFUK_MUM} mumda sonra +1R'ye de gitti (stop dar mı?)")
        for tf in sb.TIMEFRAMES:
            for yon, ad in (("AL", "LONG"), ("SAT", "SHORT")):
                g = [x for x in u if x["tf"] == tf and x["side"] == yon]
                if g:
                    satir.append(f"   {tf} {ad}: {_grup(g)}")

    # 6c: giriş anındaki bağlam
    satir += ["", "6c) Giriş anındaki bağlama göre (A kurallarıyla R; çeyreklik dilimler)",
              "   sütunlar: sinyal | R/işlem (komisyonsuz) | 16 mumda önce +1R / önce −1R | MFE16 · MAE16 medyan"]
    satir += _sayisal_tablo("Stop mesafesi (ATR)", a_tr, "risk_atr")
    satir += _sayisal_tablo("Stop mesafesi (%)", a_tr, "stop_pct")
    satir += _sayisal_tablo("Trend gücü |EMA12−EMA26|/ATR", a_tr, "trend_guc")
    satir += _sayisal_tablo("EMA26'ya uzaklık (ATR, + = trend yönünde)", a_tr, "ema26_uzaklik")
    satir += _sayisal_tablo("RSI", a_tr, "rsi")
    satir += _sayisal_tablo("Volatilite (ATR %)", a_tr, "atr_pct")
    satir += _sayisal_tablo("Üst ZD trend gücü |EMA12−EMA26|/ATR", a_tr, "htf_guc")
    satir += _kategori_tablo("BTC trendi (aynı zaman dilimi)", a_tr, lambda x: _iliski(x, "btc_trend"))
    satir += _kategori_tablo("BTC trendi (üst zaman dilimi)", a_tr, lambda x: _iliski(x, "btc_htf_trend"))
    satir += _kategori_tablo("Sinyal kaynağı", a_tr, lambda x: f"{x['src']} {'LONG' if x['side'] == 'AL' else 'SHORT'}")
    satir += _kategori_tablo("Saat (TR)", a_tr, lambda x: f"{(int(_ts_str(x['T'])[11:13]) // 4) * 4:02d}–"
                                                         f"{(int(_ts_str(x['T'])[11:13]) // 4) * 4 + 4:02d}")

    # 6d: ham sinyaller ve eleme nedenleri
    satir += ["", "6d) Ham sinyaller ve filtreler (bir mumda birden çok kaynak varsa her birine sayılır)"]
    nedenler = ["çelişkili", "trend yatay", "trend ters", "üst ZD yatay", "üst ZD ters", "üst ZD veri yok",
                "stop >4 ATR", "GEÇTİ"]
    for tf in sb.TIMEFRAMES:
        top = {}
        for _, bs in d["semboller"]:
            ham = (_json_oku(os.path.join(SINYAL, f"{bs}_{tf}.json"), {}) or {}).get("ham", {})
            for k, nd in ham.items():
                for n_, c in nd.items():
                    top.setdefault(k, {})
                    top[k][n_] = top[k].get(n_, 0) + c
        satir.append(f"  {tf}:")
        for k in sorted(top, key=lambda k: (k.split()[0], k.split()[1])):
            toplam = sum(top[k].values())
            parca = " · ".join(f"{n_} {top[k][n_]} ({_pct(top[k][n_] / toplam)})" for n_ in nedenler if top[k].get(n_))
            satir.append(f"   {k}: {toplam} ham → {parca}")
    return satir


def _b_rapor(d, b):
    hepsi = []
    for _, bsym in d["semboller"]:
        hepsi += _json_oku(os.path.join(SONUC, f"{bsym}.json"), []) or []
    sym_sira = {bs: i for i, (_, bs) in enumerate(d["semboller"])}
    tf_sira = {tf: i for i, tf in enumerate(sb.TIMEFRAMES)}
    sinir = _ay_bas_ts(d["dogrulama_aylar"][0])
    dogr_acik = d.get("dogrulama", True)
    mod = d.get("giris_modu") or "tanımsız"
    if not dogr_acik:
        hepsi = [x for x in hepsi if x["T"] < sinir]       # güvenlik: doğrulama dönemi hiçbir yere girmez
    sinyal_say = len({(x["sym"], x["tf"], x["T"]) for x in hepsi})
    atla_say = {}
    for x in hepsi:
        if "atla" in x and x["var"] == "A":
            atla_say[x["atla"]] = atla_say.get(x["atla"], 0) + 1

    ayar, port = {}, {}
    for v in VARYANTLAR:
        alinan, atl = _portfoy([x for x in hepsi if x["var"] == v["kod"] and "atla" not in x], sym_sira, tf_sira)
        port[v["kod"]] = (alinan, atl)
        ayar[v["kod"]] = _ist([x for x in alinan if x["T"] < sinir], d["ayar_aylar"])

    adaylar = [v for v in VARYANTLAR if ayar[v["kod"]]["n"]]
    secilen = max(adaylar, key=lambda v: (round(ayar[v["kod"]]["ort_r"], 6), v["kod"] == "A")) if adaylar else VARYANTLAR[0]
    sk = secilen["kod"]
    alinan_s = port[sk][0]
    ayar_tr = [x for x in alinan_s if x["T"] < sinir]
    dogr_tr = [x for x in alinan_s if x["T"] >= sinir]
    dg = _ist(dogr_tr, d["dogrulama_aylar"]) if dogr_acik else {"n": 0}

    # Doğrulama "bir kez" (giriş moduna göre): ilk bakış kaydedilir, sonrakilerde uyarı verilir
    kayit = _json_oku(DOGRULAMA_KAYDI, {}) or {}
    modlar = dict(kayit.get("modlar", {}))
    if kayit.get("ilk") and "eski strateji (GIRIS_MODU öncesi)" not in modlar:
        modlar["eski strateji (GIRIS_MODU öncesi)"] = kayit["ilk"]
    onceki = modlar.get(mod)
    baska = [f"{k} ({v.get('tarih', '?')})" for k, v in modlar.items() if k != mod]

    satir = [
        f"🧪 BACKTEST SONUCU (istek {d['istek']}) – {_ts_str(time.time())}",
        f"🔧 GIRIS_MODU: {mod} | Doğrulama: {'AÇIK' if dogr_acik else 'KAPALI (sadece ayar dönemi)'}",
        f"Veri: Binance USDⓈ-M Futures | {len(d['semboller']) - len(d['atlanan_coin'])} coin | "
        f"sinyal {'+'.join(sb.TIMEFRAMES)} (üst: {', '.join(sb.HIGHER_TF[t] for t in sb.TIMEFRAMES)}) | çıkış 1m",
        f"Ayar dönemi: {d['ayar_aylar'][0]} – {d['ayar_aylar'][-1]} | Doğrulama: "
        + (f"{d['dogrulama_aylar'][0]} – {d['dogrulama_aylar'][-1]}" if dogr_acik else "hesaplanmadı"),
        f"Toplam sinyal: {sinyal_say}" + (" | işleme dönmeyen: " + " · ".join(f"{k} {v}" for k, v in atla_say.items())
                                          if atla_say else ""),
        f"Pozisyon: {bt.MARGIN_USDT:.0f} USDT teminat, SL'de ≈{bt.MARGIN_USDT * bt.LOSS_PCT_OF_MARGIN:.0f} USDT (1R), "
        f"en fazla {bt.MAX_OPEN_POSITIONS} pozisyon / aynı yönde {bt.MAX_SAME_DIRECTION} | komisyon %{KOMISYON * 100:g}".replace("0.05", "0,05"),
        "",
        "1) AYAR DÖNEMİ – varyant karşılaştırması (portföy sınırları dahil)",
    ]
    for v in VARYANTLAR:
        atl = port[v["kod"]][1]
        satir.append(_ozet(f"{v['kod']} {v['ad']}", ayar[v["kod"]]))
        satir.append("   sınır nedeniyle atlanan: " + " · ".join(f"{k} {n}" for k, n in atl.items()))
    satir += [f"➡️ Seçilen: {sk} {secilen['ad']} ({SECIM_KURALI})", ""]
    gecti = False
    if not dogr_acik:
        satir += ["2) DOĞRULAMA – KAPALI (DOGRULAMA_GOSTER=false): son 4 ay hesaplanmadı, karar kuralları uygulanmadı.",
                  "   Giriş fikirlerini ilk 8 aya göre karşılaştır; seçilen tek fikir için en sonda bir kez aç."]
    else:
        satir.append(f"2) DOĞRULAMA – GIRIS_MODU {mod}, varyant {sk}, tek sefer")
        if onceki:
            satir.append(f"⚠️ Bu giriş modu için doğrulama ilk kez {onceki.get('tarih')} tarihinde (istek {onceki.get('istek')}, "
                         f"varyant {onceki.get('varyant')}) görüldü. Karar o ilk sonuca göre verilir; bu tekrar bakıştır.")
        if baska:
            satir.append("⚠️ Doğrulama dönemi daha önce başka giriş modlarıyla da görüldü: " + ", ".join(baska)
                         + ". Her ek bakış bu dönemin 'görülmemiş veri' niteliğini azaltır.")
        satir.append(_ozet(f"{sk} {secilen['ad']}", dg))

        kural = []
        if dg["n"]:
            kural = [
                ("Ortalama ≥ +0,10R", dg["ort_r"] >= KARAR_ORT_R, _r(dg["ort_r"])),
                ("%95 GA alt sınırı > 0", dg["ga"][0] > 0, _r(dg["ga"][0])),
                (f"Kâr faktörü ≥ {_f2(KARAR_PF)}", dg["pf"] >= KARAR_PF, _f2(dg["pf"])),
                (f"Kârlı ay ≥ %{KARAR_KARLI_AY * 100:.0f}",
                 dg["karli_ay"] >= math.ceil(KARAR_KARLI_AY * dg["ay_say"] - 1e-9), f"{dg['karli_ay']}/{dg['ay_say']}"),
                (f"En büyük düşüş ≤ {KARAR_MAX_DD_USDT:,.0f} USDT".replace(",", "."), dg["dd"] <= KARAR_MAX_DD_USDT,
                 _usd(-dg["dd"])),
            ]
        gecti = bool(kural) and all(k[1] for k in kural)
        satir.append("Karar kuralları:")
        satir += [f"  {'✅' if ok else '❌'} {ad}: {deger}" for ad, ok, deger in kural] or ["  doğrulamada işlem yok"]
        satir.append("SONUÇ: " + ("✅ GEÇTİ – canlı demo sadece doğrulama için sürdürülür (eşikler aşağıda)." if gecti else
                                 "❌ KALDI – anlaştığımız gibi çıkışlarla oynamayı bırakıp girişleri sorguluyoruz."))

    satir += ["", f"3) DÖKÜM – {sk}, ayar dönemi"] + _dokum(ayar_tr, ayar[sk])
    if dogr_acik:
        satir += ["", f"   {sk}, doğrulama dönemi"] + _dokum(dogr_tr, dg)
    satir += ["", f"4) MFE – {sk}, ayar dönemi"] + _mfe(ayar_tr, secilen["tp_r"])
    if dogr_acik:
        satir += [f"   {sk}, doğrulama dönemi"] + _mfe(dogr_tr, secilen["tp_r"])
    if sk != "A":
        a_ayar = [x for x in port["A"][0] if x["T"] < sinir]
        satir += ["   A (mevcut kurallar), ayar dönemi"] + _mfe(a_ayar, VARYANTLAR[0]["tp_r"])

    mc_aylar = d["test_aylar"] if dogr_acik else d["ayar_aylar"]
    ay_basina = max(1, round(len(alinan_s) / max(1, len(mc_aylar))))
    mc = _monte_carlo(alinan_s, [50, 100, ay_basina])
    satir += ["", f"5) CANLI İZLEME EŞİKLERİ – {sk}, {len(mc_aylar)} ayın tüm işlemlerinden Monte Carlo (2000 karıştırma)"]
    for u, dd95, seri95 in mc:
        etiket = f"{u} işlem" + (" (≈1 ay)" if u == ay_basina else "")
        satir.append(f"  {etiket}: %95 düşüş {_usd(-dd95)} USDT ({_f2(dd95 / (bt.MARGIN_USDT * bt.LOSS_PCT_OF_MARGIN))}R)"
                     f" | %95 kayıp serisi {seri95}")
    satir.append("  Canlıda düşüş, aynı işlem sayısındaki %95 düşüşü aşarsa: dur ve birlikte bakalım.")

    satir += _giris_analizi(d, hepsi, sinir, port)

    satir += ["", "Notlar:",
              "- Canlı sinyaller Gate.io spot verisinden geliyor; backtest Binance Futures verisini kullanıyor (fiyatlar çok yakın ama aynı değil).",
              "- Kayma, funding, kaldıraç kademesi (leverage bracket) ve bakiye yetersizliği hesaba katılmadı.",
              "- 'Komisyonsuz' değerler aynı işlemlerin komisyon düşülmeden önceki sonucudur (giriş+çıkış taker %0,05).",
              "- İndikatörler tüm geçmişten hesaplandı; 200 mumluk canlı pencereyle fark ihmal edilebilir düzeyde (EMA/RSI başlangıç etkisi).",
              f"- Ayrıntılı işlem listesi CSV dosyasında. Süre: {(time.time() - d['baslangic']) / 60:.0f} dk."]
    if d["hatalar"]:
        satir += ["", "Hatalar:"] + [f"- {h}" for h in d["hatalar"][:10]]
    if d["eksik"]:
        satir += ["Eksik veri ayları: " + "; ".join(f"{k}: {','.join(v)}" for k, v in list(d["eksik"].items())[:10])]
    metin = "\n".join(satir)

    if dogr_acik and not onceki:
        kayit.setdefault("modlar", {})[mod] = {"tarih": _ts_str(time.time()), "istek": d["istek"], "varyant": sk,
                                               "gecti": gecti, "ort_r": dg.get("ort_r"), "n": dg["n"]}
        _json_yaz(DOGRULAMA_KAYDI, kayit)

    ek = f"{d['istek']}_{_dosya_adi(mod)}"
    rapor_yol = os.path.join(CALISMA, f"bt_rapor_{ek}.txt")
    with open(rapor_yol, "w") as f:
        f.write(metin)
    csv_yol = os.path.join(CALISMA, f"bt_islemler_{ek}.csv")
    with open(csv_yol, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["varyant", "donem", "sembol", "tf", "yon", "giris_zamani", "cikis_zamani", "cikis", "yari_kar",
                    "net_usdt", "brut_usdt", "komisyon_usdt", "risk_usdt", "r", "r_komisyonsuz", "mfe_r",
                    "kaldirac", "sure_dk", "sinyal"] + BAGLAM + ["mfe16_r", "mae16_r", "ilk_1r"])
        for v in VARYANTLAR:
            for x in sorted(port[v["kod"]][0], key=lambda x: x["T"]):
                dogr = x["T"] >= sinir
                if dogr and v["kod"] != sk:
                    continue                                   # seçilmeyen varyantların doğrulaması hiç açılmaz
                w.writerow([v["kod"], "dogrulama" if dogr else "ayar", x["sym"], x["tf"],
                            "LONG" if x["side"] == "AL" else "SHORT", _ts_str(x["T"]), _ts_str(x["cikis_t"]),
                            x["cikis"], int(x["kismi"]), x["net"], x["brut"], x["kom"], x["risk"], x["r"], x["r_brut"],
                            x["mfe"], x["lev"], x["dk"], x["src"]] + [x.get(k) for k in BAGLAM]
                           + [x.get("mfe16"), x.get("mae16"), x.get("ilk1r")])

    # Ayar dönemindeki TÜM sinyaller (pozisyon sınırı olmadan), her varyantın bağımsız sonucuyla
    sinyal_yol = os.path.join(CALISMA, f"bt_sinyaller_ayar_{ek}.csv")
    alinmis = {v["kod"]: {(x["sym"], x["tf"], x["T"]) for x in port[v["kod"]][0]} for v in VARYANTLAR}
    satirlar = {}
    for x in hepsi:
        if x["T"] >= sinir:
            continue
        anahtar = (x["sym"], x["tf"], x["T"])
        satirlar.setdefault(anahtar, {})[x["var"]] = x
    with open(sinyal_yol, "w", newline="") as f:
        w = csv.writer(f)
        bas = ["sembol", "tf", "yon", "giris_zamani", "sinyal"] + BAGLAM + ["mfe16_r", "mae16_r", "ilk_1r"]
        for v in VARYANTLAR:
            k = v["kod"]
            bas += [f"{k}_r", f"{k}_r_komisyonsuz", f"{k}_cikis", f"{k}_portfoyde_alindi"]
        w.writerow(bas)
        for anahtar in sorted(satirlar, key=lambda a: (a[2], a[1], a[0])):
            g = satirlar[anahtar]
            x = g.get("A") or next(iter(g.values()))
            satir_ = [x["sym"], x["tf"], "LONG" if x.get("side") == "AL" else "SHORT", _ts_str(x["T"]), x.get("src")]
            satir_ += [x.get(k) for k in BAGLAM] + [x.get("mfe16"), x.get("mae16"), x.get("ilk1r")]
            for v in VARYANTLAR:
                y = g.get(v["kod"], {})
                if "atla" in y or not y:
                    satir_ += ["", "", y.get("atla", ""), 0]
                else:
                    satir_ += [y["r"], y["r_brut"], y["cikis"], int(anahtar in alinmis[v["kod"]])]
            w.writerow(satir_)

    notify = _NOTIFY[0]
    _gonder(notify, metin)
    _belge_gonder(rapor_yol, f"Backtest raporu (istek {d['istek']}, GIRIS_MODU {mod})")
    _belge_gonder(csv_yol, "Portföyde alınan işlemler" + (" (ayar: tüm varyantlar, doğrulama: sadece seçilen)"
                                                         if dogr_acik else " (sadece ayar dönemi)"))
    _belge_gonder(sinyal_yol, "Ayar dönemindeki tüm sinyaller, pozisyon sınırı olmadan (bağlam + MFE/MAE + her varyantın sonucu)")
    return True
