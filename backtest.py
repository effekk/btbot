"""
Backtest: sinyal botunun bir stratejisini (strateji_<kod>.py) geçmiş veride işletir.

BAŞLATMA (SSH gerekmez) – ayarlar.env:
  BACKTEST_ISTEGI   – değişince yeni backtest başlar (1, 2, 3 ...)
  BT_STRATEJI       – test edilecek strateji kodu (boşsa STRATEJILER'in ilki)
  DOGRULAMA_GOSTER  – false/tanımsız: doğrulama dönemi hiç hesaplanmaz; true: sadece geliştirme döneminde
                      karar kurallarını geçen strateji için hesaplanır (bir kez; dogrulama_kaydi.json)
  BT_STRATEJI ya da DOGRULAMA_GOSTER değişince de yeni backtest başlar.
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
  - Maliyet: komisyon taker %0,05 (her dolumda), kayma her piyasa emrinde (BTC/ETH %0,02, diğer %0,05),
    funding kaba tahmin 8 saatte %0,01 (ayrı gösterilir; karar kuralları komisyon + kayma dahil sonuca uygulanır).
  - Portföy: binance_trader'daki sınırlar (açık pozisyon, aynı yön, aynı coin).
  - Geliştirme dönemi sabit (GELISTIRME); doğrulama dönemi ondan sonraki aylar. Geliştirme işlemi doğrulama
    başlarken açıksa o mumun açılışından kapatılır (DÖNEM), böylece doğrulama açık da kapalı da olsa sonuç aynı.
  - Karar kuralları geliştirme döneminde uygulanır (ortalama R, aylık blok bootstrap GA, PF, kârlı çeyrek,
    RASTGELE'den fark). RASTGELE: aynı sinyal mumlarında, aynı stop mesafesi ve çıkış kurallarıyla, yön yazı-tura.

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

GELISTIRME = ("2021-01", "2026-06")   # sabit geliştirme dönemi; sonrası doğrulama
ISINMA_AY = 8                 # indikatörler için geliştirme başından önce indirilen ay (1d 200 mum ≈ 7 ay)
KOMISYON = 0.0005             # taker %0,05
KAYMA = {"BTCUSDT": 0.0002, "ETHUSDT": 0.0002}   # piyasa emri başına; listede olmayan coinler KAYMA_DIGER
KAYMA_DIGER = 0.0005
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

# Önceden kabul edilen karar kuralları (maliyet dahil): önce geliştirme döneminde, geçerse doğrulamada bir kez
KARAR_ORT_R = 0.10
KARAR_PF = 1.2
KARAR_KARLI_CEYREK = 0.60
KARAR_T_RASTGELE = 2.0        # RASTGELE'den fark (eşleştirilmiş) t değeri
BOOT = 20000                  # aylık blok bootstrap tekrar sayısı
RASTGELE_TEKRAR = 200
RASTGELE_TOHUM = 20261101

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
    kod, dogr = _strateji_kodu(), _dogrulama_ayari()
    if d.get("istek") != istek or d.get("strateji", kod) != kod or d.get("dogrulama", dogr) != dogr:
        try:
            d = _yeni_is(istek, kod, dogr, notify)
        except Exception as e:
            notify(f"❌ Backtest ({istek}) başlatılamadı: {e}")
            d = {"istek": istek, "strateji": kod, "dogrulama": dogr, "asama": "bitti"}
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


def _yeni_is(istek, kod, dogr, notify):
    strat = sb.load_strategy(kod)              # dosya yoksa / alan eksikse burada hata verir
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
        "istek": istek, "strateji": kod, "dogrulama": dogr, "asama": "indir",
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
        (f"Doğrulama: {dog[0]} – {dog[-1]} ({len(dog)} ay), sadece karar kuralları geçilirse, bir kez" if dogr and dog else
         "Doğrulama: KAPALI (DOGRULAMA_GOSTER=false) – hiç hesaplanmayacak" if dog else "Doğrulama: veri yok"),
        "Canlı bot her dakikadan sonra kalan sürede ilerler. Aşama bitince haber veririm.",
    ]))
    return d


def _birimler(d):
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
        islev = {"indir": _b_indir, "sinyal": _b_sinyal, "simule": _b_simule, "rapor": _b_rapor}
        bitti = islev[d["asama"]](d, b)
    except Exception as e:
        n = d["deneme"].get(anahtar, 0) + 1
        d["deneme"][anahtar] = n
        print(f"Backtest hata ({anahtar}, deneme {n}): {e}")
        if n >= 3:
            d["hatalar"].append(f"{anahtar}: {e}"[:300])
            if d["asama"] == "indir" and b[1] == d["zfs"][0]:
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
def _islem(sig, strat, v, kol, sinir):
    """
    Tek işlem, stratejinin zaman dilimindeki mumlarla. Sonuçlar R cinsinden (1R = giriş ile stop arası);
    USDT karşılığı için Binance büyüklük kuralı (binance_trader) kullanılır. Maliyet: komisyon + kayma
    (her dolum piyasa emri sayılır) + ayrıca gösterilen funding tahmini.
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
    payi = bt.PARTIAL_SIZE

    parcalar = []                                   # (oran, fiyat, zaman)
    kalan = 1.0
    yarim = be_bekliyor = be = False
    mfe = mae = 0.0
    k = k0
    while True:
        tk = int(t[k])
        if kesim is not None and tk >= kesim:
            parcalar.append((kalan, float(O[k]), tk + 10))
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
    # open_trade ile aynı büyüklük (kaldıraç sınırı) – R'yi değiştirmez, USDT karşılığını belirler
    notional = bt.MARGIN_USDT * bt.LOSS_PCT_OF_MARGIN / (dist / fill)
    lev = max(1, math.ceil(notional / bt.MARGIN_USDT))
    if lev > bt.MAX_LEVERAGE:
        lev = bt.MAX_LEVERAGE
        notional = bt.MARGIN_USDT * lev
    risk = notional / fill * dist
    kom_r = sum(o * (fill + px) for o, px, _ in parcalar) * KOMISYON / dist
    kayma_r = sum(o * (fill + px) for o, px, _ in parcalar) * KAYMA.get(sig["sym"], KAYMA_DIGER) / dist
    fund_r = sum(o * (zt - T) / 3600 for o, _, zt in parcalar) / 8 * FUNDING_8S * fill / dist
    r = r_brut - kom_r - kayma_r
    sonuc.update({"net": round(r * risk, 2), "brut": round(r_brut * risk, 2), "kom": round(kom_r * risk, 2),
                  "kayma": round(kayma_r * risk, 2), "fund": round(fund_r * risk, 2), "risk": round(risk, 2),
                  "r": round(r, 4), "r_brut": round(r_brut, 4), "kom_r": round(kom_r, 4), "kayma_r": round(kayma_r, 4),
                  "r_fund": round(r - fund_r, 4), "lev": lev})
    return sonuc


def _ters(sig, v):
    """RASTGELE için aynı sinyalin ters yönlüsü: girişe göre aynı stop ve hedef mesafesi, ters taraf."""
    k0 = int(sig["i"]) + 1
    if k0 >= len(v["t"]):
        return None
    fill = float(v["o"][k0])
    y = dict(sig, side="SAT" if sig["side"] == "AL" else "AL", stop=2 * fill - float(sig["stop"]))
    if sig.get("target") is not None:
        y["target"] = 2 * fill - float(sig["target"])
    return y


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
                r = _islem(sig, strat, v, kol, sinir)
                if "atla" in r:
                    r.update({"sym": bsym, "tf": sig["tf"], "T": sig["T"], "side": sig["side"], "src": sig["src"]})
                else:
                    ters = _ters(sig, v)
                    rt = _islem(ters, strat, v, kol, sinir) if ters else {"atla": "veri yok"}
                    if "atla" not in rt:
                        # Yazı-tura: her sinyal mumunun kendi sabit tohumu → aynı mumun yönleri her testte aynı
                        sec_ = np.random.default_rng([RASTGELE_TOHUM, sum(map(ord, bsym)), int(sig["i"])]) \
                            .integers(0, 2, RASTGELE_TEKRAR)
                        r["rastgele_ort"] = round(float(np.where(sec_ == 1, r["r"], rt["r"]).mean()), 4)
                        r["ters_r"] = rt["r"]
                        r["rastgele_sec"] = sec_.tolist()
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


def _blok_ga(tr, aylar, anahtar="r", seed=5):
    """Aylık blok bootstrap: ayları (içindeki tüm işlemlerle) yerine koyarak çek; işlem başına ortalama R'nin %95 aralığı.
    İşlemler aynı aylarda kümelendiği (coinler birlikte hareket ettiği) için tek tek bootstrap'tan daha dürüst."""
    if not tr or not aylar:
        return None
    idx = {a: i for i, a in enumerate(aylar)}
    top = np.zeros(len(aylar))
    say = np.zeros(len(aylar))
    for x in tr:
        i = idx.get(_ts_ay(x["T"]))
        if i is not None:
            top[i] += x[anahtar]
            say[i] += 1
    rng = np.random.default_rng(seed)
    secim = rng.integers(0, len(aylar), (BOOT, len(aylar)))
    t_, s_ = top[secim].sum(1), say[secim].sum(1)
    ort = t_[s_ > 0] / s_[s_ > 0]
    return float(np.percentile(ort, 2.5)), float(np.percentile(ort, 97.5))


def _t_rastgele(tr):
    """İşlemler ile aynı sinyal mumlarındaki RASTGELE ortalaması arasındaki farkın (eşleştirilmiş) t değeri."""
    f = np.array([x["r"] - x["rastgele_ort"] for x in tr if x.get("rastgele_ort") is not None])
    if len(f) < 3 or f.std(ddof=1) <= 0:
        return None, len(f), None
    return float(f.mean() / (f.std(ddof=1) / math.sqrt(len(f)))), len(f), float(f.mean())


def _rastgele_dagilim(tr):
    """Portföyde alınan işlemlerin sinyal mumlarında RASTGELE tekrarların ortalama R dağılımı."""
    g = [x for x in tr if x.get("rastgele_sec")]
    if not g:
        return None
    m = np.array([np.where(np.array(x["rastgele_sec"]) == 1, x["r"], x["ters_r"]) for x in g]).mean(0)
    return float(m.mean()), float(np.percentile(m, 2.5)), float(np.percentile(m, 97.5))


def _kurallar(s, tr):
    """Önceden kayıtlı karar kuralları (maliyet dahil). Dönüş: [(ad, geçti_mi, değer metni)]."""
    if not s["n"]:
        return []
    tval, tn, tfark = _t_rastgele(tr)
    return [
        ("Ortalama ≥ +0,10R", s["ort_r"] >= KARAR_ORT_R, _r(s["ort_r"])),
        ("Aylık blok bootstrap %95 GA alt sınırı > 0", s["ga"] is not None and s["ga"][0] > 0,
         _r(s["ga"][0]) if s["ga"] else "–"),
        (f"Kâr faktörü ≥ {_f2(KARAR_PF)}", s["pf"] >= KARAR_PF, _f2(s["pf"])),
        (f"Kârlı çeyrek ≥ %{KARAR_KARLI_CEYREK * 100:.0f}",
         s["karli_ceyrek"] >= math.ceil(KARAR_KARLI_CEYREK * s["ceyrek_say"] - 1e-9),
         f"{s['karli_ceyrek']}/{s['ceyrek_say']}"),
        (f"RASTGELE'den fark t ≥ {_f2(KARAR_T_RASTGELE)}", tval is not None and tval >= KARAR_T_RASTGELE,
         f"t = {_f2(tval)} (fark {_r(tfark)}, {tn} işlem)"),
    ]


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
    s["ga"] = _blok_ga(tr, aylar)
    s["ga_brut"] = _blok_ga(tr, aylar, "r_brut")
    kayip = net[net <= 0].sum()
    s["pf"] = float(net[net > 0].sum() / -kayip) if kayip < 0 else float("inf")
    s["kazanma"] = float((net > 0).mean())
    s["net"] = float(net.sum())
    brut = np.array([x["brut"] for x in tr])
    s["r_brut"] = float(np.mean([x["r_brut"] for x in tr]))
    s["kom_r"] = float(np.mean([x["kom_r"] for x in tr]))
    s["kayma_r"] = float(np.mean([x["kayma_r"] for x in tr]))
    s["kom_usdt"] = float(np.sum([x["kom"] for x in tr]))
    s["kayma_usdt"] = float(np.sum([x["kayma"] for x in tr]))
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
    # Funding tahmini (ayrı gösterilir)
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
    return (f"  {ad}: {len(tr)} işlem | {_r(r.mean())}/işlem (maliyetsiz {_r(np.mean([x['r_brut'] for x in tr]))}) | "
            f"PF {_f2(pf)} (maliyetsiz {_f2(pfb)}) | kazanma {_pct((net > 0).mean())} | net {_usd(net.sum())}")


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
    """Bir sinyal grubunun özeti: R (maliyetli / maliyetsiz) + çıkıştan bağımsız ölçüler."""
    r = np.mean([x["r"] for x in g])
    rb = np.mean([x["r_brut"] for x in g])
    u = [x for x in g if x.get("ilk1r")]
    kar = sum(x["ilk1r"] == "kâr" for x in u) / len(u) if u else None
    zar = sum(x["ilk1r"] == "zarar" for x in u) / len(u) if u else None
    mfe = np.median([x["mfe16"] for x in u]) if u else None
    mae = np.median([x["mae16"] for x in u]) if u else None
    return (f"{len(g)} | {_r(r)} (maliyetsiz {_r(rb)}) | önce +1R {_pct(kar)} / önce −1R {_pct(zar)} | "
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
    ga = f"{_r(s['ga'][0])} … {_r(s['ga'][1])}" if s.get("ga") else "–"
    gab = f"{_r(s['ga_brut'][0])} … {_r(s['ga_brut'][1])}" if s.get("ga_brut") else "–"
    satir = [
        f"{ad}: {s['n']} işlem | {_r(s['ort_r'])}/işlem (aylık blok %95 GA {ga}) | "
        f"PF {_f2(s['pf'])} | kazanma {_pct(s['kazanma'])} | net {_usd(s['net'])} USDT | "
        f"en büyük düşüş {_usd(-s['dd'])} USDT ({_f2(s['dd_r'])}R) | "
        + (f"kârlı çeyrek {s['karli_ceyrek']}/{s['ceyrek_say']}" if ceyrek else f"kârlı ay {s['karli_ay']}/{s['ay_say']}"),
        f"   maliyetsiz: {_r(s['r_brut'])}/işlem (GA {gab}) | PF {_f2(s['pf_brut'])} · maliyet/işlem: komisyon "
        f"{_r(-s['kom_r'])} · kayma {_r(-s['kayma_r'])} (toplam {_usd(-s['kom_usdt'] - s['kayma_usdt'])} USDT)",
    ]
    if "r_fund" in s:
        satir.append(f"   maliyet + funding tahmini (8 saatte %{FUNDING_8S * 100:g}): {_r(s['r_fund'])}/işlem | "
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
    except Exception:
        return None


# =========================================================================
# 5) Rapor
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
        f"en fazla {bt.MAX_OPEN_POSITIONS} pozisyon / aynı yönde {bt.MAX_SAME_DIRECTION}",
        f"Maliyet: komisyon %{KOMISYON * 100:g} (her dolumda), kayma BTC/ETH %{KAYMA['BTCUSDT'] * 100:g} · diğer "
        f"%{KAYMA_DIGER * 100:g} (her piyasa emrinde), funding 8 saatte %{FUNDING_8S * 100:g} (ayrı)".replace(".", ","),
    ]
    if atla_say:
        satir.append("İşleme dönmeyen sinyal: " + " · ".join(f"{k} {n_}" for k, n_ in atla_say.items()))

    kural_g = _kurallar(gel, gel_tr)
    gel_gecti = bool(kural_g) and all(k[1] for k in kural_g)
    satir += ["", "1) GELİŞTİRME DÖNEMİ – karar kuralları (maliyet dahil, portföy sınırlarıyla)"]
    satir += [f"  {'✅' if ok else '❌'} {ad}: {deger}" for ad, ok, deger in kural_g] or ["  işlem yok"]
    satir.append("SONUÇ: " + ("✅ GEÇTİ – doğrulama bir kez açılabilir (DOGRULAMA_GOSTER=true)" if gel_gecti else
                              "❌ KALDI – plan gereği sonuca göre parametre, saat ya da coin filtresi önerilmez"))
    satir += [""] + _t_ozet(kod, gel)
    satir.append("   sınır nedeniyle atlanan: " + " · ".join(f"{k} {n_}" for k, n_ in atl.items()))
    rd = _rastgele_dagilim(gel_tr)
    if rd:
        satir.append(f"   RASTGELE ({RASTGELE_TEKRAR} tekrar, aynı sinyal mumları, yön yazı-tura): ortalama {_r(rd[0])}/işlem "
                     f"(tekrarların %2,5–97,5'i {_r(rd[1])} … {_r(rd[2])})")

    gecti = False
    satir.append("")
    if not dogr_acik:
        satir += ["2) DOĞRULAMA – KAPALI (DOGRULAMA_GOSTER=false): hesaplanmadı."]
    elif not gel_gecti:
        satir += ["2) DOĞRULAMA – açılmadı: geliştirme döneminde karar kuralları geçilmedi."]
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
                ("Aylık blok %95 GA alt sınırı > 0", dg["ga"] is not None and dg["ga"][0] > 0,
                 _r(dg["ga"][0]) if dg["ga"] else "–"),
                (f"Kâr faktörü ≥ {_f2(KARAR_PF)}", dg["pf"] >= KARAR_PF, _f2(dg["pf"])),
                (f"Kârlı çeyrek ≥ %{KARAR_KARLI_CEYREK * 100:.0f}",
                 dg["karli_ceyrek"] >= math.ceil(KARAR_KARLI_CEYREK * dg["ceyrek_say"] - 1e-9),
                 f"{dg['karli_ceyrek']}/{dg['ceyrek_say']}"),
                (f"Düşüş ≤ geliştirme döneminin {dg['n']} işlemlik %95 düşüşü"
                 + (f" ({_usd(-dd_sinir)} USDT)" if dd_sinir is not None else ""),
                 dd_sinir is not None and dg["dd"] <= dd_sinir, _usd(-dg["dd"])),
            ]
        gecti = bool(kural) and all(k[1] for k in kural)
        satir.append("Karar kuralları (maliyet dahil):")
        satir += [f"  {'✅' if ok else '❌'} {ad}: {deger}" for ad, ok, deger in kural] or ["  doğrulamada işlem yok"]
        if dg["n"] and "r_fund" in dg:
            satir.append(f"  bilgi: funding tahmini dahil ortalama {_r(dg['r_fund'])}, PF {_f2(dg['pf_fund'])}")
        satir.append("SONUÇ: " + ("✅ GEÇTİ" if gecti else "❌ KALDI"))

    satir += ["", "3) DÖKÜM – geliştirme dönemi"] + _dokum(gel_tr, gel)
    if dogr_acik and gel_gecti:
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
        satir.append(f"  Bağlama göre – sadece bilgi, filtre yapılmaz (sütunlar: sinyal | R (maliyetsiz) | {UFUK_MUM} mumda "
                     "önce +1R / −1R | MFE16 · MAE16 medyan)")
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

    mc_aylar = gel_aylar + (dog_aylar if dogr_acik and gel_gecti else [])
    mc_tr = gel_tr + (dog_tr if dogr_acik and gel_gecti else [])
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
              "- Funding gerçek oranlarla değil, kaba tahminle (her iki yön için maliyet) hesaplandı; karar kuralları komisyon + kayma dahil sonuca uygulanır.",
              "- Çıkışlar stratejinin zaman dilimindeki mumlarla işletildi (1 dakikalık veri kullanılmadı); aynı mumda stop önce.",
              "- Stop mumun ötesinde açılırsa (boşluk) açılış fiyatından çıkılır; kaldıraç kademesi yok.",
              "- GA: aylık blok bootstrap (%95). RASTGELE t değeri: her işlem ile aynı sinyal mumundaki RASTGELE ortalaması "
              "arasındaki farklardan (eşleştirilmiş).",
              "- Canlı sinyaller Gate.io spot verisinden; backtest Binance Futures verisinden.",
              f"- Süre: {(time.time() - d['baslangic']) / 60:.0f} dk."]
    if d["hatalar"]:
        satir += ["", "Hatalar:"] + [f"- {h}" for h in d["hatalar"][:10]]
    if d["eksik"]:
        satir += ["Eksik veri ayları: " + "; ".join(f"{k}: {','.join(v)}" for k, v in list(d["eksik"].items())[:10])]
    metin = "\n".join(satir)

    if dogr_acik and gel_gecti and not onceki:
        kayit.setdefault("modlar", {})[kod] = {"tarih": _ts_str(time.time()), "istek": d["istek"], "varyant": kod,
                                               "gecti": gecti, "ort_r": dg.get("ort_r"), "n": dg["n"]}
        _json_yaz(DOGRULAMA_KAYDI, kayit)

    ek = f"{d['istek']}_{_dosya_adi(kod)}"
    rapor_yol = os.path.join(CALISMA, f"bt_rapor_{ek}.txt")
    with open(rapor_yol, "w") as f:
        f.write(metin)
    sutun = ["strateji", "donem", "portfoyde_alindi", "sembol", "tf", "yon", "giris_zamani", "cikis_zamani", "cikis",
             "yari_kar", "giris_fiyati", "cikis_fiyati", "stop", "net_usdt", "brut_usdt", "komisyon_usdt", "kayma_usdt",
             "funding_tahmini_usdt", "risk_usdt", "r", "r_maliyetsiz", "r_funding_dahil", "rastgele_ort_r", "mfe_r",
             "mae_r", "kaldirac", "sure_saat", "sinyal"] + BAGLAM + ["mfe16_r", "mae16_r", "ilk_1r"]
    csv_yol = os.path.join(CALISMA, f"bt_islemler_{ek}.csv")
    with open(csv_yol, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(sutun)
        for x in sorted(tr_hepsi, key=lambda y: y["T"]):
            w.writerow([kod, "dogrulama" if x["T"] >= sinir else "gelistirme", int((x["sym"], x["T"]) in al_k),
                        x["sym"], x["tf"], "LONG" if x["side"] == "AL" else "SHORT", _ts_str(x["T"]),
                        _ts_str(x["cikis_t"]), x["cikis"], int(x["kismi"]), x["giris_px"], x["cikis_px"], x["stop"],
                        x["net"], x["brut"], x["kom"], x["kayma"], x["fund"], x["risk"], x["r"], x["r_brut"],
                        x["r_fund"], x.get("rastgele_ort"), x["mfe"], x["mae"], x["lev"], round(x["dk"] / 60, 1), x["src"]]
                       + [x.get(k) for k in BAGLAM] + [x.get("mfe16"), x.get("mae16"), x.get("ilk1r")])

    notify = _NOTIFY[0]
    _gonder(notify, metin)
    _belge_gonder(rapor_yol, f"Backtest raporu (istek {d['istek']}, {kod})")
    _belge_gonder(csv_yol, "Tüm sinyaller (sınırsız) – portfoyde_alindi sütunu portföy sınırlarıyla alınanları gösterir")
    return True
