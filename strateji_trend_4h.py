"""
STRATEJİ: Trend Kırılım 4h  (kod: trend_4h)

Bir strateji = bir dosya. Bot, ayarlar.env'deki STRATEJILER listesinde adı geçen
strateji_<kod>.py dosyalarını çalıştırır. Yeni strateji eklemek için bu dosyayı kopyala,
adını strateji_<yeni_kod>.py yap ve aşağıdaki bölümleri değiştir.

Kurallar (backtest: istek 7, geliştirme dönemi 378 işlem, +0,14R/işlem funding dahil):
  Giriş : 4s mum, önceki 20 mumun en yükseğinin üstünde İLK kez kapanır (SHORT: en düşüğünün altında)
          ve günlük trend (signal_bot.market_state) aynı yöndedir.
  Stop  : giriş ∓ 2,5 ATR (borsada STOP_MARKET).
  Çıkış : 4s mum önceki 10 mumun en düşüğünün altında kapanınca (SHORT: en yükseğinin üstünde).
          TP, yarı kâr ve süre sınırı yok.

Erişim kuralı (backtest uyumu): giris() ve iz_cikis() içinde sadece len(df), df["sütun"][i]
ve df["sütun"][a:b].min()/.max() kullanılır. hazirla() gerçek DataFrame alır.
"""
import math

# ---------------------------------------------------------------------------
# Kimlik ve tanıtım (Telegram'daki strateji kartı bunlardan oluşur)
# ---------------------------------------------------------------------------
KOD = "trend_4h"
AD = "Trend Kırılım 4h"
YON = "ikisi"                 # "long" | "short" | "ikisi"
ZAMAN_DILIMI = "4h"
UST_ZAMAN_DILIMI = "1d"       # trend filtresi için; gerekmiyorsa None
COINLER = None                # None = ayarlar.env'deki SYMBOLS listesi; ya da ["SOL_USDT", "SUI_USDT", ...]
ACIKLAMA = ("4 saatlik grafikte trend takibi yapıyor; fiyat son 20 mumun kanalını kırıp kapandığında, "
            "günlük trend de aynı yöndeyse pozisyon açıyor. Kâr hedefi koymuyor: trend sürdükçe pozisyonu "
            "taşıyor, fiyat son 10 mumun kanalını ters yönde kırınca çıkıyor.")

# ---------------------------------------------------------------------------
# Çıkış ayarları (binance_trader ve backtest bunları okur)
# ---------------------------------------------------------------------------
CIKIS = {
    "stop_atr": 2.5,          # stop = giriş ∓ 2,5 × ATR(14)
    "tp_r": None,             # TP = giriş ± tp_r × risk; None = TP yok
    "yari_kar": None,         # giriş→TP yolunun bu oranında pozisyonun yarısı kapanır, SL girişe çekilir; None = yok
    "sure_mum": None,         # yarı kâra bu kadar mumda ulaşamazsa kapat (yarı kârdan sonra ×2); None = yok
    "iz": True,               # True = her mum kapanışında iz_cikis() kontrol edilir
}

# Durma uyarıları (rapor.py): (en fazla işlem, düşüş sınırı USDT, art arda kayıp sınırı).
# Backtest işlemlerinden Monte Carlo %95, 1R = 100 USDT.
DURMA = [(30, 1240, 12), (60, 1790, 14), (None, 2250, 15)]

KANAL_GIRIS = 20
KANAL_CIKIS = 10


# ---------------------------------------------------------------------------
# Kurallar
# ---------------------------------------------------------------------------
def _kanal(values, n, fn):
    """i. eleman: önceki n mumun (mevcut mum HARİÇ) max/min'i; ilk n mumda NaN."""
    return [fn(values[i - n:i]) if i >= n else float("nan") for i in range(len(values))]


def hazirla(df):
    """Stratejiye özel sütunlar (ortak indikatörler signal_bot.compute_indicators'ta)."""
    highs, lows = df["high"].tolist(), df["low"].tolist()
    df["kanal_ust"] = _kanal(highs, KANAL_GIRIS, max)
    df["kanal_alt"] = _kanal(lows, KANAL_GIRIS, min)
    df["cikis_ust"] = _kanal(highs, KANAL_CIKIS, max)
    df["cikis_alt"] = _kanal(lows, KANAL_CIKIS, min)


def _ok(*xs):
    return all(x is not None and not math.isnan(x) for x in xs)


def giris(df, i, ust_trend):
    """i. (kapanmış) mumda giriş var mı? Dönüş: ("AL" | "SAT", ["kaynak"]) ya da None."""
    if i < 1:
        return None
    c, c0 = float(df["close"][i]), float(df["close"][i - 1])
    hi, hi0 = float(df["kanal_ust"][i]), float(df["kanal_ust"][i - 1])
    lo, lo0 = float(df["kanal_alt"][i]), float(df["kanal_alt"][i - 1])
    if not _ok(hi, hi0, lo, lo0):
        return None
    if c > hi and c0 <= hi0 and ust_trend == "YÜKSELİŞ":
        return "AL", ["KANAL20"]
    if c < lo and c0 >= lo0 and ust_trend == "DÜŞÜŞ":
        return "SAT", ["KANAL20"]
    return None


def iz_cikis(df, i, side):
    """i. (kapanmış) mumda çıkış koşulu oluştu mu? Dönüş: açıklama metni ya da None."""
    c = float(df["close"][i])
    if side == "AL":
        lvl = float(df["cikis_alt"][i])
        if _ok(lvl) and c < lvl:
            return f"mum önceki {KANAL_CIKIS} mumun dibinin altında kapandı ({c:g} < {lvl:g})"
    else:
        lvl = float(df["cikis_ust"][i])
        if _ok(lvl) and c > lvl:
            return f"mum önceki {KANAL_CIKIS} mumun tepesinin üstünde kapandı ({c:g} > {lvl:g})"
    return None


def cikis_metni(side):
    """Açılış mesajında çıkış kuralının kısa anlatımı."""
    yer = "dibinin altında" if side == "AL" else "tepesinin üstünde"
    return f"{ZAMAN_DILIMI} mum önceki {KANAL_CIKIS} mumun {yer} kapanınca"
