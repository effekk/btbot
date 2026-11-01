"""
STRATEJİ: Sıkışma Kırılımı 1h  (kod: sikisma_1h)  — HENÜZ BACKTEST AŞAMASINDA, canlıda kullanma.

Fikir: fiyat 16 saat dar bir kutuda sıkışır; oynaklık genişlerken kutunun dışında güçlü bir mum kapanırsa
o yönde işlem açılır. Kurallar araştırma dokümanında önceden kayıtlıdır; testten önce değiştirilmez.

  Kutu     : sinyal mumundan ÖNCEKİ 16 mum. KH = en yüksek, KL = en düşük, W = KH − KL.
  Sıkışma  : W, önceki 200 mumdaki 16'lık kutu genişliklerinin en dar %20'si içinde.
  Kırılım  : LONG  close ≥ KH + 0,1W ve (close − low)/(high − low) ≥ 0,6
             SHORT close ≤ KL − 0,1W ve (high − close)/(high − low) ≥ 0,6
             ortak: |close − open| ≥ 0,5 × mum boyu, 1,0 × ATR ≤ mum boyu ≤ 3,0 × ATR (ATR: önceki mum)
  Tek deneme: son 16 mumda sıkışma varken kutu dışında kapanış olduysa sinyal yok.
  V1       : ek olarak 4h trendi (signal_bot.market_state) aynı yönde.
  Stop     : LONG KL − 0,1W, SHORT KH + 0,1W; risk %1,2–%6,0 dışındaysa sinyal yok.
  Çıkış    : 3R hedef ya da 16 mum (16 saat) süre sınırı. Başa baş / iz süren / yarı kâr yok.

15m sürümü (istek 9) kaldı: yön bilgisi yoktu, maliyet ≈ 0,13R/işlem. 1h sürümü aynı kuralların
daha geniş stoplu hali; risk alt sınırı maliyet ≤ ≈ 0,15R olacak şekilde %1,2. Bunun dışında kural aynı.

Erişim kuralı (backtest uyumu): giris() ve stop() içinde sadece df["sütun"][i] ve
df["sütun"][a:b].min()/.max() kullanılır. hazirla() gerçek DataFrame alır.
"""
import math

KOD = "sikisma_1h"
AD = "Sıkışma Kırılımı 1h"
YON = "ikisi"
ZAMAN_DILIMI = "1h"
UST_ZAMAN_DILIMI = "4h"
COINLER = ["BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "BNB_USDT",
           "DOGE_USDT", "ADA_USDT", "AVAX_USDT", "LINK_USDT", "SUI_USDT"]
ACIKLAMA = ("1 saatlik grafikte iki yönde çalışıyor; fiyat 16 saat dar bir kutuda sıkıştıktan sonra "
            "oynaklık genişlerken kutunun dışında güçlü bir mum kapanınca pozisyon açıyor. Fitille kırılımlara "
            "ve aynı sıkışmadan ikinci denemeye girmiyor; 3R hedef ya da 16 saat sonra çıkıyor.")

CIKIS = {
    "stop_atr": None,         # stop, aşağıdaki stop() fonksiyonundan gelir (kutunun karşı ucu)
    "tp_r": 3.0,
    "yari_kar": None,
    "sure_mum": 16,
    "iz": False,
}
DURMA = [(None, 2250, 15)]    # backtest sonrası Monte Carlo ile güncellenecek

VARYANT = "V0"                # "V0" | "V1" — backtest her biri için ayrı çalıştırır

KUTU = 16                     # kutu uzunluğu (mum)
PENCERE = 200                 # sıkışma karşılaştırma penceresi (mum)
DAR_YUZDE = 0.20              # en dar %20
KIRILIM_PAYI = 0.1            # × W
GOVDE_ORAN = 0.5
KAPANIS_YERI = 0.6
ATR_MIN, ATR_MAX = 1.0, 3.0   # mum boyu / ATR
STOP_PAYI = 0.1               # × W
RISK_MIN, RISK_MAX = 0.012, 0.06


def hazirla(df):
    """Kutu, sıkışma ve deneme sütunları. Sadece geçmişe bakar (shift(1): sinyal mumu kutuya girmez)."""
    df["kh"] = df["high"].shift(1).rolling(KUTU).max()
    df["kl"] = df["low"].shift(1).rolling(KUTU).min()
    df["w"] = df["kh"] - df["kl"]
    esik = df["w"].shift(1).rolling(PENCERE).quantile(DAR_YUZDE)
    df["sikisma"] = (df["w"] <= esik).astype(float)
    df["atr_once"] = df["atr"].shift(1)
    disari = (df["close"] > df["kh"]) | (df["close"] < df["kl"])
    df["deneme"] = (disari & (df["sikisma"] == 1)).astype(float)


def _ok(*xs):
    return all(x is not None and not math.isnan(x) for x in xs)


def giris(df, i, ust_trend):
    """i. (kapanmış) mumda giriş var mı? Dönüş: ("AL" | "SAT", ["SIKISMA"]) ya da None (NO TRADE)."""
    if i < KUTU or float(df["sikisma"][i]) != 1:
        return None
    if float(df["deneme"][i - KUTU:i].max()) == 1:          # bu kutudan deneme yapılmış
        return None
    kh, kl, w, a = float(df["kh"][i]), float(df["kl"][i]), float(df["w"][i]), float(df["atr_once"][i])
    o, h, l, c = float(df["open"][i]), float(df["high"][i]), float(df["low"][i]), float(df["close"][i])
    if not _ok(kh, kl, w, a) or w <= 0 or a <= 0:
        return None
    boy = h - l
    if boy <= 0 or boy < ATR_MIN * a or boy > ATR_MAX * a or abs(c - o) < GOVDE_ORAN * boy:
        return None
    if c >= kh + KIRILIM_PAYI * w and (c - l) / boy >= KAPANIS_YERI:
        side = "AL"
    elif c <= kl - KIRILIM_PAYI * w and (h - c) / boy >= KAPANIS_YERI:
        side = "SAT"
    else:
        return None
    if VARYANT == "V1" and ust_trend != ("YÜKSELİŞ" if side == "AL" else "DÜŞÜŞ"):
        return None
    return side, ["SIKISMA"]


def stop(df, i, side, entry, atr):
    """Kutunun karşı ucu ± pay. Risk %1,2–%6,0 dışındaysa None (NO TRADE)."""
    kh, kl, w = float(df["kh"][i]), float(df["kl"][i]), float(df["w"][i])
    s = kl - STOP_PAYI * w if side == "AL" else kh + STOP_PAYI * w
    risk = abs(entry - s) / entry
    return s if RISK_MIN <= risk <= RISK_MAX else None
