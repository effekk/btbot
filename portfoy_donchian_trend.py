"""
PORTFÖY STRATEJİSİ: Volatilite hedefli Donchian trend topluluğu  (kod: donchian_trend)

Kaynak: "Kripto Trading Stratejisi: Akademik Literatür Araştırması", Bölüm 7.4 ve 7.6
(şablon: Zarattini, Pagani & Barbon 2025, SSRN 5209907). Araştırmada "(varsayım)" diye işaretli
kurallar burada AYARLANABİLİR PARAMETRE olarak duruyor (PARAMETRELER içinde kaynak = "varsayım").

Bu modül SADECE hesaplar: her coin ve her gün için alt model sinyalleri, toplam sinyal S, volatilite,
hedef ağırlık ve rebalans sonrası ağırlık üretir. Emir göndermez, borsaya ve Telegram'a bağlanmaz.
signal_bot'un tek işlemli strateji yapısına (strateji_<kod>.py) uymaz; bu yüzden adı portfoy_ ile başlar
ve STRATEJILER listesine eklenmez.

Girdi: her coin için günlük mumlar (UTC 00:00 kapanış), pandas DataFrame:
    index  : gün (UTC, artan, boşluksuz olması beklenir)
    sütunlar: open, high, low, close, quote_volume (USDT cinsinden günlük hacim; evren seçimi için)
Zaman kuralı: t gününün kapanışında hesaplanan her şey yalnızca t ve öncesindeki veriyi kullanır;
ağırlık t+1 gününün ilk fiyatından (açılış) itibaren uygulanır.

Formüller (Bölüm 7.3):
    U(n,t) = max(H[t-n .. t-1]),  L(n,t) = min(L[t-n .. t-1])           (t günü HARİÇ)
    S_i(t) = (1/|N|) Σ s_{i,n}(t),  N = DONCHIAN_PERIYOTLARI
    w_i(t) = (1/K) · S_i(t) · min(σ* / σ̂_i(t), L_max)
    σ̂ = günlük getirilerin standart sapması × √365
"""
import math

import numpy as np
import pandas as pd

KOD = "donchian_trend"
AD = "Volatilite Hedefli Donchian Trend Topluluğu"
ZAMAN_DILIMI = "1d"           # Bölüm 7.7: günlük mum, UTC 00:00 kapanış

# =========================================================================
# PARAMETRELER  (kaynak: "makale" = araştırmada Zarattini ve ark.'na dayanan kural,
#                         "varsayım" = araştırmada (varsayım) diye işaretli, ayarlanabilir)
# =========================================================================
PARAMETRELER = {
    # --- Bölüm 7.6 ---
    "DONCHIAN_PERIYOTLARI": ((5, 10, 20, 30, 60, 90, 150, 250, 360), "makale"),
    "HEDEF_VOL": (0.25, "makale"),                 # σ*, yıllık
    "COIN_KALDIRAC_TAVANI": (2.0, "makale"),       # L_max: min(σ*/σ̂, L_max)
    "TOPLAM_MARUZIYET_TAVANI": (1.0, "varsayım"),  # Σw üst sınırı; None = sınır yok (sadece L_max)
    "VOL_PENCERE": (30, "varsayım"),               # gerçekleşen volatilite penceresi (gün)
    "REBALANS_ESIGI": (0.20, "makale"),            # sadece volatiliteden doğan ağırlık farkı bunu aşarsa düzelt
    # --- Bölüm 7.4: evren ---
    "EVREN_BOYUTU": (20, "makale"),                # hacme göre ilk 20
    "EVREN_MIN_GUN": (365, "makale"),              # en az 365 gündür listelenmiş
    "EVREN_HACIM_GUN": (30, "makale"),             # son 30 günün medyan günlük hacmi
    "EVREN_MIN_HACIM": (2_000_000, "makale"),      # ≥ 2 milyon $
    # --- Bölüm 7.4: trailing stop (varsayım) ---
    "STOP_BASLANGIC": ("alt", "varsayım"),         # giriş anında stop: "alt" = L(n,t)
    "STOP_GUNCELLEME": ("orta", "varsayım"),       # her gün aday: "orta" = (U+L)/2, "alt" = L(n,t)
    # --- Bölüm 7.5 (varsayım, mühendislik önlemleri; emir değil, sadece seviye/uyarı üretilir) ---
    "FELAKET_STOP_ATR": (3.0, "varsayım"),         # borsada bekleyecek stop: giriş − 3 × ATR(20); None = kapalı
    "FELAKET_STOP_ATR_PERIYOT": (20, "varsayım"),
    "KILL_SWITCH_DUSUS": (0.25, "varsayım"),       # hesap zirveden %25 düşerse yeni giriş yok; None = kapalı
}

# Evrenden çıkarılacak stablecoin / wrapped coinler (Bölüm 7.4). Taban sembol (USDT'siz) olarak.
HARIC_COINLER = {
    "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FDUSD", "USDD", "PYUSD", "USDE", "EUR", "EURI", "AEUR",
    "GUSD", "FRAX", "LUSD", "SUSD", "UST", "USTC", "XUSD", "USD1",
    "WBTC", "WETH", "WBETH", "STETH", "WSTETH", "CBETH", "RETH", "BETH", "WBNB", "BTCB", "PAXG",
}


def param(ad, ayar=None):
    """Parametre değeri; ayar sözlüğünde varsa o (backtest varyantları için), yoksa varsayılan."""
    if ayar and ad in ayar:
        return ayar[ad]
    return PARAMETRELER[ad][0]


# =========================================================================
# Göstergeler
# =========================================================================
def donchian(df, n):
    """U(n,t), L(n,t): önceki n günün en yükseği / en düşüğü (t günü hariç). İlk n gün NaN."""
    return df["high"].shift(1).rolling(n).max(), df["low"].shift(1).rolling(n).min()


def gercek_vol(df, pencere):
    """Yıllıklandırılmış gerçekleşen volatilite: son `pencere` günün günlük getirilerinin std'si × √365."""
    return df["close"].pct_change().rolling(pencere).std() * math.sqrt(365)


def atr(df, n):
    """Wilder ATR(n) (felaket stop'u için)."""
    onceki = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - onceki).abs(), (df["low"] - onceki).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()


# =========================================================================
# Alt modeller (Bölüm 7.4): giriş, trailing stop, çıkış
# =========================================================================
def alt_model(df, n, ayar=None):
    """
    Tek periyotlu alt model. Dönüş: (s, stop) – iki pd.Series.
      Giriş: pozisyon yokken C(t) ≥ U(n,t) → s = 1, stop = L(n,t) (STOP_BASLANGIC="alt").
      Pozisyondayken her gün: aday = (U+L)/2 (STOP_GUNCELLEME="orta"), stop = max(önceki stop, aday)
        (stop asla aşağı inmez); C(t) < stop → s = 0 (çıkış). Çıkıştan sonra yeniden giriş aynı kuralla.
    U ve L t günü hariç önceki n günden hesaplandığı için stop'u güncelleyip aynı günün kapanışıyla
    karşılaştırmak ileriye bakma değildir.
    """
    U, L = donchian(df, n)
    u, l, c = U.to_numpy(), L.to_numpy(), df["close"].to_numpy()
    bas, gun = param("STOP_BASLANGIC", ayar), param("STOP_GUNCELLEME", ayar)

    def seviye(tur, i):
        if tur == "alt":
            return l[i]
        if tur == "orta":
            return (u[i] + l[i]) / 2
        raise ValueError(f"bilinmeyen stop türü: {tur!r}")

    s = np.zeros(len(c))
    st = np.full(len(c), np.nan)
    acik, stop = False, np.nan
    for i in range(len(c)):
        if np.isnan(u[i]) or np.isnan(l[i]) or np.isnan(c[i]):
            acik, stop = False, np.nan
            continue
        if acik:
            stop = max(stop, seviye(gun, i))
            if c[i] < stop:
                acik, stop = False, np.nan
        elif c[i] >= u[i]:
            acik, stop = True, seviye(bas, i)
        s[i] = 1.0 if acik else 0.0
        st[i] = stop if acik else np.nan
    return pd.Series(s, index=df.index), pd.Series(st, index=df.index)


def coin_sinyalleri(df, ayar=None):
    """
    Bir coin için tüm alt modeller. Dönüş: DataFrame
      s_<n>, stop_<n> (her periyot), S (ortalama sinyal, 0…1), vol (σ̂), atr (felaket stop'u için).
    """
    out = pd.DataFrame(index=df.index)
    periyotlar = param("DONCHIAN_PERIYOTLARI", ayar)
    for n in periyotlar:
        out[f"s_{n}"], out[f"stop_{n}"] = alt_model(df, n, ayar)
    out["S"] = out[[f"s_{n}" for n in periyotlar]].mean(axis=1)
    out["vol"] = gercek_vol(df, param("VOL_PENCERE", ayar))
    out["atr"] = atr(df, param("FELAKET_STOP_ATR_PERIYOT", ayar))
    return out


# =========================================================================
# Evren (Bölüm 7.4): aylık
# =========================================================================
def evren_sec(gunluk, gun, ayar=None):
    """
    `gun` tarihinde (ayın ilk günü) geçerli evren: `gun`den ÖNCEKİ verilerle seçilir.
      - ilk mumu en az EVREN_MIN_GUN gün önce olan,
      - HARIC_COINLER'de olmayan,
      - son EVREN_HACIM_GUN günün medyan quote_volume'u ≥ EVREN_MIN_HACIM olan coinler;
      - medyan hacme göre ilk EVREN_BOYUTU.
    gunluk: {coin (ör. "BTC_USDT" ya da "BTCUSDT"): DataFrame}. Dönüş: coin listesi (hacme göre sıralı).
    """
    gun = pd.Timestamp(gun)
    aday = []
    for coin, df in gunluk.items():
        taban = coin.replace("_", "").upper()
        taban = taban[:-4] if taban.endswith("USDT") else taban
        taban = taban.lstrip("0123456789")                 # 1000PEPE → PEPE
        if taban in HARIC_COINLER:
            continue
        gecmis = df[df.index < gun]
        if gecmis.empty or (gun - gecmis.index[0]).days < param("EVREN_MIN_GUN", ayar):
            continue
        hacim = gecmis["quote_volume"].iloc[-param("EVREN_HACIM_GUN", ayar):].median()
        if hacim >= param("EVREN_MIN_HACIM", ayar):
            aday.append((hacim, coin))
    aday.sort(reverse=True)
    return [c for _, c in aday[:param("EVREN_BOYUTU", ayar)]]


# =========================================================================
# Ağırlıklar (Bölüm 7.3–7.4, 7.6)
# =========================================================================
def hedef_agirlik(S, vol, K, ayar=None):
    """w = (1/K) · S · min(σ*/σ̂, L_max). σ̂ yoksa ya da 0 ise 0."""
    if K <= 0 or S <= 0 or vol is None or not np.isfinite(vol) or vol <= 0:
        return 0.0
    return (1.0 / K) * S * min(param("HEDEF_VOL", ayar) / vol, param("COIN_KALDIRAC_TAVANI", ayar))


def toplam_tavan(w, ayar=None):
    """TOPLAM_MARUZIYET_TAVANI (varsayım): Σw tavanı aşarsa hepsi orantılı küçültülür."""
    tavan = param("TOPLAM_MARUZIYET_TAVANI", ayar)
    top = sum(w.values())
    if tavan is None or top <= tavan or top <= 0:
        return dict(w)
    return {c: v * tavan / top for c, v in w.items()}


def rebalans(onceki_w, hedef_w, onceki_S, yeni_S, ayar=None):
    """
    Bölüm 7.4: sinyal değişimi anında uygulanır; sadece volatiliteden kaynaklı ağırlık farkı
    REBALANS_ESIGI'ni aşarsa düzeltilir. Fark, mevcut ağırlığa göre görelidir: |hedef − mevcut| / mevcut.
    Dönüş: {coin: uygulanacak ağırlık}, {coin: sebep} (sebep: "sinyal" | "vol" | "aynı").
    """
    esik = param("REBALANS_ESIGI", ayar)
    yeni, sebep = {}, {}
    for c in set(onceki_w) | set(hedef_w):
        w0, w1 = onceki_w.get(c, 0.0), hedef_w.get(c, 0.0)
        if onceki_S.get(c, 0.0) != yeni_S.get(c, 0.0) or w0 == 0.0 or w1 == 0.0:
            yeni[c], sebep[c] = w1, ("sinyal" if w1 != w0 else "aynı")
        elif abs(w1 - w0) / w0 > esik:
            yeni[c], sebep[c] = w1, "vol"
        else:
            yeni[c], sebep[c] = w0, "aynı"
    return yeni, sebep


def felaket_stop(giris_fiyati, atr_degeri, ayar=None):
    """Bölüm 7.5 (varsayım): borsada bekleyecek stop seviyesi = giriş − FELAKET_STOP_ATR × ATR(20). None = kapalı."""
    k = param("FELAKET_STOP_ATR", ayar)
    if k is None or atr_degeri is None or not np.isfinite(atr_degeri):
        return None
    return giris_fiyati - k * atr_degeri


def kill_switch(ozkaynak, ayar=None):
    """Bölüm 7.5 (varsayım): özkaynak zirveden KILL_SWITCH_DUSUS kadar düştüyse True (yeni giriş yok)."""
    esik = param("KILL_SWITCH_DUSUS", ayar)
    if esik is None or not len(ozkaynak):
        return False
    zirve = max(ozkaynak)
    return zirve > 0 and ozkaynak[-1] <= zirve * (1 - esik)


# =========================================================================
# Ana hesap: tüm günler için ağırlıklar
# =========================================================================
def portfoy_hesapla(gunluk, baslangic=None, bitis=None, ayar=None):
    """
    gunluk: {coin: günlük DataFrame}. Her gün t için (kapanışta) hesaplanır, t+1 açılışından uygulanır.
    Dönüş: dict
      "agirlik": DataFrame (gün × coin) – rebalans sonrası uygulanacak ağırlık (t kapanışında karar)
      "hedef":   DataFrame (gün × coin) – rebalans öncesi hedef ağırlık (tavan uygulanmış)
      "S":       DataFrame (gün × coin) – toplam sinyal (evren dışı coinlerde de hesaplanır, bilgi için)
      "sebep":   DataFrame (gün × coin) – "sinyal" | "vol" | "aynı" | "" (evren dışı)
      "evren":   {ay başı: [coin, ...]}
      "sinyaller": {coin: coin_sinyalleri(...) çıktısı}
    Kill-switch ve felaket stop'u burada uygulanmaz (özkaynak ve giriş fiyatı backtest/trader tarafında);
    fonksiyonları ayrı (kill_switch, felaket_stop).
    """
    sinyaller = {c: coin_sinyalleri(df, ayar) for c, df in gunluk.items()}
    gunler = sorted(set().union(*[set(df.index) for df in gunluk.values()]))
    gunler = [g for g in gunler if (baslangic is None or g >= pd.Timestamp(baslangic))
              and (bitis is None or g <= pd.Timestamp(bitis))]
    coinler = list(gunluk)
    agirlik = pd.DataFrame(0.0, index=gunler, columns=coinler)
    hedef = agirlik.copy()
    Sdf = pd.DataFrame(np.nan, index=gunler, columns=coinler)
    sebep_df = pd.DataFrame("", index=gunler, columns=coinler)
    evren_log = {}
    evren, ay = [], None
    onceki_w, onceki_S = {}, {}
    for g in gunler:
        if (g.year, g.month) != ay:                       # ayın ilk günü (verideki ilk gün) evren yenilenir
            ay = (g.year, g.month)
            evren = evren_sec(gunluk, g, ayar)
            evren_log[g] = list(evren)
        K = len(evren)
        S_t, w_t = {}, {}
        for c in coinler:
            sg = sinyaller[c]
            if g not in sg.index:
                continue
            S = float(sg.at[g, "S"]) if np.isfinite(sg.at[g, "S"]) else 0.0
            Sdf.at[g, c] = S
            if c in evren:
                S_t[c] = S
                w_t[c] = hedef_agirlik(S, float(sg.at[g, "vol"]), K, ayar)
        w_t = toplam_tavan(w_t, ayar)
        # Evrenden çıkan coin: hedef 0 (sinyal değişimi gibi hemen uygulanır)
        for c in onceki_w:
            if c not in w_t:
                w_t[c], S_t[c] = 0.0, 0.0
        yeni, sebep = rebalans(onceki_w, w_t, onceki_S, S_t, ayar)
        for c, v in w_t.items():
            hedef.at[g, c] = v
        for c, v in yeni.items():
            agirlik.at[g, c] = v
            sebep_df.at[g, c] = sebep[c]
        onceki_w = {c: v for c, v in yeni.items() if v > 0}
        onceki_S = {c: S_t.get(c, 0.0) for c in onceki_w}
    return {"agirlik": agirlik, "hedef": hedef, "S": Sdf, "sebep": sebep_df, "evren": evren_log,
            "sinyaller": sinyaller}


def parametre_ozeti(ayar=None):
    """Rapor başlığı için: her parametrenin kullanılan değeri ve kaynağı."""
    return [f"{ad} = {param(ad, ayar)!r} ({kaynak})" for ad, (_, kaynak) in PARAMETRELER.items()]
