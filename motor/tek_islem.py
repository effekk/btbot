"""
Tek işlemli backtest motoru (coin başına aynı anda 1 pozisyon).

Mum içi sıra (i mumu):
  1) Açılış: önceki kapanışta seçilen piyasa tetiği varsa giriş değerlendirilir (açılış fiyatı).
  2) Kapanış: strateji mumu işler, yeni tetikler üretir.
(cikis_bul'daki limit_mi seçeneği limit girişli stratejiler için genel olarak duruyor.)
Çıkış: aynı mumda stop ve hedef varsa önce stop. Hedef limit emirdir: high hedefin
ÜSTÜNE geçmeli. 48. mum kapanınca sonraki açılışta piyasa emriyle çıkış.
Stop'ta açılış boşluğu varsa (açılış stopun ötesinde) çıkış açılış fiyatından.
"""
from strateji.ifvg_5m import IfvgStrateji, MUM_MS, GUN_MS

MALIYET = {
    "taker": 0.0005,
    "maker": 0.0002,
    "kayma": 0.0002,          # BTC/ETH; piyasa girişi, stop, süre dolumu
    "funding_8s": 0.0001,     # 8 saatte, iki yönde de maliyet
    "risk_usdt": 300.0,
}


def maliyet_hesapla(giris_tipi, cikis_tipi, giris, cikis, d, sure_ms, m=MALIYET):
    """Maliyeti R cinsinden ve USDT olarak döndürür (risk = m['risk_usdt'])."""
    g_oran = m["maker"] if giris_tipi == "limit" else m["taker"] + m["kayma"]
    c_oran = m["maker"] if cikis_tipi == "hedef" else m["taker"] + m["kayma"]
    g_kom = m["maker"] if giris_tipi == "limit" else m["taker"]
    c_kom = m["maker"] if cikis_tipi == "hedef" else m["taker"]
    funding = m["funding_8s"] * giris * (sure_ms / (8 * 3600 * 1000))
    maliyet_fiyat = g_oran * giris + c_oran * cikis + funding     # birim miktar başına
    adet = m["risk_usdt"] / d
    return {
        "maliyet_R": maliyet_fiyat / d,
        "maliyet_usdt": maliyet_fiyat * adet,
        "komisyon_usdt": (g_kom * giris + c_kom * cikis) * adet,
    }


def cikis_bul(T, O, H, L, C, e, yon, stop, hedef, limit_mi, omur=48):
    """e: giriş mumu. Dönüş: (çıkış mumu, fiyat, tip, çıkış zamanı ms, aktif son mum)."""
    n = len(C)
    son = min(e + omur, n)
    for j in range(e, son):
        if yon > 0:
            if L[j] <= stop:
                px = stop if j == e else min(O[j], stop)
                return j, px, "stop", T[j] + MUM_MS, j
            if not (limit_mi and j == e) and H[j] > hedef:
                return j, hedef, "hedef", T[j] + MUM_MS, j
        else:
            if H[j] >= stop:
                px = stop if j == e else max(O[j], stop)
                return j, px, "stop", T[j] + MUM_MS, j
            if not (limit_mi and j == e) and L[j] < hedef:
                return j, hedef, "hedef", T[j] + MUM_MS, j
    if e + omur < n:
        x = e + omur
        return x, O[x], "sure", T[x], x - 1
    return n - 1, C[n - 1], "veri_sonu", T[n - 1] + MUM_MS, n - 1


def calistir(sembol, T, O, H, L, C, gun_bias, varyant, bas_ms, bit_ms, param=None, maliyet=MALIYET):
    """T/O/H/L/C: listeler (ısınma verisi dahil, bit_ms'den sonrası kesilmiş olmalı).
    Sadece girişi [bas_ms, bit_ms] aralığında olan işlemler alınır."""
    st = IfvgStrateji(T, O, H, L, C, gun_bias, varyant, param)
    p = st.p
    n = len(C)
    islemler = []
    dusen = st.dusen
    poz = None              # dict: e, x, aktif_son
    bekleyen = None         # seçilmiş piyasa tetiği (Kurulum)
    gun_sayac = {}

    def donemde(t):
        return bas_ms <= t <= bit_ms

    def kaydet(k, e, giris, giris_tipi):
        nonlocal poz
        stop, hedef = k.stop, k.yon * k.hedef
        x, cpx, ctip, czaman, aktif_son = cikis_bul(T, O, H, L, C, e, k.yon, stop, hedef,
                                                   giris_tipi == "limit", p["pozisyon_omur"])
        d = abs(giris - stop)
        brut = k.yon * (cpx - giris) / d
        mal = maliyet_hesapla(giris_tipi, ctip, giris, cpx, d, czaman - T[e], maliyet)
        poz = {"e": e, "x": x, "aktif_son": aktif_son}
        g = T[e] // GUN_MS
        gun_sayac[g] = gun_sayac.get(g, 0) + 1
        islemler.append({
            "sembol": sembol, "varyant": varyant, "id": k.id, "senaryo": k.senaryo, "tip": k.tip,
            "yon": k.yon, "m1": k.m1, "m3": k.m3, "inv": k.k, "tetik": k.tetik_mum,
            "tetik_turu": k.tetik_turu, "duzeltme": k.duzeltme_mum, "duzeltme_h": k.fiyat(k.duzeltme_h),
            "fvg_atr_orani": k.atr_orani,
            "bolge_stop": k.stop, "bolge_giris": k.giris_kenari, "maks3": k.fiyat(k.maks3),
            "e": e, "giris_zaman": T[e], "giris": giris, "giris_tipi": giris_tipi,
            "stop": stop, "hedef": hedef, "hedef_tip": k.hedef_tip,
            "x": x, "cikis_zaman": czaman, "cikis": cpx, "cikis_tipi": ctip,
            "stop_yuzde": d / giris * 100, "hedef_R": abs(hedef - giris) / d,
            "brut_R": brut, "maliyet_R": mal["maliyet_R"], "net_R": brut - mal["maliyet_R"],
            "maliyet_usdt": mal["maliyet_usdt"], "komisyon_usdt": mal["komisyon_usdt"],
            "bias": st.bias(T[e]),
        })

    basladi = False
    for i in range(n):
        t = T[i]
        if not basladi and t >= bas_ms:
            dusen.clear()            # sayımlar sadece dönem içi
            basladi = True
        # 1) açılış: piyasa girişi
        if bekleyen is not None:
            k, bekleyen = bekleyen, None
            if donemde(t):
                g = t // GUN_MS
                if gun_sayac.get(g, 0) >= p["gunluk_max_islem"]:
                    dusen["gunluk_sinir"] += 1
                else:
                    sebep, _ = st.giris_degerlendir(k, O[i], t)
                    if sebep:
                        dusen[sebep] += 1
                    else:
                        kaydet(k, i, O[i], "piyasa")
        # 3) kapanış
        acik = poz is not None and poz["e"] <= i < poz["x"]
        tetikler = st.mum_kapandi(i)
        if tetikler:
            sonraki_donemde = i + 1 < n and donemde(T[i + 1])
            if acik:
                if sonraki_donemde:
                    dusen["pozisyon_acik"] += len(tetikler)
            else:
                tetikler.sort(key=lambda k: -k.m3)      # en yeni FVG önce (onay bekliyor)
                bekleyen = tetikler[0]
                if sonraki_donemde:
                    dusen["cakisma"] += len(tetikler) - 1
        if poz is not None and i >= poz["aktif_son"]:
            poz = None
    return islemler, dict(dusen)
