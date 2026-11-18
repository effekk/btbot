"""
İstatistikler, karar kuralları ve zorunlu karşılaştırmalar.
Karşılaştırmalarda çıkış kuralları motordakiyle aynıdır (vektörel sim_toplu);
gerçek işlemlerin motor sonucu ile sim_toplu sonucu tutarlılık için karşılaştırılır.
"""
import numpy as np

from motor.tek_islem import MALIYET

MUM_MS = 300_000
GUN_MS = 86_400_000

KARAR = {"ort_R": 0.10, "bs_alt": 0.0, "pf": 1.2, "karli_ceyrek": 0.60, "t_rastgele": 2.0}


# ---------------------------------------------------------------------------
def sim_toplu(V, e, yon, giris, d, hedef_R, limit_mi, omur=48, m=MALIYET):
    """Vektörel işlem simülasyonu. V: dict(t,o,h,l,c) numpy. Diziler işlem başına.
    Stop = giris - yon*d, hedef = giris + yon*hedef_R*d. Dönüş: net R dizisi."""
    t, O, H, L, C = V["t"], V["o"], V["h"], V["l"], V["c"]
    n = len(C)
    N = len(e)
    if N == 0:
        return np.array([])
    stop = giris - yon * d
    hedef = giris + yon * hedef_R * d
    idx = e[:, None] + np.arange(omur)[None, :]
    gecerli = idx < n
    idxc = np.minimum(idx, n - 1)
    Hh, Ll, Oo = H[idxc], L[idxc], O[idxc]
    uzun = (yon > 0)[:, None]
    stop_vur = np.where(uzun, Ll <= stop[:, None], Hh >= stop[:, None]) & gecerli
    hedef_vur = np.where(uzun, Hh > hedef[:, None], Ll < hedef[:, None]) & gecerli
    hedef_vur[:, 0] &= ~limit_mi
    BUYUK = omur + 10
    s_ilk = np.where(stop_vur.any(1), stop_vur.argmax(1), BUYUK)
    h_ilk = np.where(hedef_vur.any(1), hedef_vur.argmax(1), BUYUK)
    satir = np.arange(N)
    cikis = np.empty(N)
    tip = np.empty(N, dtype=object)
    czaman = np.empty(N, dtype=np.int64)
    # stop
    ms = (s_ilk <= h_ilk) & (s_ilk < BUYUK)
    j = np.minimum(s_ilk, omur - 1)
    o_j = Oo[satir, j]
    px = np.where(yon > 0, np.minimum(o_j, stop), np.maximum(o_j, stop))
    px = np.where(j == 0, stop, px)
    cikis[ms] = px[ms]
    tip[ms] = "stop"
    czaman[ms] = t[idxc[satir, j]][ms] + MUM_MS
    # hedef
    mh = (h_ilk < s_ilk) & (h_ilk < BUYUK)
    jh = np.minimum(h_ilk, omur - 1)
    cikis[mh] = hedef[mh]
    tip[mh] = "hedef"
    czaman[mh] = t[idxc[satir, jh]][mh] + MUM_MS
    # süre / veri sonu
    mr = ~(ms | mh)
    xs = e + omur
    sure = mr & (xs < n)
    cikis[sure] = O[np.minimum(xs, n - 1)][sure]
    tip[sure] = "sure"
    czaman[sure] = t[np.minimum(xs, n - 1)][sure]
    vs = mr & ~(xs < n)
    cikis[vs] = C[n - 1]
    tip[vs] = "veri_sonu"
    czaman[vs] = t[n - 1] + MUM_MS
    brut = yon * (cikis - giris) / d
    g_oran = np.where(limit_mi, m["maker"], m["taker"] + m["kayma"])
    c_oran = np.where(tip == "hedef", m["maker"], m["taker"] + m["kayma"])
    sure_ms = czaman - t[np.minimum(e, n - 1)]
    funding = m["funding_8s"] * giris * sure_ms / (8 * 3600 * 1000)
    mal = (g_oran * giris + c_oran * cikis + funding) / d
    return brut - mal


def islem_dizileri(islemler):
    e = np.array([x["e"] for x in islemler], dtype=np.int64)
    yon = np.array([x["yon"] for x in islemler], dtype=np.float64)
    giris = np.array([x["giris"] for x in islemler], dtype=np.float64)
    d = np.abs(giris - np.array([x["stop"] for x in islemler], dtype=np.float64))
    hR = np.array([x["hedef_R"] for x in islemler], dtype=np.float64)
    lim = np.array([x["giris_tipi"] == "limit" for x in islemler], dtype=bool)
    return e, yon, giris, d, hR, lim


# ---------------------------------------------------------------------------
def _ay_anahtari(ms):
    return np.asarray(ms, dtype="datetime64[ms]").astype("datetime64[M]")


def ozet(R, giris_ms, cikis_ms, bas_ms, bit_ms, rng, bs_tekrar=10_000):
    R = np.asarray(R, dtype=float)
    sonuc = {"n": int(len(R))}
    if len(R) == 0:
        return sonuc
    poz, neg = R[R > 0].sum(), -R[R < 0].sum()
    sonuc.update({
        "kazanma": float((R > 0).mean()),
        "ort_R": float(R.mean()),
        "toplam_R": float(R.sum()),
        "pf": float(poz / neg) if neg > 0 else float("inf"),
    })
    # aylık blok bootstrap (dönemdeki tüm aylar, işlemsiz aylar dahil)
    aylar = np.arange(_ay_anahtari(bas_ms), _ay_anahtari(bit_ms) + 1)
    ay = _ay_anahtari(giris_ms)
    konum = np.searchsorted(aylar, ay)
    toplam = np.bincount(konum, weights=R, minlength=len(aylar))
    adet = np.bincount(konum, minlength=len(aylar))
    secim = rng.integers(0, len(aylar), size=(bs_tekrar, len(aylar)))
    st, sa = toplam[secim].sum(1), adet[secim].sum(1)
    ortlar = st[sa > 0] / sa[sa > 0]
    sonuc["bs_alt"], sonuc["bs_ust"] = (float(x) for x in np.percentile(ortlar, [2.5, 97.5]))
    # çeyrekler
    g = np.asarray(giris_ms, dtype="datetime64[ms]").astype("datetime64[M]").astype(int)
    yil, ayno = g // 12 + 1970, g % 12
    ceyrek = [f"{y}-Ç{a // 3 + 1}" for y, a in zip(yil, ayno)]
    tablo = {}
    for c_, r in zip(ceyrek, R):
        x = tablo.setdefault(c_, [0, 0.0])
        x[0] += 1
        x[1] += r
    sonuc["ceyrekler"] = {k: {"n": v[0], "toplam_R": v[1], "ort_R": v[1] / v[0]} for k, v in sorted(tablo.items())}
    sonuc["karli_ceyrek"] = float(np.mean([v[1] > 0 for v in tablo.values()]))
    # maksimum düşüş (R), çıkış sırasına göre
    sira = np.argsort(cikis_ms, kind="stable")
    egri = np.cumsum(R[sira])
    tepe = np.maximum.accumulate(np.r_[0.0, egri])[1:]
    sonuc["maks_dusus_R"] = float((tepe - egri).max())
    return sonuc


def rastgele_yon(R_long, R_short, R_gercek, rng, tekrar=1000):
    """Aynı giriş zamanı ve stop/hedef mesafeleri, yön yazı-tura."""
    N = len(R_gercek)
    if N < 2:
        return {"t": float("nan")}
    yaz = rng.integers(0, 2, size=(tekrar, N)).astype(bool)
    ortlar = np.where(yaz, R_long[None, :], R_short[None, :]).mean(1)
    return t_degeri(R_gercek, ortlar)


def rastgele_zaman_toplam(V, islemler, rng, tekrar=200):
    """Bias yönünde, aynı coinde, aynı gün dağılımıyla rastgele zamanlı girişler.
    Stop % / hedef R / giriş tipi aynı coinin gerçek işlemlerinden birlikte örneklenir.
    Rastgele giriş, giriş tipine göre ücretlendirilir; giriş anı mumun açılışıdır.
    Dönüş: tekrar başına toplam net R dizisi (birden fazla coin toplanabilsin diye)."""
    N = len(islemler)
    if N == 0:
        return np.zeros(tekrar)
    t = V["t"]
    gun_no = np.array([x["giris_zaman"] // GUN_MS for x in islemler])
    yon = np.array([x["yon"] for x in islemler], dtype=float)
    stop_y = np.array([x["stop_yuzde"] / 100 for x in islemler])
    hR = np.array([x["hedef_R"] for x in islemler])
    lim = np.array([x["giris_tipi"] == "limit" for x in islemler])
    gun_t = t // GUN_MS
    bas = np.searchsorted(gun_t, gun_no, "left")
    bit = np.searchsorted(gun_t, gun_no, "right")
    toplam = np.empty(tekrar)
    for r in range(tekrar):
        e = bas + (rng.random(N) * (bit - bas)).astype(np.int64)
        orn = rng.integers(0, N, N)
        giris = V["o"][e]
        toplam[r] = sim_toplu(V, e, yon, giris, giris * stop_y[orn], hR[orn], lim[orn]).sum()
    return toplam


def t_degeri(gercek_R, rastgele_ortlar):
    std = rastgele_ortlar.std(ddof=1)
    g = float(np.mean(gercek_R))
    return {"rastgele_ort": float(rastgele_ortlar.mean()), "rastgele_std": float(std),
            "t": float((g - rastgele_ortlar.mean()) / std) if std > 0 else float("nan"),
            "yuzdelik": float((rastgele_ortlar < g).mean() * 100)}


def karar(oz, t_rast):
    k = {
        "ort_R": oz.get("ort_R", -1) >= KARAR["ort_R"],
        "bs_alt": oz.get("bs_alt", -1) > KARAR["bs_alt"],
        "pf": oz.get("pf", 0) >= KARAR["pf"],
        "karli_ceyrek": oz.get("karli_ceyrek", 0) >= KARAR["karli_ceyrek"],
        "t_rastgele": (t_rast if np.isfinite(t_rast) else -1) >= KARAR["t_rastgele"],
    }
    return k, all(k.values())
