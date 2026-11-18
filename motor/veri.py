"""
Veri: önce bt_calisma/veri/<SEMBOL>_1m.npz kullanılır. Eksik aylar data.binance.vision'dan
(USDⓈ-M futures, 1m) indirilip bt_calisma/ifvg_5m/veri/indirilen/ altına AYRI dosya olarak
kaydedilir. Mevcut dosyalara asla yazılmaz. 5m mumlar 1m'den, günlük mumlar 5m'den
(00:00 UTC sınırıyla) üretilir.
"""
import io
import zipfile
from pathlib import Path

import numpy as np
import requests

URL = "https://data.binance.vision/data/futures/um/{tur}/klines/{s}/1m/{s}-1m-{tarih}.zip"
DAKIKA_MS = 60_000
MUM5_MS = 300_000
GUN_MS = 86_400_000

ADLAR = {
    "t": ["t", "ts", "time", "zaman", "open_time", "opentime", "timestamp", "tarih", "date", "acilis_zamani"],
    "o": ["o", "open", "acilis", "ac"],
    "h": ["h", "high", "yuksek", "en_yuksek", "yk"],
    "l": ["l", "low", "dusuk", "en_dusuk", "dk"],
    "c": ["c", "close", "kapanis", "kp"],
}


class VeriHatasi(Exception):
    pass


def _ms_yap(a):
    a = np.asarray(a)
    if np.issubdtype(a.dtype, np.datetime64):
        return a.astype("datetime64[ms]").astype(np.int64)
    a = a.astype(np.int64, copy=False)
    if a.size == 0:
        return a
    if a.max() > 10**17:
        a = np.where(a > 10**17, a // 1_000_000, a)     # ns
    if a.max() > 10**14:
        a = np.where(a > 10**14, a // 1_000, a)         # µs
    if a.min() < 10**11:
        a = np.where(a < 10**11, a * 1000, a)           # s
    if np.mean(a[:: max(1, a.size // 10000)] % DAKIKA_MS == DAKIKA_MS - 1) > 0.5:   # kapanış zamanı
        a = a - (DAKIKA_MS - 1)
    return a


def _dogrula(t, o, h, l, c):
    ok = (h >= np.maximum(o, c) - 1e-9) & (l <= np.minimum(o, c) + 1e-9)
    return t.size > 0 and ok.mean() > 0.999


def npz_oku(yol):
    """Anahtar adlarını otomatik tanır. Tanıyamazsa içeriği anlatan VeriHatasi verir."""
    try:
        z = np.load(yol, allow_pickle=False)
        z.files
    except ValueError:
        z = np.load(yol, allow_pickle=True)
    with z:
        dosyalar = list(z.files)
        kucuk = {k.lower(): k for k in dosyalar}
        sec = {}
        for alan, adaylar in ADLAR.items():
            for a in adaylar:
                if a in kucuk:
                    sec[alan] = z[kucuk[a]]
                    break
        if len(sec) == 5:
            t = _ms_yap(sec["t"])
            o, h, l, c = (np.asarray(sec[x], dtype=np.float64) for x in "ohlc")
        else:
            iki = [k for k in dosyalar if z[k].ndim == 2 and z[k].shape[1] >= 5]
            if len(iki) != 1:
                ozet = ", ".join(f"{k}{z[k].shape}:{z[k].dtype}" for k in dosyalar)
                raise VeriHatasi(f"{Path(yol).name} anahtarları tanınamadı: {ozet}")
            m = z[iki[0]]
            kolon = None
            for k in dosyalar:
                if z[k].ndim == 1 and z[k].dtype.kind in "US" and len(z[k]) == m.shape[1]:
                    kolon = [str(x).lower() for x in z[k]]
            if kolon:
                idx = {}
                for alan, adaylar in ADLAR.items():
                    for a in adaylar:
                        if a in kolon:
                            idx[alan] = kolon.index(a)
                            break
                if len(idx) != 5:
                    raise VeriHatasi(f"{Path(yol).name} kolonları tanınamadı: {kolon}")
            else:
                idx = {"t": 0, "o": 1, "h": 2, "l": 3, "c": 4}
            t = _ms_yap(m[:, idx["t"]])
            o, h, l, c = (m[:, idx[x]].astype(np.float64) for x in "ohlc")
    if not _dogrula(t, o, h, l, c):
        raise VeriHatasi(f"{Path(yol).name}: OHLC doğrulaması başarısız (kolon sırası farklı olabilir)")
    if np.all(np.diff(t) > 0):
        return t, o, h, l, c
    sira = np.argsort(t, kind="stable")
    return t[sira], o[sira], h[sira], l[sira], c[sira]


def _zip_oku(icerik):
    import pandas as pd
    with zipfile.ZipFile(io.BytesIO(icerik)) as zf:
        ad = [x for x in zf.namelist() if x.endswith(".csv")][0]
        df = pd.read_csv(zf.open(ad), header=None, usecols=[0, 1, 2, 3, 4], dtype=str)
    df = df.apply(pd.to_numeric, errors="coerce").dropna()
    a = df.to_numpy(dtype=np.float64)
    return _ms_yap(a[:, 0]), a[:, 1], a[:, 2], a[:, 3], a[:, 4]


def _indir(url, hedef: Path, log):
    if hedef.exists():
        return hedef.read_bytes()
    r = requests.get(url, timeout=120)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    gecici = hedef.with_suffix(".parca")
    gecici.write_bytes(r.content)
    gecici.rename(hedef)
    log(f"indirildi: {hedef.name}")
    return r.content


def _ay_baslangic_ms(y, a):
    return int(np.datetime64(f"{y:04d}-{a:02d}-01", "ms").astype(np.int64))


def _aylar(bas, bit):
    y, a = bas
    while (y, a) <= bit:
        yield y, a
        a += 1
        if a == 13:
            y, a = y + 1, 1


def yukle_1m(sembol, eski_veri_dir: Path, indirme_dir: Path, bas=(2020, 10), bit=(2026, 10), log=print):
    """Dönüş: (t,o,h,l,c), rapor dict."""
    rapor = {"sembol": sembol, "kaynak": [], "indirilen_ay": [], "eksik_ay": []}
    parcalar = []
    yol = eski_veri_dir / f"{sembol}_1m.npz"
    if yol.exists():
        d = npz_oku(yol)
        parcalar.append(d)
        rapor["kaynak"].append(f"{yol.name}: {_tarih(d[0][0])} – {_tarih(d[0][-1])}, {len(d[0]):,} mum")
    else:
        rapor["kaynak"].append(f"{yol.name} yok")
    mevcut = parcalar[0][0] if parcalar else np.array([], dtype=np.int64)
    indirme_dir.mkdir(parents=True, exist_ok=True)
    for y, a in _aylar(bas, bit):
        bas_ms = _ay_baslangic_ms(y, a)
        y2, a2 = (y + 1, 1) if a == 12 else (y, a + 1)
        bit_ms = _ay_baslangic_ms(y2, a2)
        beklenen = (bit_ms - bas_ms) // DAKIKA_MS
        adet = int(np.searchsorted(mevcut, bit_ms) - np.searchsorted(mevcut, bas_ms))
        if adet >= 0.99 * beklenen:
            continue
        tarih = f"{y:04d}-{a:02d}"
        icerik = _indir(URL.format(tur="monthly", s=sembol, tarih=tarih),
                        indirme_dir / f"{sembol}-1m-{tarih}.zip", log)
        if icerik is not None:
            parcalar.append(_zip_oku(icerik))
            rapor["indirilen_ay"].append(tarih)
            continue
        # aylık dosya yoksa günlükler
        gun_say = 0
        for g in range((bit_ms - bas_ms) // GUN_MS):
            gun = np.datetime64(bas_ms + g * GUN_MS, "ms").astype("datetime64[D]")
            icerik = _indir(URL.format(tur="daily", s=sembol, tarih=str(gun)),
                            indirme_dir / f"{sembol}-1m-{gun}.zip", log)
            if icerik is not None:
                parcalar.append(_zip_oku(icerik))
                gun_say += 1
        rapor["indirilen_ay"].append(f"{tarih} ({gun_say} günlük dosya)")
    if not parcalar:
        raise VeriHatasi(f"{sembol}: veri bulunamadı")
    if len(parcalar) == 1:
        t, o, h, l, c = parcalar[0]
    else:
        t = np.concatenate([p[0] for p in parcalar])
        _, ilk = np.unique(t, return_index=True)        # aynı dakika varsa mevcut dosyadaki tercih edilir
        ilk.sort()
        t = t[ilk]
        cols = [np.concatenate([p[j] for p in parcalar])[ilk] for j in range(1, 5)]
        parcalar.clear()
        sira = np.argsort(t, kind="stable")
        t = t[sira]
        o, h, l, c = (x[sira] for x in cols)
        del cols
    alt, ust = _ay_baslangic_ms(*bas), _ay_baslangic_ms(*((bit[0] + 1, 1) if bit[1] == 12 else (bit[0], bit[1] + 1)))
    i0, i1 = np.searchsorted(t, alt), np.searchsorted(t, ust)
    if i0 > 0 or i1 < len(t):
        t, o, h, l, c = t[i0:i1], o[i0:i1], h[i0:i1], l[i0:i1], c[i0:i1]
    for y, a in _aylar(bas, bit):
        b = _ay_baslangic_ms(y, a)
        y2, a2 = (y + 1, 1) if a == 12 else (y, a + 1)
        e = _ay_baslangic_ms(y2, a2)
        adet = int(np.searchsorted(t, e) - np.searchsorted(t, b))
        if adet < 0.99 * (e - b) // DAKIKA_MS:
            rapor["eksik_ay"].append(f"{y:04d}-{a:02d} (%{100 * adet / ((e - b) // DAKIKA_MS):.1f})")
    rapor["toplam_1m"] = int(len(t))
    return (t, o, h, l, c), rapor


def birlestir(t, o, h, l, c, aralik_ms):
    """Mumları aralik_ms sınırlarında birleştirir (UTC)."""
    kova = (t // aralik_ms) * aralik_ms
    sinir = np.flatnonzero(np.diff(kova)) + 1
    bas = np.r_[0, sinir]
    son = np.r_[sinir - 1, len(t) - 1]
    return (kova[bas], o[bas], np.maximum.reduceat(h, bas), np.minimum.reduceat(l, bas), c[son])


def yukle_5m(sembol, eski_veri_dir: Path, cikti_veri_dir: Path, log=print):
    """5m veriyi döndürür; önbellek (kendi dosyamız) varsa onu kullanır."""
    onbellek = cikti_veri_dir / f"{sembol}_5m_2020-10_2026-10.npz"
    if onbellek.exists():
        z = np.load(onbellek)
        d = (z["t"], z["o"], z["h"], z["l"], z["c"])
        return d, {"sembol": sembol, "kaynak": [f"önbellek {onbellek.name}"], "indirilen_ay": [], "eksik_ay": [],
                   "toplam_5m": int(len(d[0]))}
    (t, o, h, l, c), rapor = yukle_1m(sembol, eski_veri_dir, cikti_veri_dir / "indirilen", log=log)
    d = birlestir(t, o, h, l, c, MUM5_MS)
    del t, o, h, l, c
    rapor["toplam_5m"] = int(len(d[0]))
    beklenen = (d[0][-1] - d[0][0]) // MUM5_MS + 1
    rapor["eksik_5m"] = int(beklenen - len(d[0]))
    if not rapor["eksik_ay"]:
        cikti_veri_dir.mkdir(parents=True, exist_ok=True)
        np.savez(onbellek, t=d[0], o=d[1], h=d[2], l=d[3], c=d[4])
    return d, rapor


def _tarih(ms):
    return str(np.datetime64(int(ms), "ms"))[:16].replace("T", " ")
