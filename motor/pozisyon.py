"""
Pozisyon boyutu kontrolü (karar kurallarını DEĞİŞTİRMEZ, sadece raporlanır).
Risk 300 USDT, başlangıç 5.000 USDT, isolated margin.
Teminat <= güncel özsermayenin %40'ı (pozisyon başına); kaldıraç = yukarı yuvarlanmış
(büyüklük / azami teminat). Kaldıraç bracket sınırını aşarsa ya da tasfiye fiyatı
stoptan en az 2 kat uzakta değilse işlem atlanır.
"""
import hashlib
import hmac
import json
import math
import os
import time
from pathlib import Path

import numpy as np
import requests

TABANLAR = ["https://demo-fapi.binance.com", "https://testnet.binancefuture.com"]


def bracket_al(dosya: Path, semboller, log=print):
    """Önce kayıtlı dosya; yoksa secrets.env'deki demo anahtarıyla bir kez çekip kaydeder."""
    if dosya.exists():
        return json.loads(dosya.read_text()), f"kayıtlı dosya ({dosya.name})"
    anahtar, gizli = os.environ.get("BINANCE_API_KEY"), os.environ.get("BINANCE_API_SECRET")
    if not anahtar or not gizli:
        return None, "BINANCE_API_KEY/SECRET bulunamadı"
    hatalar = []
    for taban in TABANLAR:
        sonuc = {}
        try:
            for s in semboller:
                q = f"symbol={s}&timestamp={int(time.time() * 1000)}&recvWindow=10000"
                imza = hmac.new(gizli.encode(), q.encode(), hashlib.sha256).hexdigest()
                r = requests.get(f"{taban}/fapi/v1/leverageBracket?{q}&signature={imza}",
                                 headers={"X-MBX-APIKEY": anahtar}, timeout=20)
                r.raise_for_status()
                veri = r.json()
                veri = veri[0] if isinstance(veri, list) else veri
                sonuc[s] = sorted(veri["brackets"], key=lambda b: b["notionalFloor"])
            dosya.write_text(json.dumps({"kaynak": taban, "zaman": int(time.time()), "bracket": sonuc}, indent=1))
            log(f"leverage bracket alındı: {taban}")
            return json.loads(dosya.read_text()), f"{taban} (demo; canlıdan farklı olabilir)"
        except Exception as ex:
            hatalar.append(f"{taban}: {str(ex)[:120]}")
    return None, "bracket alınamadı: " + " | ".join(hatalar)


def _bracket(liste, notional):
    for b in liste:
        if b["notionalFloor"] <= notional < b["notionalCap"]:
            return b
    return liste[-1]


def tasfiye_fiyati(yon, adet, giris, teminat, mmr, cum):
    """Binance isolated, tek pozisyon (hedge yok)."""
    if yon > 0:
        return (teminat + cum - adet * giris) / (adet * mmr - adet)
    return (teminat + cum + adet * giris) / (adet * mmr + adet)


def simule(islemler, bracket, baslangic=5000.0, risk=300.0, teminat_orani=0.40, tasfiye_kat=2.0):
    """islemler: tüm coinler, giriş zamanına göre işlenir; özsermaye çıkışta güncellenir."""
    olaylar = []
    for x in islemler:
        olaylar.append((x["giris_zaman"], 1, x))
    olaylar.sort(key=lambda o: (o[0], o[1]))
    bakiye = baslangic
    acik = []          # (çıkış zamanı, pnl)
    gerekli_kaldirac, atlanan = [], {"kaldirac_siniri": 0, "tasfiye_yakin": 0, "bakiye_yok": 0}
    egri_t, egri = [], []
    komisyonlar, sonuclar = [], []
    for zaman, _, x in olaylar:
        # bu girişten önce kapanan pozisyonları bakiyeye işle
        acik.sort()
        while acik and acik[0][0] <= zaman:
            ct, pnl = acik.pop(0)
            bakiye += pnl
            egri_t.append(ct)
            egri.append(bakiye)
        if bakiye <= 0:
            atlanan["bakiye_yok"] += 1
            continue
        d = abs(x["giris"] - x["stop"])
        adet = risk / d
        notional = adet * x["giris"]
        azami_teminat = teminat_orani * bakiye
        kald = max(1, math.ceil(notional / azami_teminat))
        gerekli_kaldirac.append(kald)
        b = _bracket(bracket[x["sembol"]], notional)
        if kald > b["initialLeverage"]:
            atlanan["kaldirac_siniri"] += 1
            continue
        teminat = notional / kald
        lp = tasfiye_fiyati(x["yon"], adet, x["giris"], teminat, b["maintMarginRatio"], b["cum"])
        if abs(x["giris"] - lp) < tasfiye_kat * d:
            atlanan["tasfiye_yakin"] += 1
            continue
        pnl = x["net_R"] * risk
        komisyonlar.append(x["komisyon_usdt"])
        sonuclar.append(x["net_R"])
        acik.append((x["cikis_zaman"], pnl))
    for ct, pnl in sorted(acik):
        bakiye += pnl
        egri_t.append(ct)
        egri.append(bakiye)
    egri = np.array(egri) if egri else np.array([baslangic])
    tepe = np.maximum.accumulate(np.r_[baslangic, egri])[1:]
    dd = float(((tepe - egri) / tepe).max() * 100) if len(egri) else 0.0
    seri = en_uzun = 0
    for r in sonuclar:          # giriş sırasına göre
        seri = seri + 1 if r < 0 else 0
        en_uzun = max(en_uzun, seri)
    gk = np.array(gerekli_kaldirac) if gerekli_kaldirac else np.array([0])
    return {
        "acilan": len(sonuclar), "atlanan": atlanan,
        "kaldirac_medyan": float(np.median(gk)), "kaldirac_p90": float(np.percentile(gk, 90)),
        "kaldirac_maks": int(gk.max()),
        "ort_komisyon_usdt": float(np.mean(komisyonlar)) if komisyonlar else 0.0,
        "son_bakiye": float(egri[-1]), "maks_dusus_yuzde": dd, "en_uzun_zarar_serisi": en_uzun,
        "egri_t": egri_t, "egri": egri.tolist(),
    }
