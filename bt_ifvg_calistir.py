"""
ifvg_5m backtest çalıştırıcısı (sunucuda, arka planda; bt_tetik.py başlatır).

Kullanım:  python bt_ifvg_calistir.py grafik|gelistirme|dogrulama

  grafik      : geliştirme döneminden rastgele 20 kurulum grafiği (sonuç YOK)
  gelistirme  : V1 ve V2 geliştirme sonuçları + karar kuralları
  dogrulama   : sadece geliştirmede geçen tek varyant, bir kez
Çıktılar: /opt/sinyalbot/bt_calisma/ifvg_5m/
"""
import csv
import hashlib
import json
import os
import sys
import time
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent
KOK = REPO.parent
BT = KOK / "bt_calisma"
CIKTI = BT / "ifvg_5m"
os.environ.setdefault("MPLCONFIGDIR", str(CIKTI / ".matplotlib"))
sys.path.insert(0, str(REPO))

import numpy as np  # noqa: E402

from motor import grafik, istatistik, pozisyon, telegram, veri  # noqa: E402
from motor.tek_islem import MALIYET, calistir  # noqa: E402
from strateji import ifvg_5m as S  # noqa: E402

SEMBOLLER = ["BTCUSDT", "ETHUSDT"]
GEL = ("2021-01-01", "2024-06-30")
DOG = ("2024-07-01", "2026-10-31")
TOHUM = 20261118


def env_yukle():
    for yol in (REPO / "ayarlar.env", KOK / "secrets.env"):
        if not yol.exists():
            continue
        for satir in yol.read_text(encoding="utf-8").splitlines():
            satir = satir.strip()
            if not satir or satir.startswith("#") or "=" not in satir:
                continue
            a, d = satir.split("=", 1)
            a = a.strip().removeprefix("export ").strip()
            os.environ.setdefault(a, d.strip().strip('"').strip("'"))


def ms(tarih, gun_sonu=False):
    t = int(np.datetime64(tarih, "ms").astype(np.int64))
    return t + 86_400_000 - 1 if gun_sonu else t


LOG = None


def log(*a):
    satir = time.strftime("%H:%M:%S ") + " ".join(str(x) for x in a)
    print(satir, flush=True)
    if LOG:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(satir + "\n")


def durum_yaz(**kw):
    yol = CIKTI / "durum.json"
    d = json.loads(yol.read_text()) if yol.exists() else {}
    d.update(kw)
    yol.write_text(json.dumps(d, ensure_ascii=False, indent=1))


def kural_ozeti():
    h = hashlib.sha256()
    for y in (REPO / "strateji" / "ifvg_5m.py", REPO / "motor" / "tek_islem.py"):
        h.update(y.read_bytes())
    return h.hexdigest()[:16]


# ---------------------------------------------------------------------------
def veri_hazirla():
    hazir, raporlar = {}, []
    for s in SEMBOLLER:
        log(f"{s} verisi hazırlanıyor")
        (t, o, h, l, c), rap = veri.yukle_5m(s, BT / "veri", CIKTI / "veri", log=log)
        hazir[s] = {"t": t, "o": o, "h": h, "l": l, "c": c}
        raporlar.append(rap)
    return hazir, raporlar


def donem_verisi(V, bit_ms):
    m = V["t"] <= bit_ms
    V = {k: v[m] for k, v in V.items()}
    gt, go, gh, gl, gc = veri.birlestir(V["t"], V["o"], V["h"], V["l"], V["c"], 86_400_000)
    bias, son_sh, son_sl, shp, slp = S.gunluk_bias(gh, gl, gc, S.PARAM["gunluk_swing_n"])
    G = {"t": gt, "o": go, "h": gh, "l": gl, "c": gc, "bias": bias, "son_sh": son_sh, "son_sl": son_sl,
         "sh_piv": shp, "sl_piv": slp}
    gun_bias = {int(g // 86_400_000): int(b) for g, b in zip(gt, bias)}
    return V, G, gun_bias


def kos(hazir, varyantlar, bas_ms, bit_ms):
    sonuc = {v: {"islemler": [], "dusen": {}} for v in varyantlar}
    dv = {}
    for s in SEMBOLLER:
        V, G, gun_bias = donem_verisi(hazir[s], bit_ms)
        dv[s] = (V, G)
        L = [V[k].tolist() for k in "tohlc"]
        for v in varyantlar:
            t0 = time.time()
            isl, dus = calistir(s, *L, gun_bias, v, bas_ms, bit_ms)
            log(f"{s} {v}: {len(isl)} işlem ({time.time() - t0:.0f} sn)")
            sonuc[v]["islemler"] += isl
            for a, b in dus.items():
                sonuc[v]["dusen"][a] = sonuc[v]["dusen"].get(a, 0) + b
        del L
    for v in varyantlar:
        sonuc[v]["islemler"].sort(key=lambda x: x["giris_zaman"])
    return sonuc, dv


def veri_mesaji(raporlar):
    satirlar = ["Veri:"]
    for r in raporlar:
        satirlar.append(f"• {r['sembol']}: " + "; ".join(r["kaynak"]))
        if r["indirilen_ay"]:
            satirlar.append(f"  indirilen: {', '.join(r['indirilen_ay'])}")
        if r["eksik_ay"]:
            satirlar.append(f"  ⚠️ eksik kalan aylar: {', '.join(r['eksik_ay'])}")
        if "eksik_5m" in r:
            satirlar.append(f"  5m mum: {r['toplam_5m']:,} (boşluk: {r['eksik_5m']:,} mum)")
    return "\n".join(satirlar)


# ---------------------------------------------------------------------------
def mod_grafik():
    hazir, raporlar = veri_hazirla()
    bas, bit = ms(GEL[0]), ms(GEL[1], True)
    sonuc, dv = kos(hazir, ["V1", "V2"], bas, bit)
    rng = np.random.default_rng(TOHUM)
    havuz = {
        "A": [x for x in sonuc["V1"]["islemler"] if x["senaryo"] == "A"],
        "B-piyasa": [x for x in sonuc["V1"]["islemler"] if x["tip"] == "B-piyasa"],
        "B-limit": [x for x in sonuc["V2"]["islemler"] if x["tip"] == "B-limit"],
    }
    hedef_adet = {"A": 7, "B-piyasa": 7, "B-limit": 6}
    secilen = []
    for ad, n in hedef_adet.items():
        h = havuz[ad]
        if h:
            secilen += [h[i] for i in rng.choice(len(h), size=min(n, len(h)), replace=False)]
    secilen.sort(key=lambda x: x["giris_zaman"])
    gdir = CIKTI / f"grafik_{time.strftime('%Y%m%d_%H%M')}"
    gdir.mkdir(parents=True, exist_ok=True)
    yollar = []
    for j, x in enumerate(secilen, 1):
        V, G = dv[x["sembol"]]
        yol = gdir / f"{j:02d}_{x['sembol']}_{x['varyant']}_{x['tip']}.png"
        grafik.kurulum_grafigi(yol, V, G, x, None)
        yollar.append(str(yol))
    # kurulum ayrıntıları (sonuç sütunları yok)
    alanlar = ["sembol", "varyant", "tip", "yon", "bias", "m1", "m3", "inv", "geri_cekilme", "kirilim", "tetik", "e",
               "giris_tipi", "giris", "stop", "hedef", "hedef_tip", "hedef_R", "stop_yuzde", "cikis_tipi"]
    with open(gdir / "kurulumlar.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(alanlar + ["giris_utc", "inversion_utc"])
        for x in secilen:
            V = dv[x["sembol"]][0]
            w.writerow([x[a] for a in alanlar] + [veri._tarih(x["giris_zaman"]), veri._tarih(V["t"][x["inv"]])])
    satir = [f"ifvg_5m v1 — KURULUM GRAFİKLERİ (geliştirme dönemi, rastgele {len(secilen)} kurulum)",
             "Sonuçlar gösterilmiyor; önce kuralların doğru kodlandığını onayla.",
             f"Seçim: A {sum(x['senaryo'] == 'A' for x in secilen)}, B-piyasa "
             f"{sum(x['tip'] == 'B-piyasa' for x in secilen)}, B-limit {sum(x['tip'] == 'B-limit' for x in secilen)} "
             f"(her yol kontrol edilsin diye; tohum {TOHUM})", "", veri_mesaji(raporlar), "",
             "Grafikler: turuncu = FVG bölgesi, sarı şerit = m1–m3, mor = inversion/A tetik seviyesi, "
             "mavi = geri çekilme/kırılım, yeşil = hedef, kırmızı = stop, X = çıkış. Sağ panel: günlük bias."]
    telegram.mesaj("\n".join(satir))
    telegram.resimler(yollar, "ifvg_5m kurulumlar")
    telegram.dosya(gdir / "kurulumlar.csv", "Kurulum ayrıntıları (sonuç sütunu yok)")


# ---------------------------------------------------------------------------
def analiz(sonuc_v, dv, bas, bit, rng):
    isl = sonuc_v["islemler"]
    R = np.array([x["net_R"] for x in isl])
    gz = np.array([x["giris_zaman"] for x in isl], dtype=np.int64)
    cz = np.array([x["cikis_zaman"] for x in isl], dtype=np.int64)
    oz = istatistik.ozet(R, gz, cz, bas, bit, rng)
    # rastgele yön + sürekli long + motor/vektör tutarlılık
    RL, RS, tut = [], [], []
    rz_toplam = np.zeros(200)
    for s in SEMBOLLER:
        xs = [x for x in isl if x["sembol"] == s]
        if not xs:
            continue
        V = dv[s][0]
        e, yon, giris, d, hR, lim = istatistik.islem_dizileri(xs)
        rl = istatistik.sim_toplu(V, e, np.ones_like(yon), giris, d, hR, lim)
        rs = istatistik.sim_toplu(V, e, -np.ones_like(yon), giris, d, hR, lim)
        RL.append(rl)
        RS.append(rs)
        gercek = np.where(yon > 0, rl, rs)
        tut.append(np.abs(gercek - np.array([x["net_R"] for x in xs])).max())
        rz_toplam += istatistik.rastgele_zaman_toplam(V, xs, rng)
    RL = np.concatenate(RL) if RL else np.array([])
    RS = np.concatenate(RS) if RS else np.array([])
    # RL/RS sembol sırasına göre; R'yi de aynı sıraya diz
    R_sirali = np.concatenate([[x["net_R"] for x in isl if x["sembol"] == s] for s in SEMBOLLER]) if isl else R
    ry = istatistik.rastgele_yon(RL, RS, R_sirali, rng, 1000)
    rz = istatistik.t_degeri(R, rz_toplam / max(len(isl), 1)) if len(isl) > 1 else {"t": float("nan")}
    long_oz = {"ort_R": float(RL.mean()) if len(RL) else float("nan"),
               "pf": float(RL[RL > 0].sum() / -RL[RL < 0].sum()) if (RL < 0).any() else float("nan")}
    kk, gecti = istatistik.karar(oz, ry.get("t", float("nan")))
    return {"oz": oz, "rastgele_yon": ry, "rastgele_zaman": rz, "surekli_long": long_oz,
            "karar": kk, "gecti": gecti, "tutarlilik": float(max(tut) if tut else 0.0),
            "ort_maliyet_R": float(np.mean([x["maliyet_R"] for x in isl])) if isl else 0.0,
            "ort_maliyet_usdt": float(np.mean([x["maliyet_usdt"] for x in isl])) if isl else 0.0,
            "ort_stop_yuzde": float(np.mean([x["stop_yuzde"] for x in isl])) if isl else 0.0,
            "senaryo": {a: sum(x["senaryo"] == a for x in isl) for a in "AB"},
            "cikis": {a: sum(x["cikis_tipi"] == a for x in isl) for a in ("stop", "hedef", "sure", "veri_sonu")},
            "dusen": sonuc_v["dusen"]}


def _f(x, b=2):
    return "—" if x is None or (isinstance(x, float) and not np.isfinite(x)) else f"{x:.{b}f}"


def rapor_metni(v, a, poz, donem_ad):
    oz = a["oz"]
    t = [f"=== {v} ({S.VARYANTLAR[v]}) — {donem_ad} ===",
         f"İşlem: {oz['n']} (Senaryo A {a['senaryo']['A']}, B {a['senaryo']['B']})"]
    if oz["n"] == 0:
        return "\n".join(t + ["İşlem yok."])
    ok = lambda b: "✅" if b else "❌"  # noqa: E731
    kk = a["karar"]
    t += [f"Kazanma oranı: %{oz['kazanma'] * 100:.1f}",
          f"Ortalama: {oz['ort_R']:+.3f}R {ok(kk['ort_R'])} (≥ +0,10)",
          f"Bootstrap %95 GA: [{oz['bs_alt']:+.3f}, {oz['bs_ust']:+.3f}] {ok(kk['bs_alt'])} (alt > 0)",
          f"PF: {_f(oz['pf'])} {ok(kk['pf'])} (≥ 1,2)",
          f"Kârlı çeyrek: %{oz['karli_ceyrek'] * 100:.0f} {ok(kk['karli_ceyrek'])} (≥ %60)",
          f"Rastgele yön t: {_f(a['rastgele_yon']['t'])} {ok(kk['t_rastgele'])} (≥ 2) "
          f"[rastgele ort {_f(a['rastgele_yon'].get('rastgele_ort'), 3)}R]",
          f"SONUÇ: {'GEÇTİ' if a['gecti'] else 'GEÇMEDİ'}",
          "",
          f"Sürekli long: ort {_f(a['surekli_long']['ort_R'], 3)}R, PF {_f(a['surekli_long']['pf'])}",
          f"Rastgele zaman (bias yönünde): ort {_f(a['rastgele_zaman'].get('rastgele_ort'), 3)}R, "
          f"t {_f(a['rastgele_zaman']['t'])}, gerçek yüzdelik %{_f(a['rastgele_zaman'].get('yuzdelik'), 0)}",
          f"Toplam: {oz['toplam_R']:+.1f}R   Maks. düşüş: {oz['maks_dusus_R']:.1f}R",
          f"Ort. stop mesafesi: %{a['ort_stop_yuzde']:.3f}",
          f"Ort. maliyet: {a['ort_maliyet_R']:.3f}R = {a['ort_maliyet_usdt']:.1f} USDT/işlem",
          f"Çıkışlar: stop {a['cikis']['stop']}, hedef {a['cikis']['hedef']}, süre {a['cikis']['sure']}",
          "Düşen/atlanan: " + ", ".join(f"{k} {v_}" for k, v_ in sorted(a["dusen"].items())),
          "", "Çeyrekler (işlem / toplam R / ort R):"]
    for c, d in oz["ceyrekler"].items():
        t.append(f"  {c}: {d['n']:4d}  {d['toplam_R']:+7.1f}  {d['ort_R']:+.3f}")
    if poz:
        t += ["", "Pozisyon boyutu kontrolü (300 USDT risk, 5.000 başlangıç; karar kurallarını etkilemez):",
              f"  Kaldıraç medyan {poz['kaldirac_medyan']:.0f}x, %90 {poz['kaldirac_p90']:.0f}x, en yüksek {poz['kaldirac_maks']}x",
              f"  Açılan {poz['acilan']}, atlanan: kaldıraç sınırı {poz['atlanan']['kaldirac_siniri']}, "
              f"tasfiye yakın {poz['atlanan']['tasfiye_yakin']}, bakiye yok {poz['atlanan']['bakiye_yok']}",
              f"  Ort. komisyon {poz['ort_komisyon_usdt']:.1f} USDT, son bakiye {poz['son_bakiye']:,.0f} USDT",
              f"  Maks. düşüş %{poz['maks_dusus_yuzde']:.1f}, en uzun zarar serisi {poz['en_uzun_zarar_serisi']}"]
    t.append(f"(motor/vektör tutarlılık farkı: {a['tutarlilik']:.2e})")
    return "\n".join(t)


def islem_csv(yol, isl):
    if not isl:
        return
    alanlar = list(isl[0].keys())
    with open(yol, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=alanlar + ["giris_utc", "cikis_utc"])
        w.writeheader()
        for x in isl:
            w.writerow({**x, "giris_utc": veri._tarih(x["giris_zaman"]), "cikis_utc": veri._tarih(x["cikis_zaman"])})


def sonuc_isle(sonuc, dv, varyantlar, bas, bit, donem_ad, etiket):
    rng = np.random.default_rng(TOHUM)
    br, br_kaynak = pozisyon.bracket_al(CIKTI / "leverage_bracket.json", SEMBOLLER, log)
    rdir = CIKTI / f"{etiket}_{time.strftime('%Y%m%d_%H%M')}"
    rdir.mkdir(parents=True, exist_ok=True)
    metinler, analizler, dosyalar = [], {}, []
    for v in varyantlar:
        a = analiz(sonuc[v], dv, bas, bit, rng)
        poz = pozisyon.simule(sonuc[v]["islemler"], br["bracket"]) if br else None
        analizler[v] = a
        metinler.append(rapor_metni(v, a, poz, donem_ad))
        islem_csv(rdir / f"islemler_{v}.csv", sonuc[v]["islemler"])
        dosyalar.append(rdir / f"islemler_{v}.csv")
        if poz:
            grafik.ozsermaye_grafigi(rdir / f"ozsermaye_{v}.png", poz["egri_t"], poz["egri"],
                                     f"ifvg_5m {v} — {donem_ad} — özsermaye (USDT)")
            dosyalar.append(rdir / f"ozsermaye_{v}.png")
    bas_satir = [f"ifvg_5m v1 — {donem_ad} SONUÇLARI", f"Kural özeti (hash): {kural_ozeti()}",
                 f"Maliyet: taker %{MALIYET['taker'] * 100:.2f}+kayma %{MALIYET['kayma'] * 100:.2f}, "
                 f"maker %{MALIYET['maker'] * 100:.2f}, funding %{MALIYET['funding_8s'] * 100:.2f}/8s",
                 f"Leverage bracket: {br_kaynak}"]
    metin = "\n".join(bas_satir) + "\n\n" + "\n\n".join(metinler)
    (rdir / "rapor.txt").write_text(metin, encoding="utf-8")
    return metin, analizler, rdir, dosyalar


def mod_gelistirme():
    hazir, raporlar = veri_hazirla()
    bas, bit = ms(GEL[0]), ms(GEL[1], True)
    sonuc, dv = kos(hazir, ["V1", "V2"], bas, bit)
    metin, an, rdir, dosyalar = sonuc_isle(sonuc, dv, ["V1", "V2"], bas, bit, "geliştirme 2021-01 – 2024-06", "gelistirme")
    gecen = [v for v in ("V1", "V2") if an[v]["gecti"]]
    secilen = max(gecen, key=lambda v: an[v]["oz"]["ort_R"]) if gecen else None
    karar_metni = (f"\n\nSEÇİM: {secilen} doğrulamaya gider (geçenler: {', '.join(gecen)})." if secilen
                   else "\n\nSEÇİM: hiçbir varyant geçmedi → doğrulama AÇILMAZ.")
    gecmis = CIKTI / "gelistirme_gecmis.jsonl"
    onceki = [json.loads(s) for s in gecmis.read_text().splitlines()] if gecmis.exists() else []
    if onceki and onceki[-1]["hash"] != kural_ozeti():
        karar_metni += "\n⚠️ Kural dosyaları önceki geliştirme çalıştırmasından farklı (hash değişti)."
    kayit = {"zaman": time.strftime("%Y-%m-%d %H:%M"), "hash": kural_ozeti(), "secilen": secilen,
             "gecti": {v: an[v]["gecti"] for v in an}, "ort_R": {v: an[v]["oz"].get("ort_R") for v in an}}
    with open(gecmis, "a", encoding="utf-8") as f:
        f.write(json.dumps(kayit, ensure_ascii=False) + "\n")
    (CIKTI / "gelistirme_sonuc.json").write_text(json.dumps(kayit, ensure_ascii=False, indent=1))
    metin += karar_metni
    (rdir / "rapor.txt").write_text(metin, encoding="utf-8")
    telegram.mesaj(veri_mesaji(raporlar))
    telegram.mesaj(metin)
    telegram.dosya(rdir / "rapor.txt", "Geliştirme raporu")
    for d in dosyalar:
        telegram.dosya(d)


def mod_dogrulama():
    sonuc_yol, kilit = CIKTI / "gelistirme_sonuc.json", CIKTI / "dogrulama_kilit.json"
    if kilit.exists():
        telegram.mesaj("ifvg_5m: doğrulama daha önce açıldı; ikinci kez açılmaz. " + kilit.read_text())
        return
    if not sonuc_yol.exists():
        telegram.mesaj("ifvg_5m: önce geliştirme çalıştırılmalı. Doğrulama açılmadı.")
        return
    gs = json.loads(sonuc_yol.read_text())
    if not gs.get("secilen"):
        telegram.mesaj("ifvg_5m: geliştirmede hiçbir varyant geçmedi. Doğrulama açılmaz.")
        return
    if gs["hash"] != kural_ozeti():
        telegram.mesaj("ifvg_5m: kural dosyaları geliştirme çalıştırmasından sonra değişmiş. Doğrulama açılmadı.")
        return
    v = gs["secilen"]
    hazir, raporlar = veri_hazirla()
    bas, bit = ms(DOG[0]), ms(DOG[1], True)
    sonuc, dv = kos(hazir, [v], bas, bit)
    metin, an, rdir, dosyalar = sonuc_isle(sonuc, dv, [v], bas, bit, "doğrulama 2024-07 – 2026-10", "dogrulama")
    telegram.mesaj(veri_mesaji(raporlar))
    telegram.mesaj(metin)
    telegram.dosya(rdir / "rapor.txt", "Doğrulama raporu")
    for d in dosyalar:
        telegram.dosya(d)
    kilit.write_text(json.dumps({"zaman": time.strftime("%Y-%m-%d %H:%M"), "varyant": v,
                                 "gecti": an[v]["gecti"]}, ensure_ascii=False))


def main():
    global LOG
    mod = sys.argv[1] if len(sys.argv) > 1 else ""
    CIKTI.mkdir(parents=True, exist_ok=True)
    LOG = CIKTI / f"log_{mod}_{time.strftime('%Y%m%d_%H%M')}.txt"
    env_yukle()
    durum_yaz(mod=mod, pid=os.getpid(), durum="calisiyor", basla=time.strftime("%Y-%m-%d %H:%M:%S"))
    t0 = time.time()
    try:
        {"grafik": mod_grafik, "gelistirme": mod_gelistirme, "dogrulama": mod_dogrulama}[mod]()
        durum_yaz(durum="bitti", bitis=time.strftime("%Y-%m-%d %H:%M:%S"))
        log(f"bitti ({(time.time() - t0) / 60:.1f} dk)")
    except Exception as ex:
        iz = traceback.format_exc()
        log(iz)
        durum_yaz(durum="hata", bitis=time.strftime("%Y-%m-%d %H:%M:%S"))
        telegram.mesaj(f"ifvg_5m {mod} HATA: {ex}\n\n{iz[-1500:]}")


if __name__ == "__main__":
    main()
