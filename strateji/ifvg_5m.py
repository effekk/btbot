"""
ifvg_5m (v1) — 5 dakikalık iFVG stratejisi, sadece günlük bias yönünde.

Bu dosya SADECE strateji kurallarını içerir. Emir dolumu, pozisyon takibi ve
maliyet motordadır (motor/tek_islem.py). Canlı bot da bu dosyayı kullanır.

Simetri: short tarafı, fiyatları ters çevrilmiş seride (o,h,l,c -> -o,-l,-h,-c)
long kurallarının AYNISI çalıştırılarak elde edilir. Bu yüzden aşağıdaki
yorumlar long (bullish bias) örneğine göre yazılmıştır; short tam simetriktir.

Long örneği (bearish FVG):
  m1.low > m3.high  -> bölge alt = m3.high, üst = m1.low
  inversion  = m3'ten sonraki 48 mum içinde bölge üstünün üzerinde kapanan ilk mum
  Senaryo A  = inversion mumunun açılışı <= bölge altı, değilse Senaryo B
  stop       = bölge altı

Onay bekleyen yorum: ilk tetik kurulumu tüketir; giriş filtresi (bias, stop, 1,5R,
günlük sınır, açık pozisyon) geçmezse kurulum düşer, sonraki mumlarda yeniden tetiklenmez.
"""
from bisect import bisect_left, bisect_right, insort

import numpy as np

AD = "ifvg_5m"
SURUM = "v1"

MUM_MS = 5 * 60 * 1000
GUN_MS = 24 * 60 * 60 * 1000
SAAT_MS = 60 * 60 * 1000

PARAM = {
    "fvg_omur": 48,             # inversion, m3'ten sonraki 48 mumda aranır
    "giris_omur": 48,           # giriş, inversion mumundan sonraki 48 mumda olmalı
    "pozisyon_omur": 48,        # pozisyon en fazla 48 mum açık kalır
    "min_rr": 1.5,              # en yakın hedef < 1,5R ise işlem yok
    "min_stop_orani": 0.002,    # stop mesafesi < girişin %0,20'si ise işlem yok
    "gunluk_max_islem": 4,      # coin başına, UTC günü
    "swing_n": 2,               # 5m fraktal: iki yanında 2'şer mum
    "gunluk_swing_n": 2,        # günlük swing: iki yanında 2'şer gün
    "asya_bitis_saat": 8,       # 00:00-08:00 UTC seansı
    # --- strateji sohbetinin onayını bekleyen yorumlar (bkz. mesaj) ---
    "b_ayni_mum_geri_cekilme": False,  # False: kırılım kapanışı geri çekilme mumundan SONRAKİ bir mumda olmalı
}

VARYANTLAR = {"V1": "Senaryo A + B-piyasa", "V2": "Senaryo A + B-limit"}


# ---------------------------------------------------------------------------
# Günlük bias
# ---------------------------------------------------------------------------
def gunluk_bias(gh, gl, gc, n=2):
    """Günlük high/low/close dizilerinden her gün için geçerli bias'ı üretir.

    bias[d] = d günü boyunca geçerli bias (+1 bullish, -1 bearish, 0 yok);
    sadece d-1 gününün kapanışına kadarki bilgiyle hesaplanır.
    Swing high: iki yanındaki n'er günden kesin yüksek; pivot+n günü kapanınca kesinleşir.
    Dönüş: bias, son_sh (d günü için geçerli son swing high), son_sl, sh_pivotlar, sl_pivotlar
    """
    D = len(gc)
    bias = np.zeros(D, dtype=np.int8)
    son_sh = np.full(D, np.nan)
    son_sl = np.full(D, np.nan)
    sh_piv, sl_piv = [], []
    durum, sh, sl = 0, None, None
    for k in range(D):
        p = k - n
        if p - n >= 0:
            if all(gh[p] > gh[p - j] and gh[p] > gh[p + j] for j in range(1, n + 1)):
                sh = gh[p]
                sh_piv.append(p)
            if all(gl[p] < gl[p - j] and gl[p] < gl[p + j] for j in range(1, n + 1)):
                sl = gl[p]
                sl_piv.append(p)
        if sh is not None and gc[k] > sh:
            durum = 1
        elif sl is not None and gc[k] < sl:
            durum = -1
        if k + 1 < D:
            bias[k + 1] = durum
            son_sh[k + 1] = np.nan if sh is None else sh
            son_sl[k + 1] = np.nan if sl is None else sl
    return bias, son_sh, son_sl, sh_piv, sl_piv


# ---------------------------------------------------------------------------
# Kurulum nesnesi (fiyatlar yön uzayında; gerçek fiyat = yon * değer)
# ---------------------------------------------------------------------------
class Kurulum:
    __slots__ = ("id", "yon", "tip", "senaryo", "m1", "m3", "k", "alt", "ust", "maks3",
                 "maks_sonra", "geri_cekilme_mum", "tetik_mum", "kirilim", "limit_aktif",
                 "hedef", "hedef_tip")

    def __init__(self, **kw):
        for a in self.__slots__:
            setattr(self, a, kw.get(a))

    # gerçek fiyatlar
    def fiyat(self, deger):
        return None if deger is None else self.yon * deger

    @property
    def stop(self):
        return self.yon * self.alt

    @property
    def giris_kenari(self):
        return self.yon * self.ust


class _FVG:
    __slots__ = ("m3", "alt", "ust", "maks3")

    def __init__(self, m3, alt, ust, maks3):
        self.m3, self.alt, self.ust, self.maks3 = m3, alt, ust, maks3


# ---------------------------------------------------------------------------
# Tek yön takipçisi (long mantığı; short için ters seri)
# ---------------------------------------------------------------------------
class _TekYon:
    def __init__(self, yon, T, O, H, L, C, varyant, p, sayac):
        self.yon = yon
        self.s = 1.0 if yon > 0 else -1.0
        self.T, self._O, self._C = T, O, C
        self._H = H if yon > 0 else L     # yön uzayında high
        self._L = L if yon > 0 else H     # yön uzayında low
        self.varyant = varyant
        self.p = p
        self.sayac = sayac                # ortak id sayacı (liste)
        self.fvgler = []
        self.kurulumlar = []
        self.yeni_limitler = []
        self.lik = []                     # alınmamış kesinleşmiş swing high'lar (sıralı)
        self.swingler = []                # (pivot mum, seviye) — kırılım seviyesi için
        self.gun_h = None
        self.ogh = None                   # önceki gün high
        self.ogh_alindi = False
        self.asya_h = None
        self.asya_bitti = False
        self.asya_alindi = False

    def h(self, i):
        return self.s * self._H[i]

    def l(self, i):
        return self.s * self._L[i]

    # ---- hedef -----------------------------------------------------------
    def hedef_bul(self, giris):
        """Girişin üstündeki en yakın alınmamış likidite. (seviye, tip) ya da None."""
        adaylar = []
        j = bisect_right(self.lik, giris)
        if j < len(self.lik):
            adaylar.append((self.lik[j], "swing"))
        if self.ogh is not None and not self.ogh_alindi and self.ogh > giris:
            adaylar.append((self.ogh, "onceki_gun"))
        if self.asya_bitti and self.asya_h is not None and not self.asya_alindi and self.asya_h > giris:
            adaylar.append((self.asya_h, "asya_00_08"))
        if not adaylar:
            return None
        return min(adaylar)

    def gun_bitti(self):
        self.ogh = self.gun_h
        self.ogh_alindi = False
        self.gun_h = None
        self.asya_h = None
        self.asya_bitti = False
        self.asya_alindi = False

    def _kirilim(self, k, i):
        """B-piyasa kırılım seviyesi: i mumundan önce kesinleşmiş, pivotu inversion'dan
        sonra olan son swing high; yoksa inversion mumu dahil i'den önceki en yüksek high."""
        n = self.p["swing_n"]
        for q, sev in reversed(self.swingler):
            if q <= k.k:
                break
            if q + n <= i - 1:
                return sev, "swing"
        return k.maks_sonra, "en_yuksek"

    # ---- ana adım --------------------------------------------------------
    def mum_kapandi(self, i, dusen):
        s, p = self.s, self.p
        o = s * self._O[i]
        h = s * self._H[i]
        l = s * self._L[i]
        c = s * self._C[i]
        tetikler = []

        # 1) likidite durumları (bu mum dahil)
        lik = self.lik
        if lik and lik[0] < h:
            del lik[:bisect_left(lik, h)]
        if self.ogh is not None and h > self.ogh:
            self.ogh_alindi = True
        saat = (self.T[i] % GUN_MS) // SAAT_MS
        if saat < p["asya_bitis_saat"]:
            self.asya_h = h if self.asya_h is None else max(self.asya_h, h)
            if (self.T[i] + MUM_MS) % GUN_MS >= p["asya_bitis_saat"] * SAAT_MS:
                self.asya_bitti = True
        else:
            if self.asya_h is not None:
                if self.asya_bitti and h > self.asya_h:
                    self.asya_alindi = True
                self.asya_bitti = True
        self.gun_h = h if self.gun_h is None else max(self.gun_h, h)

        # 2) 5m swing high kesinleşmesi (pivot = i - n)
        n = p["swing_n"]
        q = i - n
        if q - n >= 0:
            hq = self.h(q)
            if all(hq > self.h(q - j) and hq > self.h(q + j) for j in range(1, n + 1)):
                insort(lik, hq)
                self.swingler.append((q, hq))
                if len(self.swingler) > 400:
                    del self.swingler[:200]

        # 3) mevcut kurulumlar
        omur = p["giris_omur"]
        kalan = []
        for k in self.kurulumlar:
            if k.tip == "A":
                if c > k.maks3:
                    k.tetik_mum = i
                    tetikler.append(k)
                elif c < k.alt:
                    dusen["iptal_bolge_alti"] += 1
                elif i >= k.k + omur - 1:
                    dusen["sure_doldu"] += 1
                else:
                    kalan.append(k)
            elif k.tip == "B-piyasa":
                if p["b_ayni_mum_geri_cekilme"] and k.geri_cekilme_mum is None and l <= k.ust:
                    k.geri_cekilme_mum = i
                sev, _ = self._kirilim(k, i)
                if k.geri_cekilme_mum is not None and c > sev:
                    k.tetik_mum = i
                    k.kirilim = sev
                    tetikler.append(k)
                elif c < k.alt:
                    dusen["iptal_bolge_alti"] += 1
                else:
                    if k.geri_cekilme_mum is None and l <= k.ust:
                        k.geri_cekilme_mum = i
                    k.maks_sonra = max(k.maks_sonra, h)
                    if i >= k.k + omur - 1:
                        dusen["sure_doldu"] += 1
                    else:
                        kalan.append(k)
            else:  # B-limit (dolum motorda)
                if c < k.alt:
                    dusen["iptal_bolge_alti"] += 1
                elif i >= k.k + omur:
                    dusen["sure_doldu"] += 1
                else:
                    kalan.append(k)
        self.kurulumlar = kalan

        # 4) inversion
        kalan_f = []
        for f in self.fvgler:
            if c > f.ust:
                self.sayac[0] += 1
                senaryo = "A" if o <= f.alt else "B"
                tip = "A" if senaryo == "A" else ("B-piyasa" if self.varyant == "V1" else "B-limit")
                k = Kurulum(id=self.sayac[0], yon=self.yon, tip=tip, senaryo=senaryo,
                            m1=f.m3 - 2, m3=f.m3, k=i, alt=f.alt, ust=f.ust, maks3=f.maks3,
                            maks_sonra=h, limit_aktif=False)
                dusen["kurulum_" + senaryo] += 1
                if tip == "A" and c > k.maks3:
                    k.tetik_mum = i
                    tetikler.append(k)
                else:
                    self.kurulumlar.append(k)
                    if tip == "B-limit":
                        self.yeni_limitler.append(k)
            elif i - f.m3 >= p["fvg_omur"]:
                dusen["fvg_inversionsuz_doldu"] += 1
            else:
                kalan_f.append(f)
        self.fvgler = kalan_f

        # 5) yeni FVG (m3 = i)
        if i >= 2:
            l1 = self.l(i - 2)
            if l1 > h:
                self.fvgler.append(_FVG(i, h, l1, max(self.h(i - 2), self.h(i - 1), h)))
                dusen["fvg"] += 1
        return tetikler


# ---------------------------------------------------------------------------
# Strateji (iki yön + bias + filtreler)
# ---------------------------------------------------------------------------
class IfvgStrateji:
    """T: ms açılış zamanları, O/H/L/C: listeler. gun_bias: {gün_no: bias} (gün_no = ms // GUN_MS)."""

    def __init__(self, T, O, H, L, C, gun_bias, varyant, param=None):
        assert varyant in VARYANTLAR
        self.p = dict(PARAM, **(param or {}))
        self.T, self.O, self.H, self.L, self.C = T, O, H, L, C
        self.gun_bias = gun_bias
        self.varyant = varyant
        self.dusen = _Sayac()
        sayac = [0]
        self.yonler = {1: _TekYon(1, T, O, H, L, C, varyant, self.p, sayac),
                       -1: _TekYon(-1, T, O, H, L, C, varyant, self.p, sayac)}
        self.gun = None
        self.aktif_bias = None

    def bias(self, zaman_ms):
        return int(self.gun_bias.get(zaman_ms // GUN_MS, 0))

    def _gun_degis(self, yeni_gun, i):
        for ty in self.yonler.values():
            ty.gun_bitti()
        yeni = int(self.gun_bias.get(yeni_gun, 0))
        if self.aktif_bias is not None and yeni != self.aktif_bias:
            # bias değişti: bekleyen kurulumlar ve limitler iptal (bu mumda oluşanlar yeni güne ait)
            for ty in self.yonler.values():
                kalan = []
                for k in ty.kurulumlar:
                    if k.k < i:
                        self.dusen["bias_degisti"] += 1
                    else:
                        kalan.append(k)
                ty.kurulumlar = kalan
        self.aktif_bias = yeni
        self.gun = yeni_gun

    def mum_kapandi(self, i):
        """i mumu kapandı. Dönüş: piyasa tetikleri (giriş i+1 açılışında değerlendirilir)."""
        t = self.T[i]
        g = t // GUN_MS
        if self.gun is None:
            self.gun = g
            self.aktif_bias = self.bias(t)
        elif g != self.gun:          # veri boşluğu nedeniyle 23:55 mumu yoksa
            self._gun_degis(g, i)
        tetikler = self.yonler[1].mum_kapandi(i, self.dusen) + self.yonler[-1].mum_kapandi(i, self.dusen)
        if (t + MUM_MS) % GUN_MS == 0:
            self._gun_degis(g + 1, i)       # bu mumda (i) oluşan kurulumlar yeni güne aittir, korunur
        # B-limit yerleştirme kontrolleri (emir i+1'den itibaren geçerli)
        for ty in self.yonler.values():
            for k in ty.yeni_limitler:
                if k not in ty.kurulumlar:
                    continue
                sebep = self._limit_kontrol(ty, k, t + MUM_MS)
                if sebep:
                    ty.kurulumlar.remove(k)
                    self.dusen[sebep] += 1
                else:
                    k.limit_aktif = True
            ty.yeni_limitler = []
        return tetikler

    def _limit_kontrol(self, ty, k, emir_zamani):
        if self.bias(emir_zamani) != k.yon:
            return "bias_uyumsuz"
        giris, stop = k.ust, k.alt
        d = giris - stop
        if d / abs(giris) < self.p["min_stop_orani"]:
            return "stop_dar"
        hb = ty.hedef_bul(giris)
        if hb is None or hb[0] - giris < self.p["min_rr"] * d:
            return "hedef_1_5R_alti"
        k.hedef, k.hedef_tip = hb
        return None

    def giris_degerlendir(self, k, giris_fiyati, zaman_ms):
        """Piyasa girişi (kaymasız açılış fiyatı). Dönüş: (None, bilgi) ya da (sebep, None)."""
        if self.bias(zaman_ms) != k.yon:
            return "bias_uyumsuz", None
        ty = self.yonler[k.yon]
        giris = k.yon * giris_fiyati
        stop = k.alt
        if giris <= stop:
            return "giris_stopun_otesinde", None
        d = giris - stop
        if d / abs(giris) < self.p["min_stop_orani"]:
            return "stop_dar", None
        hb = ty.hedef_bul(giris)
        if hb is None or hb[0] - giris < self.p["min_rr"] * d:
            return "hedef_1_5R_alti", None
        k.hedef, k.hedef_tip = hb
        return None, {"giris": giris_fiyati, "stop": k.yon * stop, "hedef": k.yon * hb[0], "hedef_tip": hb[1]}

    def limit_dolumlari(self, i):
        """i mumunda dolan aktif limitler, dolum sırasına göre (long: yüksek limit önce)."""
        dolan = []
        for yon, ty in self.yonler.items():
            l = ty.l(i)
            for k in ty.kurulumlar:
                if k.tip == "B-limit" and k.limit_aktif and l < k.ust:
                    dolan.append(k)
        dolan.sort(key=lambda k: -k.ust)
        return dolan

    def kurulum_kaldir(self, k):
        ty = self.yonler[k.yon]
        if k in ty.kurulumlar:
            ty.kurulumlar.remove(k)


class _Sayac(dict):
    def __missing__(self, anahtar):
        return 0
