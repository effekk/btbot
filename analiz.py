"""
Kural karşılaştırması: botun GERÇEK işlemleri üzerinde farklı çıkış kurallarını dener.

Her kapanmış işlem için (islemler.jsonl) açılıştan sonraki 1 dakikalık mumlar Binance'ten çekilir
ve her kural bu fiyat hareketi üzerinde dakika dakika uygulanır. Sonuçlar Telegram'a gönderilir.

Varsayımlar:
  - Aynı mumda hem SL'ye hem hedefe dokunulduysa önce SL vurulmuş sayılır (temkinli).
  - Emirler tam tetik fiyatından dolar (kayma yok); komisyon giriş+çıkış %0,05+%0,05.
  - Funding hesaba katılmaz.

Sunucuda çalıştırmak için:  sinyalbot-analiz
"""
import datetime
import json
import os
import time
import urllib.parse
import urllib.request

import binance_trader as bt
import rapor

TAKER_FEE = 0.0005
MAX_HORIZON_H = 72          # "süre sınırı yok" kuralında bir işlem en fazla bu kadar saat izlenir
H = 3600

# Her kural: zaman dilimine göre süre sınırı (saniye, None = yok), yarı kârdan sonra çarpan,
# ve +0,5R'de kapatılacak oran (SL'ye dokunmadan).
RULES = [
    ("Şu anki (15m 4s · 1h 24s)", {"15m": 4 * H, "1h": 24 * H}, 2, 0.0),
    ("15m 6 saat", {"15m": 6 * H, "1h": 24 * H}, 2, 0.0),
    ("15m 8 saat", {"15m": 8 * H, "1h": 24 * H}, 2, 0.0),
    ("Süre sınırı yok", {"15m": None, "1h": None}, 0, 0.0),
    ("Şu anki + %25 kâr @+0,5R", {"15m": 4 * H, "1h": 24 * H}, 2, 0.25),
    ("Süre yok + %25 kâr @+0,5R", {"15m": None, "1h": None}, 0, 0.25),
]


def klines(symbol, start_ms, end_ms):
    """1 dakikalık mumlar: [(açılış_ms, high, low, close), ...]"""
    out, cur = [], int(start_ms)
    while cur < end_ms:
        rows = bt._request("GET", "/fapi/v1/klines",
                           {"symbol": symbol, "interval": "1m", "startTime": cur,
                            "endTime": int(end_ms), "limit": 1500}, signed=False)
        if not rows:
            break
        out += [(int(r[0]), float(r[2]), float(r[3]), float(r[4])) for r in rows]
        cur = int(rows[-1][0]) + 60_000
        if len(rows) < 1500:
            break
        time.sleep(0.2)
    return out


def simulate(t, candles, limits, after_mult, early_frac):
    """
    Tek bir işlemi kurala göre oynatır. Dönüş: (R, çıkış türü, en yüksek kâr R'si)
    R = 1 → SL mesafesi kadar kâr (≈ +300 USDT).
    """
    long = t["side"] == "AL"
    sgn = 1 if long else -1
    entry, sl0, tp = t["entry"], t["sl"], t["tp"]
    risk = abs(entry - sl0)
    if risk <= 0:
        return None
    mid = entry + (tp - entry) * bt.PARTIAL_AT
    early = entry + sgn * 0.5 * risk
    opened_ms = int(datetime.datetime.fromisoformat(t["opened"]).timestamp() * 1000)
    limit = limits.get(t.get("tf") or "1h")

    def r_at(price):
        return sgn * (price - entry) / risk

    f, sl, got = 1.0, sl0, 0.0
    early_done = early_frac == 0
    half_done = False
    mfe = 0.0
    first_full = (opened_ms // 60_000 + 1) * 60_000      # girişten sonraki ilk tam dakika
    for (ts, hi, lo, cl) in candles:
        if ts < first_full:
            continue
        fav, adv = (hi, lo) if long else (lo, hi)
        # 1) SL (aynı mumda önce SL – temkinli)
        if (adv <= sl) if long else (adv >= sl):
            got += f * r_at(sl)
            return got, ("GİRİŞ" if half_done else "SL"), mfe
        mfe = max(mfe, r_at(fav))
        # 2) +0,5R'de erken kısmi kâr (SL'ye dokunmadan)
        if not early_done and ((fav >= early) if long else (fav <= early)):
            got += early_frac * r_at(early)
            f -= early_frac
            early_done = True
        # 3) Yarı kâr: orijinal miktarın yarısı kapanır, SL girişe
        if not half_done and ((fav >= mid) if long else (fav <= mid)):
            part = min(0.5, f)
            got += part * r_at(mid)
            f -= part
            half_done = True
            sl = entry
        # 4) TP
        if (fav >= tp) if long else (fav <= tp):
            got += f * r_at(tp)
            return got, "TP", mfe
        # 5) Süre sınırı
        if limit is not None and not (half_done and not after_mult):
            lim = limit * (after_mult if half_done else 1)
            if (ts + 60_000 - opened_ms) / 1000 >= lim:
                got += f * r_at(cl)
                return got, "SÜRE", mfe
    # Veri bitti: son fiyattan kapatılmış say
    if candles:
        got += f * r_at(candles[-1][3])
    return got, "AÇIK", mfe


def run(notify):
    trades = [t for t in rapor.load()
              if all(t.get(k) is not None for k in ("symbol", "side", "entry", "sl", "tp", "opened", "risk_usdt"))]
    if not trades:
        notify("📐 Analiz: henüz kaydı olan kapanmış işlem yok.")
        return
    now_ms = int(time.time() * 1000)

    results = {name: [] for name, *_ in RULES}
    actual, mfe_list = [], []
    skipped = 0
    for t in trades:
        opened_ms = int(datetime.datetime.fromisoformat(t["opened"]).timestamp() * 1000)
        end_ms = min(now_ms, opened_ms + MAX_HORIZON_H * H * 1000)
        try:
            candles = klines(t["symbol"], opened_ms - 60_000, end_ms)
        except Exception as e:
            print(f"{t['symbol']}: mumlar alınamadı ({e})")
            skipped += 1
            continue
        if not candles:
            skipped += 1
            continue
        risk_px = abs(t["entry"] - t["sl"])
        fee_r = 2 * TAKER_FEE * t["entry"] / risk_px            # komisyon, R cinsinden
        for name, limits, mult, early in RULES:
            res = simulate(t, candles, limits, mult, early)
            if res is None:
                continue
            r, ex, mfe = res
            results[name].append((r - fee_r, ex, t))
            if name == RULES[0][0]:
                mfe_list.append((mfe, r - fee_r, ex))
        if t.get("net") is not None:
            actual.append((t["net"], len(results[RULES[0][0]]) - 1))
        time.sleep(0.1)

    n = len(results[RULES[0][0]])
    avg_risk = sum(t["risk_usdt"] for t in trades) / len(trades)
    lines = [f"📐 Kural karşılaştırması – {n} gerçek işlem "
             f"({min(t['opened'] for t in trades)[:10]} → {max(t['closed'] for t in trades)[:10]})", ""]
    if actual:
        base = results[RULES[0][0]]
        sim_now = sum(base[i][0] * base[i][2]["risk_usdt"] for _, i in actual)
        lines.append(f"Gerçekleşen (Binance): {rapor._usd(sum(a for a, _ in actual))} USDT | "
                     f"aynı kuralın simülasyonu: {rapor._usd(sim_now)} USDT")
        lines.append("(fark büyükse simülasyon gerçeği iyi yansıtmıyor demektir)")
        lines.append("")

    best = None
    for name, *_ in RULES:
        rs = results[name]
        if not rs:
            continue
        usdt = sum(r * t["risk_usdt"] for r, _, t in rs)
        avg_r = sum(r for r, _, _ in rs) / len(rs)
        wins = sum(1 for r, _, _ in rs if r > 0)
        ex = {}
        for _, e, _ in rs:
            ex[e] = ex.get(e, 0) + 1
        ex_txt = " · ".join(f"{k} {v}" for k, v in sorted(ex.items(), key=lambda kv: -kv[1]))
        lines.append(f"▸ {name}")
        lines.append(f"   {rapor._usd(usdt)} USDT | {rapor._r(avg_r)}/işlem | kazanan {wins}/{len(rs)}")
        lines.append(f"   {ex_txt}")
        if best is None or usdt > best[1]:
            best = (name, usdt)

    # Senin gözlemin: +0,5R'yi görüp zararla kapanan işlemler (şu anki kural)
    went_up = [m for m in mfe_list if m[0] >= 0.5]
    lost_after = [m for m in went_up if m[1] < 0]
    lines += ["",
              f"🔍 +0,5R (%15 ROI) görüp sonra zararla kapanan: {len(lost_after)}/{len(went_up)} "
              f"(+0,5R görenler içinde)"]
    if best:
        lines.append(f"🏆 Bu veride en iyisi: {best[0]}")
    lines.append(f"(1R ≈ {avg_risk:.0f} USDT. Aynı mumda SL+hedef olursa önce SL sayıldı.)")
    if n < 30:
        lines.append(f"⚠️ {n} işlem az: farklar şans eseri olabilir, karar için en az 30-50 işlem gerekir.")
    if skipped:
        lines.append(f"({skipped} işlem için fiyat verisi alınamadı, hesaba katılmadı)")

    text = "\n".join(lines)
    print(text)
    notify(text)


def _send_telegram(text):
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat:
        return
    data = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
    urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=15).read()


if __name__ == "__main__":
    run(_send_telegram)
