"""
Kripto sinyal botu (Hetzner sunucusunda dakikada bir çalışır).

Dosya düzeni:
  signal_bot.py         – veri (Gate.io), ortak indikatörler, tarama, Telegram   ← stratejiye özel kod YOK
  strateji_<kod>.py     – bir stratejinin tamamı: coinler, zaman dilimi, giriş, stop, çıkış
  binance_trader.py     – Binance Futures'ta işlem açma ve yönetme
  rapor.py              – işlem kaydı, günlük / haftalık rapor, durma uyarıları

Hangi stratejilerin çalışacağı ayarlar.env → STRATEJILER (virgülle birden fazla).
Ayarlar ortam değişkenlerinden: SYMBOLS, STRATEJILER, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
TEST_RUN, TRADING_ENABLED, RAPOR_ISTEGI, BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_BASE_URL
"""
import datetime
import importlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo

import pandas as pd

# =========================================================================
# AYARLAR
# =========================================================================
DEFAULT_SYMBOLS = ("BTC_USDT, ETH_USDT, SOL_USDT, XRP_USDT, BNB_USDT, DOGE_USDT, ADA_USDT, "
                   "AVAX_USDT, LINK_USDT, DOT_USDT, LTC_USDT, TRX_USDT, SUI_USDT, NEAR_USDT, "
                   "APT_USDT, ARB_USDT, OP_USDT, ATOM_USDT, PEPE_USDT, SHIB_USDT")


def _env(name, default=""):
    return os.environ.get(name, default).strip().strip('"').strip("'").strip()


def _flag(name):
    """true/True/"true"/yes/1 -> True; boşluk, tırnak ve görünmeyen karakterler yok sayılır."""
    return _env(name).lower() in ("true", "1", "yes", "evet")


SYMBOLS = [s.strip().upper() for s in _env("SYMBOLS", DEFAULT_SYMBOLS).split(",") if s.strip()]
STRATEJILER = [s.strip().lower() for s in (_env("STRATEJILER") or "trend_4h").split(",") if s.strip()]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TEST_RUN = _flag("TEST_RUN")
TRADING_ENABLED = _flag("TRADING_ENABLED")   # true → sinyalde Binance Futures'ta işlem açılır
REPORT_NOW = _flag("REPORT_NOW")             # sinyalbot-rapor: raporu hemen gönder
TZ = ZoneInfo("Europe/Istanbul")             # mesajlardaki saatler Türkiye saati
STATE_FILE = "state.json"
CANDLE_LIMIT = 200
ATR_PERIOD = 14

INTERVAL_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
                    "4h": 14400, "8h": 28800, "1d": 86400}


# =========================================================================
# Ortak indikatörler (her stratejinin kullanabileceği sütunlar)
# =========================================================================
def ema(series, period):
    if len(series) == 0:
        return []
    out = [series[0]]
    k = 2 / (period + 1)
    for x in series[1:]:
        out.append((x - out[-1]) * k + out[-1])
    return out


def rsi(series, period=14):
    """Wilder RSI. İlk `period` mum için None."""
    out = [None] * len(series)
    deltas = [series[i] - series[i - 1] for i in range(1, len(series))]
    if len(deltas) < period:
        return out
    avg_gain = sum(d for d in deltas[:period] if d > 0) / period
    avg_loss = sum(-d for d in deltas[:period] if d < 0) / period
    for i in range(period, len(deltas)):
        d = deltas[i]
        avg_gain = (avg_gain * (period - 1) + max(d, 0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-d, 0)) / period
        out[i + 1] = 100 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    return out


def bollinger_bands(series, period=20, std_factor=2):
    mid, up, low = [], [], []
    for i in range(len(series)):
        if i < period - 1:
            mid.append(None), up.append(None), low.append(None)
            continue
        w = series[i - period + 1:i + 1]
        sma = sum(w) / period
        sd = (sum((p - sma) ** 2 for p in w) / period) ** 0.5
        mid.append(sma), up.append(sma + std_factor * sd), low.append(sma - std_factor * sd)
    return mid, up, low


def atr(highs, lows, closes, period=14):
    """Wilder ATR. İlk `period` mum için None."""
    n = len(closes)
    out = [None] * n
    if n <= period:
        return out
    trs = [highs[0] - lows[0]] + [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]),
                                      abs(lows[i] - closes[i - 1])) for i in range(1, n)]
    value = sum(trs[1:period + 1]) / period
    out[period] = value
    for i in range(period + 1, n):
        value = (value * (period - 1) + trs[i]) / period
        out[i] = value
    return out


def compute_indicators(df):
    """Ortak sütunlar: ema_12, ema_26, rsi, bb_m/bb_u/bb_l, atr. Stratejiye özel olanlar strateji.hazirla()'da."""
    closes = df["close"].tolist()
    df["ema_12"] = ema(closes, 12)
    df["ema_26"] = ema(closes, 26)
    df["rsi"] = rsi(closes, 14)
    df["bb_m"], df["bb_u"], df["bb_l"] = bollinger_bands(closes, 20, 2)
    df["atr"] = atr(df["high"].tolist(), df["low"].tolist(), closes, ATR_PERIOD)


def _v(x):
    """NaN / None -> None, aksi halde float."""
    if x is None or pd.isna(x):
        return None
    return float(x)


def market_state(df, idx):
    """Trend (YÜKSELİŞ / DÜŞÜŞ / YATAY): EMA12/26 dizilimi, kapanış ve EMA26 eğimi; dar farkta YATAY."""
    e12, e26 = _v(df["ema_12"][idx]), _v(df["ema_26"][idx])
    close = float(df["close"][idx])
    bb_u, bb_m = _v(df["bb_u"][idx]), _v(df["bb_m"][idx])
    slope = e26 - _v(df["ema_26"][idx - 10]) if idx >= 10 else 0.0
    std = (bb_u - bb_m) / 2 if bb_u is not None and bb_m is not None else None
    if std is not None and std > 0 and abs(e12 - e26) < 0.3 * std:
        return "YATAY"
    if e12 > e26 and close > e26 and slope > 0:
        return "YÜKSELİŞ"
    if e12 < e26 and close < e26 and slope < 0:
        return "DÜŞÜŞ"
    return "YATAY"


# =========================================================================
# Strateji yükleme, alarm ve mesajlar
# =========================================================================
_STRATS = {}


def load_strategy(kod):
    """strateji_<kod>.py dosyasını yükler ve gerekli alanları kontrol eder."""
    if kod not in _STRATS:
        m = importlib.import_module(f"strateji_{kod}")
        for name in ("KOD", "AD", "YON", "ZAMAN_DILIMI", "CIKIS", "giris", "hazirla"):
            if not hasattr(m, name):
                raise RuntimeError(f"strateji_{kod}.py içinde {name} tanımlı değil")
        if m.ZAMAN_DILIMI not in INTERVAL_SECONDS:
            raise RuntimeError(f"strateji_{kod}.py: geçersiz ZAMAN_DILIMI {m.ZAMAN_DILIMI!r}")
        if m.CIKIS.get("iz") and not hasattr(m, "iz_cikis"):
            raise RuntimeError(f"strateji_{kod}.py: CIKIS['iz'] açık ama iz_cikis() tanımlı değil")
        _STRATS[kod] = m
    return _STRATS[kod]


def check_alert(strat, df, htf_trend):
    """
    Son KAPANMIŞ mumda (len-2) stratejinin giriş kuralını uygular. Dönüş: None ya da
    side, entry, stop, target (TP yoksa None), sources, risk_atr, risk_pct, atr_pct, time,
    trend, htf_trend, strateji, cikis.
    """
    n = len(df)
    if n < 40:
        return None
    i = n - 2
    res = strat.giris(df, i, htf_trend)
    if not res:
        return None
    side, sources = res
    if (strat.YON == "long" and side != "AL") or (strat.YON == "short" and side != "SAT"):
        return None
    entry = float(df["close"][i])
    a = _v(df["atr"][i])
    if a is None or a <= 0:
        return None
    cikis = strat.CIKIS
    if hasattr(strat, "stop"):                       # strateji kendi stop'unu hesaplayabilir
        stop = strat.stop(df, i, side, entry, a)
        if stop is None:
            return None
    else:
        stop = entry - cikis["stop_atr"] * a if side == "AL" else entry + cikis["stop_atr"] * a
    risk = abs(entry - stop)
    if stop <= 0 or risk <= 0:
        return None
    target = None
    if cikis.get("tp_r"):
        target = entry + cikis["tp_r"] * risk if side == "AL" else entry - cikis["tp_r"] * risk
        if target <= 0:
            return None
    return {
        "side": side, "sources": sources, "entry": entry, "stop": stop, "target": target,
        "risk_pct": risk / entry * 100, "atr_pct": a / entry * 100, "risk_atr": risk / a,
        "time": df["datetime"][i], "trend": market_state(df, i), "htf_trend": htf_trend,
        "strateji": strat.KOD, "strateji_ad": strat.AD, "cikis": dict(cikis),
        "cikis_metni": strat.cikis_metni(side) if hasattr(strat, "cikis_metni") else None,
    }


def fmt_price(p):
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.8g}"


def _yon_txt(yon):
    return {"long": "LONG", "short": "SHORT"}.get(yon, "LONG + SHORT")


def strategy_card(strat):
    """Sitedeki formatta strateji tanıtımı."""
    coins = getattr(strat, "COINLER", None) or SYMBOLS
    names = ", ".join(c.split("_")[0] for c in coins[:6]) + (f" +{len(coins) - 6}" if len(coins) > 6 else "")
    return (f"🏆 Strateji: {strat.AD} - {_yon_txt(strat.YON)}\n"
            f"🏛️ Piyasa: Kripto ({names})\n\n"
            f"{getattr(strat, 'ACIKLAMA', '')}")


def alert_text(strat, symbol, a):
    tf = strat.ZAMAN_DILIMI
    close_t = (a["time"] + datetime.timedelta(seconds=INTERVAL_SECONDS[tf])).strftime("%d.%m %H:%M")
    head = ("🟢 LONG" if a["side"] == "AL" else "🔴 SHORT") + f" – {symbol} {tf}"
    htf = getattr(strat, "UST_ZAMAN_DILIMI", None)
    lines = [
        head,
        f"🏆 {strat.AD}",
        f"Fiyat: {fmt_price(a['entry'])} (mum kapanışı {close_t}) | Sinyal: {'+'.join(a['sources'])}",
        f"Trend {tf}: {a['trend']}" + (f" | {htf}: {a['htf_trend'] or '-'}" if htf else ""),
        f"🛑 SL: {fmt_price(a['stop'])} → -%{a['risk_pct']:.2f} ({a['risk_atr']:.1f} ATR)",
    ]
    if a["target"] is not None:
        lines.append(f"🎯 TP: {fmt_price(a['target'])} → +%{a['cikis']['tp_r'] * a['risk_pct']:.2f}")
    if a["cikis"].get("iz") and hasattr(strat, "cikis_metni"):
        lines.append(f"🚪 Çıkış: {strat.cikis_metni(a['side'])}")
    lines.append("(yüzdeler sinyal fiyatından, kaldıraçsız)")
    return "\n".join(lines)


# =========================================================================
# Gate.io ve Telegram
# =========================================================================
_CACHE = {}


def fetch_candles(symbol, interval):
    """Gate.io mumları + ortak indikatörler. Aynı çalıştırmada aynı veri bir kez çekilir."""
    key = (symbol, interval)
    if key not in _CACHE:
        q = urllib.parse.urlencode({"currency_pair": symbol, "interval": interval, "limit": CANDLE_LIMIT})
        req = urllib.request.Request(f"https://api.gateio.ws/api/v4/spot/candlesticks?{q}",
                                     headers={"Accept": "application/json", "User-Agent": "signal-bot"})
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="ignore")
            if "INVALID_CURRENCY_PAIR" in body:
                raise RuntimeError("Gate.io'da böyle bir parite yok, listeden çıkar")
            raise RuntimeError(f"HTTP {e.code}: {body[:200]}")
        # Gate: [zaman, hacim(quote), kapanış, yüksek, düşük, açılış, ...]
        rows = [[datetime.datetime.fromtimestamp(int(c[0]), tz=TZ).replace(tzinfo=None),
                 float(c[5]), float(c[3]), float(c[4]), float(c[2])] for c in data]
        df = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close"])
        df = df.sort_values("datetime").reset_index(drop=True)
        compute_indicators(df)
        _CACHE[key] = df
    return _CACHE[key].copy()


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[Telegram kapalı] " + text.replace("\n", " | "))
        return True
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": TELEGRAM_CHAT_ID, "text": text}).encode()
        urllib.request.urlopen(url, data=data, timeout=15).read()
        return True
    except Exception as e:
        print(f"Telegram gönderilemedi: {e}")
        return False


# =========================================================================
# Durum dosyası (aynı sinyali iki kez göndermemek için)
# =========================================================================
def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"alerted": {}, "last_error_msg": 0, "last_scan": {}}


def save_state(state):
    week_ago = time.time() - 7 * 86400
    state["alerted"] = {k: v for k, v in state["alerted"].items() if v > week_ago}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


# =========================================================================
# Tarama
# =========================================================================
def last_closed_open_ts(timeframe, now):
    """Son kapanmış mumun açılış zamanı (epoch saniye)."""
    sec = INTERVAL_SECONDS[timeframe]
    return int(now // sec * sec - sec)


def _log_event(kind, symbol=None, **data):
    try:
        import rapor
        rapor.log_event(kind, symbol, **data)
    except Exception as e:
        print(f"  olay kaydedilemedi ({e})")


def try_trade(symbol, strat, alert):
    import binance_trader
    try:
        opened, msg = binance_trader.open_trade(symbol, alert, strat.ZAMAN_DILIMI)
    except Exception as e:
        opened, msg = False, f"❌ {symbol} {strat.ZAMAN_DILIMI}: işlem açılamadı – {e}"
    print("  " + msg.replace("\n", " | "))
    kind = "open" if opened else ("skip" if msg.startswith("⏭") else "error")
    _log_event(kind, symbol, tf=strat.ZAMAN_DILIMI, strateji=strat.KOD, msg=msg.split("\n")[0])
    send_telegram(msg)


def scan_strategy(strat, state, errors, report):
    tf, htf = strat.ZAMAN_DILIMI, getattr(strat, "UST_ZAMAN_DILIMI", None)
    sent = 0
    for symbol in getattr(strat, "COINLER", None) or SYMBOLS:
        try:
            df = fetch_candles(symbol, tf)
            if len(df) < 40:
                raise RuntimeError("yeterli mum yok")
            strat.hazirla(df)
            htf_trend = None
            if htf:
                hdf = fetch_candles(symbol, htf)
                if len(hdf) >= 40:
                    htf_trend = market_state(hdf, len(hdf) - 2)
            trend = market_state(df, len(df) - 2)
            report.append(f"{symbol} {tf}: {trend} / {htf or '-'}: {htf_trend or '-'}")

            alert = check_alert(strat, df, htf_trend)
            if not alert:
                print(f"[{strat.KOD}] {symbol}: sinyal yok ({trend})")
                continue
            key = f"{strat.KOD}|{symbol}|{alert['time'].isoformat()}|{alert['side']}"
            if key in state["alerted"]:
                print(f"[{strat.KOD}] {symbol}: sinyal daha önce gönderildi, atlandı")
            elif send_telegram(alert_text(strat, symbol, alert)):
                state["alerted"][key] = time.time()
                sent += 1
                print(f"[{strat.KOD}] {symbol}: {alert['side']} sinyali gönderildi")
                _log_event("signal", symbol, tf=tf, strateji=strat.KOD, side=alert["side"])
                if TRADING_ENABLED:
                    try_trade(symbol, strat, alert)
        except Exception as e:
            msg = f"[{strat.KOD}] {symbol} {tf}: {e}"
            if msg not in errors:
                errors.append(msg)
            print(f"HATA – {msg}")
        time.sleep(0.1)
    return sent


def _test_message(strats, scanned, total, errors, sent, report, remote):
    now = datetime.datetime.now(TZ).strftime("%d.%m %H:%M")
    msg = [f"✅ {'Durum raporu' if remote and not TEST_RUN else 'Test'}: sinyal botu çalışıyor ({now})",
           f"Stratejiler: {', '.join(s.KOD for s in strats)} | taranan: {', '.join(scanned) or '-'} | "
           f"{total - len(errors)}/{total} tarama başarılı | {sent} sinyal gönderildi"]
    if TRADING_ENABLED:
        import binance_trader
        msg += ["", binance_trader.status_line()]
    else:
        msg += ["", f"ℹ️ Otomatik işlem KAPALI (TRADING_ENABLED={os.environ.get('TRADING_ENABLED', 'tanımsız')!r})"]
    for s in strats:
        msg += ["", strategy_card(s)]
    dist = {}
    for line in report:
        trend = line.split(" / ")[0].split(": ")[-1]
        dist[trend] = dist.get(trend, 0) + 1
    if dist:
        msg += ["", "Trend dağılımı: " + " · ".join(f"{t} {n}" for t, n in sorted(dist.items(), key=lambda kv: -kv[1]))]
    msg += ["", "Trendler:"] + report
    if errors:
        msg += ["", "Hatalar:"] + errors
    return "\n".join(msg)


def main():
    strats = []
    for kod in STRATEJILER:
        try:
            strats.append(load_strategy(kod))
        except Exception as e:
            print(f"Strateji yüklenemedi ({kod}): {e}")
            send_telegram(f"❌ Strateji yüklenemedi: {kod} – {e}")
    if not strats:
        sys.exit(1)

    state = load_state()
    state.setdefault("last_scan", {})

    # Uzaktan durum isteği: ayarlar.env'deki RAPOR_ISTEGI değeri değişince bir kez
    # test mesajı (strateji kartları, trendler, Binance durumu) ve günün raporu gönderilir.
    remote_req = _env("RAPOR_ISTEGI")
    remote = bool(remote_req) and state.get("last_remote_request") != remote_req
    if remote:
        state["last_remote_request"] = remote_req
        save_state(state)
        print(f"Uzaktan durum isteği alındı ({remote_req})")
    test_mode = TEST_RUN or remote

    if TRADING_ENABLED:
        import binance_trader
        import rapor
        binance_trader.manage_positions(send_telegram)   # yarı kâr, iz süren çıkış, süre, kapanış bildirimi
        rapor.maybe_send_daily(send_telegram, binance_trader.daily_numbers, force=REPORT_NOW or remote)
        rapor.maybe_send_weekly(send_telegram, binance_trader.status_line, force=REPORT_NOW)
        if REPORT_NOW:
            print("Rapor gönderildi.")
            return
        binance_trader.cleanup_orphan_orders()

    now_ts = time.time()
    errors, report, sent, scanned, total = [], [], 0, [], 0
    for strat in strats:
        closed = last_closed_open_ts(strat.ZAMAN_DILIMI, now_ts)
        last = state["last_scan"].get(strat.KOD, state["last_scan"].get(strat.ZAMAN_DILIMI, 0))
        if not test_mode and last >= closed:
            print(f"[{strat.KOD}] yeni {strat.ZAMAN_DILIMI} mum kapanmadı, atlandı")
            continue
        n_err = len(errors)
        coins = getattr(strat, "COINLER", None) or SYMBOLS
        sent += scan_strategy(strat, state, errors, report)
        scanned.append(strat.KOD)
        total += len(coins)
        if len(errors) - n_err < len(coins):          # en az bir coin tarandıysa
            state["last_scan"][strat.KOD] = closed

    if test_mode:
        send_telegram(_test_message(strats, scanned, total, errors, sent, report, remote))
    elif errors and time.time() - state.get("last_error_msg", 0) > 6 * 3600:
        now = datetime.datetime.now(TZ).strftime("%d.%m %H:%M")
        send_telegram(f"⚠️ Sinyal botu ({now}): {len(errors)}/{total} tarama başarısız\n" + "\n".join(errors[:10]))
        state["last_error_msg"] = time.time()

    save_state(state)
    print(f"Bitti: taranan {scanned}, {sent} sinyal, {len(errors)} hata")

    # Backtest: ayarlar.env'deki BACKTEST_ISTEGI değişince başlar, canlı işten sonra kalan sürede ilerler
    if not TEST_RUN:
        try:
            import backtest
            backtest.adim(send_telegram)
        except Exception as e:
            print(f"Backtest adımı hata verdi: {e}")

    if total and len(errors) == total:
        sys.exit(1)


if __name__ == "__main__":
    main()
