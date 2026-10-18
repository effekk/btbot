"""
Gate.io sinyal botu – GitHub Actions üzerinde çalışır, bilgisayar gerekmez.

Her çalıştırmada izleme listesindeki coinleri tarar. Son KAPANMIŞ mumda
trend yönünde ve üst zaman dilimiyle uyumlu bir sinyal bulursa Telegram'a gönderir.
Aynı sinyal ikinci kez gönderilmez (state.json).

Ayarlar ortam değişkenlerinden okunur (.github/workflows/signal.yml içinde):
  SYMBOLS, TIMEFRAME, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TEST_RUN
"""
import datetime
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
SYMBOLS = [s.strip().upper() for s in os.environ.get("SYMBOLS", DEFAULT_SYMBOLS).split(",") if s.strip()]
TIMEFRAME = os.environ.get("TIMEFRAME", "1h").strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TEST_RUN = os.environ.get("TEST_RUN", "").lower() == "true"
TZ = ZoneInfo("Europe/Istanbul")      # mesajlardaki saatler Türkiye saati
STATE_FILE = "state.json"

RECENT_WINDOW = 3
TREND_SOURCES = {"EMA", "MACD"}
REVERSION_SOURCES = {"RSI", "BB"}
MIN_INDICATORS = 1        # alarm için aynı mumda en az kaç indikatör tetiklenmeli
STOP_LOOKBACK = 10        # stop seviyesi: son kaç kapanmış mumun dibi / tepesi
CANDLE_LIMIT = 200

INTERVAL_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
                    "4h": 14400, "8h": 28800, "1d": 86400, "7d": 604800}
HIGHER_TF = {"1m": "15m", "5m": "1h", "15m": "1h", "30m": "4h", "1h": "4h",
             "4h": "1d", "8h": "1d", "1d": "7d"}


# =========================================================================
# İndikatörler, sinyaller ve alarm kuralı (masaüstü sürümüyle birebir aynı)
# =========================================================================
def ema(series, period):
    if len(series) == 0:
        return []
    ema_values = []
    multiplier = 2 / (period + 1)
    ema_values.append(series[0])
    for i in range(1, len(series)):
        prev_ema = ema_values[-1]
        value = (series[i] - prev_ema) * multiplier + prev_ema
        ema_values.append(value)
    return ema_values


def rsi(series, period=14):
    if len(series) < 2:
        return [None] * len(series)
    deltas = [series[i] - series[i - 1] for i in range(1, len(series))]
    rsi_list = [None] * len(series)
    if len(deltas) < period:
        return rsi_list
    gains = [d for d in deltas[:period] if d > 0]
    losses = [abs(d) for d in deltas[:period] if d < 0]
    avg_gain = sum(gains) / period if gains else 0
    avg_loss = sum(losses) / period if losses else 0

    for i in range(period, len(deltas)):
        delta = deltas[i]
        gain = delta if delta > 0 else 0
        loss = abs(delta) if delta < 0 else 0

        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

        if avg_loss == 0:
            rsi_value = 100
        else:
            rs = avg_gain / avg_loss
            rsi_value = 100 - (100 / (1 + rs))

        rsi_list[i + 1] = rsi_value

    return rsi_list


def macd(series, fast=12, slow=26, signal=9):
    if len(series) == 0:
        return [], [], []
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = ema(macd_line, signal)
    histogram = [m - sig for m, sig in zip(macd_line, signal_line)]
    return macd_line, signal_line, histogram


def bollinger_bands(series, period=20, std_factor=2):
    if len(series) == 0:
        return [], [], []
    middle_band, upper_band, lower_band = [], [], []
    for i in range(len(series)):
        if i < period - 1:
            middle_band.append(None)
            upper_band.append(None)
            lower_band.append(None)
        else:
            window = series[i - period + 1: i + 1]
            sma = sum(window) / period
            variance = sum((p - sma) ** 2 for p in window) / period
            stddev = variance ** 0.5
            middle_band.append(sma)
            upper_band.append(sma + std_factor * stddev)
            lower_band.append(sma - std_factor * stddev)
    return middle_band, upper_band, lower_band


def compute_indicators(df):
    closes = df["close"].tolist()
    df["rsi"] = rsi(closes, period=14)
    macd_line, signal_line, hist_line = macd(closes, fast=12, slow=26, signal=9)
    df["macd"] = macd_line
    df["macd_signal"] = signal_line
    df["macd_hist"] = hist_line
    df["ema_12"] = ema(closes, 12)
    df["ema_26"] = ema(closes, 26)
    mb, ub, lb = bollinger_bands(closes, period=20, std_factor=2)
    df["bb_m"] = mb
    df["bb_u"] = ub
    df["bb_l"] = lb


def _v(x):
    """NaN / None -> None, aksi halde float."""
    if x is None or pd.isna(x):
        return None
    return float(x)


def compute_signals(df):
    n = len(df)
    buy_src = [[] for _ in range(n)]
    sell_src = [[] for _ in range(n)]

    close_ = df["close"].tolist()
    rsi_ = [_v(x) for x in df["rsi"]]
    macd_ = [_v(x) for x in df["macd"]]
    sig_ = [_v(x) for x in df["macd_signal"]]
    e12 = [_v(x) for x in df["ema_12"]]
    e26 = [_v(x) for x in df["ema_26"]]
    bb_u = [_v(x) for x in df["bb_u"]]
    bb_l = [_v(x) for x in df["bb_l"]]

    for i in range(1, n):
        # RSI: 30'un altından yukarı -> AL, 70'in üstünden aşağı -> SAT
        if rsi_[i - 1] is not None and rsi_[i] is not None:
            if rsi_[i - 1] < 30 <= rsi_[i]:
                buy_src[i].append("RSI")
            if rsi_[i - 1] > 70 >= rsi_[i]:
                sell_src[i].append("RSI")

        # MACD sinyal çizgisi kesişimi
        if None not in (macd_[i - 1], sig_[i - 1], macd_[i], sig_[i]):
            if macd_[i - 1] < sig_[i - 1] and macd_[i] > sig_[i]:
                buy_src[i].append("MACD")
            if macd_[i - 1] > sig_[i - 1] and macd_[i] < sig_[i]:
                sell_src[i].append("MACD")

        # EMA12 / EMA26 kesişimi
        if None not in (e12[i - 1], e26[i - 1], e12[i], e26[i]):
            if e12[i - 1] < e26[i - 1] and e12[i] > e26[i]:
                buy_src[i].append("EMA")
            if e12[i - 1] > e26[i - 1] and e12[i] < e26[i]:
                sell_src[i].append("EMA")

        # Bollinger bandı dışında kapanış
        if bb_u[i] is not None and bb_l[i] is not None:
            if close_[i] < bb_l[i]:
                buy_src[i].append("BB")
            if close_[i] > bb_u[i]:
                sell_src[i].append("BB")

    return buy_src, sell_src


def market_state(df, idx):
    """Trend (YÜKSELİŞ / DÜŞÜŞ / YATAY) ve Bollinger sıkışması."""
    e12 = _v(df["ema_12"][idx])
    e26 = _v(df["ema_26"][idx])
    close = float(df["close"][idx])
    bb_u = _v(df["bb_u"][idx])
    bb_m = _v(df["bb_m"][idx])
    bb_l = _v(df["bb_l"][idx])

    look = 10
    slope = e26 - _v(df["ema_26"][idx - look]) if idx >= look else 0.0

    std = (bb_u - bb_m) / 2 if bb_u is not None and bb_m is not None else None

    if std is not None and std > 0 and abs(e12 - e26) < 0.3 * std:
        trend = "YATAY"
    elif e12 > e26 and close > e26 and slope > 0:
        trend = "YÜKSELİŞ"
    elif e12 < e26 and close < e26 and slope < 0:
        trend = "DÜŞÜŞ"
    else:
        trend = "YATAY"

    # Sıkışma: şu anki bant genişliği son 100 mumun ortalamasının %75'inden dar mı?
    squeeze = False
    if bb_u is not None and bb_l is not None and bb_m:
        widths = []
        for j in range(max(0, idx - 100), idx + 1):
            u, l, m = _v(df["bb_u"][j]), _v(df["bb_l"][j]), _v(df["bb_m"][j])
            if u is not None and l is not None and m:
                widths.append((u - l) / m)
        if len(widths) >= 20:
            avg = sum(widths) / len(widths)
            squeeze = widths[-1] < 0.75 * avg

    return trend, squeeze


def check_alert(df, buy_src, sell_src, htf_trend, min_indicators=MIN_INDICATORS):
    """
    Sadece SON KAPANMIŞ mumdaki sinyale bakar. Alarm şu koşulların hepsinde verilir:
      - sinyal tek yönlü (aynı mumda hem AL hem SAT yok)
      - en az `min_indicators` indikatör tetiklenmiş
      - sinyal mevcut zaman dilimindeki trendle aynı yönde (AL->YÜKSELİŞ, SAT->DÜŞÜŞ)
      - üst zaman dilimindeki trend de aynı yönde (biliniyorsa)
    Yatay piyasadaki sinyaller ve trende ters sinyaller alarm üretmez.
    """
    n = len(df)
    if n < 40:
        return None
    i = n - 2
    if bool(buy_src[i]) == bool(sell_src[i]):     # sinyal yok ya da çelişkili
        return None
    side = "AL" if buy_src[i] else "SAT"
    src = buy_src[i] or sell_src[i]
    if len(src) < min_indicators:
        return None

    trend, squeeze = market_state(df, i)
    wanted = "YÜKSELİŞ" if side == "AL" else "DÜŞÜŞ"
    if trend != wanted:
        return None
    if htf_trend is not None and htf_trend != wanted:
        return None

    entry = float(df["close"][i])
    lo = max(0, i - STOP_LOOKBACK + 1)
    if side == "AL":
        stop = float(df["low"][lo:i + 1].min())
        risk = entry - stop
        target = entry + 2 * risk
    else:
        stop = float(df["high"][lo:i + 1].max())
        risk = stop - entry
        target = entry - 2 * risk
    if risk <= 0:
        return None

    return {
        "side": side, "sources": src, "trend": trend, "htf_trend": htf_trend,
        "squeeze": squeeze, "time": df["datetime"][i], "entry": entry,
        "stop": stop, "target": target, "risk_pct": risk / entry * 100,
    }


def fmt_price(p):
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.8g}"


def alert_text(symbol, timeframe, a):
    htf = HIGHER_TF.get(timeframe)
    head = ("🟢 AL" if a["side"] == "AL" else "🔴 SAT") + f" – {symbol} {timeframe}"
    lines = [
        head,
        f"Fiyat: {fmt_price(a['entry'])} (mum kapanışı "
        f"{(a['time'] + datetime.timedelta(seconds=INTERVAL_SECONDS[timeframe])).strftime('%d.%m %H:%M')})",
        f"Sinyal: {'+'.join(a['sources'])}",
        f"Trend {timeframe}: {a['trend']}" + (f" | {htf}: {a['htf_trend'] or '-'}" if htf else ""),
        f"Stop: {fmt_price(a['stop'])} ({a['risk_pct']:.2f}%)",
        f"Hedef (1:2): {fmt_price(a['target'])}",
    ]
    if a["side"] == "SAT":
        lines.append("Not: spotta short açılamaz; elindekini satmak ya da vadeli hesap.")
    if a["squeeze"]:
        lines.append("Not: Bollinger bantları sıkışık.")
    return "\n".join(lines)


# =========================================================================
# Gate.io ve Telegram
# =========================================================================
def fetch_candles(symbol, interval, limit=CANDLE_LIMIT):
    """Gate.io herkese açık REST uç noktasından mum çeker (API anahtarı gerekmez)."""
    q = urllib.parse.urlencode({"currency_pair": symbol, "interval": interval, "limit": limit})
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

    rows = []
    for c in data:
        dt = datetime.datetime.fromtimestamp(int(c[0]), tz=TZ).replace(tzinfo=None)
        rows.append([dt, float(c[5]), float(c[3]), float(c[4]), float(c[2]), float(c[1])])
    df = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close", "volume"])
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)
    compute_indicators(df)
    return df


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
        return {"alerted": {}, "last_error_msg": 0}


def save_state(state):
    week_ago = time.time() - 7 * 86400
    state["alerted"] = {k: v for k, v in state["alerted"].items() if v > week_ago}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


# =========================================================================
# Ana akış
# =========================================================================
def main():
    if TIMEFRAME not in INTERVAL_SECONDS:
        print(f"Geçersiz TIMEFRAME: {TIMEFRAME}")
        sys.exit(1)

    state = load_state()
    htf = HIGHER_TF.get(TIMEFRAME)
    errors, report, sent = [], [], 0

    for symbol in SYMBOLS:
        try:
            df = fetch_candles(symbol, TIMEFRAME)
            if len(df) < 40:
                raise RuntimeError("yeterli mum yok")
            buy_src, sell_src = compute_signals(df)

            htf_trend = None
            if htf:
                hdf = fetch_candles(symbol, htf, limit=120)
                if len(hdf) >= 40:
                    htf_trend, _ = market_state(hdf, len(hdf) - 2)

            trend, _ = market_state(df, len(df) - 2)
            report.append(f"{symbol}: {trend} / {htf or '-'}: {htf_trend or '-'}")

            alert = check_alert(df, buy_src, sell_src, htf_trend)
            if alert:
                key = f"{symbol}|{TIMEFRAME}|{alert['time'].isoformat()}|{alert['side']}"
                if key in state["alerted"]:
                    print(f"{symbol}: sinyal daha önce gönderildi, atlandı")
                elif send_telegram(alert_text(symbol, TIMEFRAME, alert)):
                    state["alerted"][key] = time.time()
                    sent += 1
                    print(f"{symbol}: {alert['side']} sinyali gönderildi")
            else:
                print(f"{symbol}: alarm yok ({trend})")
        except Exception as e:
            errors.append(f"{symbol}: {e}")
            print(f"{symbol}: HATA – {e}")
        time.sleep(0.2)

    now = datetime.datetime.now(TZ).strftime("%d.%m %H:%M")
    if TEST_RUN:
        msg = [f"✅ Test: sinyal botu çalışıyor ({now})",
               f"Zaman dilimi: {TIMEFRAME} | {len(SYMBOLS) - len(errors)}/{len(SYMBOLS)} coin tarandı | "
               f"{sent} sinyal gönderildi"]
        msg += ["", "Trendler:"] + report
        if errors:
            msg += ["", "Hatalar:"] + errors
        send_telegram("\n".join(msg))
    elif errors and time.time() - state.get("last_error_msg", 0) > 6 * 3600:
        # Hata bildirimi en fazla 6 saatte bir
        send_telegram(f"⚠️ Sinyal botu ({now}): {len(errors)}/{len(SYMBOLS)} coin taranamadı\n"
                      + "\n".join(errors[:10]))
        state["last_error_msg"] = time.time()

    save_state(state)
    print(f"Bitti: {len(SYMBOLS) - len(errors)} coin tarandı, {sent} sinyal, {len(errors)} hata")
    if len(errors) == len(SYMBOLS):
        sys.exit(1)   # hepsi başarısızsa GitHub da e-posta ile haber verir


if __name__ == "__main__":
    main()
