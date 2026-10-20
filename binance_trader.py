"""
Binance USDⓈ-M Futures otomatik işlem modülü (varsayılan: DEMO hesap).

Sinyal botu bir sinyal bulduğunda `open_trade(...)` çağrılır:
  1. Aynı coinde açık pozisyon varsa ya da açık pozisyon sayısı sınırdaysa işlem açılmaz.
  2. Pozisyon büyüklüğü: MARGIN_USDT teminat, SL'ye gelirse teminatın LOSS_PCT_OF_MARGIN'i kadar zarar.
       notional = (MARGIN × LOSS_PCT) / (|giriş − SL| / giriş)
       kaldıraç = yukarı yuvarla(notional / MARGIN)  (coinin izin verdiği maksimumla sınırlı)
  3. ISOLATED marjin, piyasa emriyle giriş.
  4. SL (STOP_MARKET) ve TP (TAKE_PROFIT_MARKET) "pozisyonun tamamını kapat" emirleri.
     SL konamazsa pozisyon güvenlik için hemen piyasa emriyle kapatılır.

Ayarlar (ortam değişkenleri):
  BINANCE_API_KEY, BINANCE_API_SECRET   – GitHub Secrets
  BINANCE_BASE_URL                      – varsayılan demo: https://demo-fapi.binance.com
"""
import hashlib
import hmac
import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request

# =========================================================================
# AYARLAR
# =========================================================================
BASE_URL = os.environ.get("BINANCE_BASE_URL", "https://demo-fapi.binance.com").strip().rstrip("/")
API_KEY = os.environ.get("BINANCE_API_KEY", "").strip()
API_SECRET = os.environ.get("BINANCE_API_SECRET", "").strip()

MARGIN_USDT = 1000.0          # her işlemde kullanılacak teminat
LOSS_PCT_OF_MARGIN = 0.30     # SL'de teminatın %30'u kaybedilir (1000 $ → 300 $)
MAX_OPEN_POSITIONS = 4        # aynı anda en fazla kaç pozisyon
MAX_LEVERAGE = 50             # coinin izni daha yüksek olsa bile bu sınırı aşma
MAX_PRICE_DIFF_PCT = 1.0      # Gate sinyal fiyatı ile Binance fiyatı bundan fazla farklıysa işlem açma

IS_DEMO = "demo" in BASE_URL or "testnet" in BASE_URL


class BinanceError(Exception):
    def __init__(self, status, code, msg):
        super().__init__(f"Binance {status} [{code}]: {msg}")
        self.status, self.code, self.msg = status, code, msg


# =========================================================================
# Düşük seviye istekler
# =========================================================================
_time_offset = None


def _server_offset():
    global _time_offset
    if _time_offset is None:
        t = _request("GET", "/fapi/v1/time", signed=False)["serverTime"]
        _time_offset = int(t) - int(time.time() * 1000)
    return _time_offset


def _request(method, path, params=None, signed=True):
    params = dict(params or {})
    if signed:
        if not API_KEY or not API_SECRET:
            raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET tanımlı değil")
        params["timestamp"] = int(time.time() * 1000) + _server_offset()
        params["recvWindow"] = 10000
    query = urllib.parse.urlencode(params)
    if signed:
        sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
        query = f"{query}&signature={sig}"
    url = f"{BASE_URL}{path}" + (f"?{query}" if query else "")
    headers = {"User-Agent": "signal-bot"}
    if API_KEY:
        headers["X-MBX-APIKEY"] = API_KEY
    req = urllib.request.Request(url, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="ignore")
        try:
            j = json.loads(raw)
            raise BinanceError(e.code, j.get("code"), j.get("msg"))
        except (ValueError, AttributeError):
            if e.code == 451:
                raise BinanceError(451, None, "Binance bu sunucunun bulunduğu ülkeden erişimi engelliyor")
            raise BinanceError(e.code, None, raw[:200])


# =========================================================================
# Sembol bilgileri ve yuvarlama
# =========================================================================
_exchange_info = None


def _symbols():
    global _exchange_info
    if _exchange_info is None:
        info = _request("GET", "/fapi/v1/exchangeInfo", signed=False)
        _exchange_info = {s["symbol"]: s for s in info["symbols"]
                          if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"}
    return _exchange_info


def map_symbol(gate_symbol):
    """BTC_USDT -> (BTCUSDT, 1). PEPE_USDT -> (1000PEPEUSDT, 1000): fiyatlar 1000 ile çarpılır."""
    base, quote = gate_symbol.split("_")
    syms = _symbols()
    for mult in (1, 1000, 10000, 1000000):
        name = f"{base}{quote}" if mult == 1 else f"{mult}{base}{quote}"
        if name in syms:
            return name, mult
    raise RuntimeError(f"{gate_symbol}: Binance Futures'ta karşılığı yok")


def _filters(symbol):
    f = {x["filterType"]: x for x in _symbols()[symbol]["filters"]}
    tick = float(f["PRICE_FILTER"]["tickSize"])
    lot = f.get("MARKET_LOT_SIZE") or f["LOT_SIZE"]
    step = max(float(lot["stepSize"]), float(f["LOT_SIZE"]["stepSize"]))
    max_qty = float(lot["maxQty"])
    min_qty = float(f["LOT_SIZE"]["minQty"])
    min_notional = float(f.get("MIN_NOTIONAL", {}).get("notional", 5))
    return tick, step, min_qty, max_qty, min_notional


def _decimals(x):
    s = f"{x:.12f}".rstrip("0")
    return len(s.split(".")[1]) if "." in s else 0


def _round_price(p, tick):
    return f"{round(round(p / tick) * tick, _decimals(tick)):.{_decimals(tick)}f}"


def _floor_qty(q, step):
    n = math.floor(q / step * (1 + 1e-9))          # kayan nokta hatasına karşı küçük pay
    return f"{n * step:.{_decimals(step)}f}"


# =========================================================================
# Hesap durumu
# =========================================================================
def _positions():
    data = _request("GET", "/fapi/v3/positionRisk")
    return [p for p in data if abs(float(p["positionAmt"])) > 0]


def _available_usdt():
    for a in _request("GET", "/fapi/v3/balance"):
        if a["asset"] == "USDT":
            return float(a["availableBalance"])
    return 0.0


def _hedge_mode():
    return bool(_request("GET", "/fapi/v1/positionSide/dual").get("dualSidePosition"))


def _max_leverage_for(symbol, notional):
    data = _request("GET", "/fapi/v1/leverageBracket", {"symbol": symbol})
    brackets = data[0]["brackets"] if isinstance(data, list) else data["brackets"]
    for b in sorted(brackets, key=lambda x: x["notionalCap"]):
        if notional <= float(b["notionalCap"]):
            return int(b["initialLeverage"])
    return 1


def _cancel_all(symbol):
    for path in ("/fapi/v1/algoOpenOrders", "/fapi/v1/allOpenOrders"):
        try:
            _request("DELETE", path, {"symbol": symbol})
        except BinanceError:
            pass


def cleanup_orphan_orders():
    """Pozisyonu kapanmış (SL ya da TP olmuş) coinlerde kalan emirleri iptal eder."""
    if not API_KEY:
        return
    try:
        open_syms = {p["symbol"] for p in _positions()}
        orphan = set()
        try:
            for o in _request("GET", "/fapi/v1/openAlgoOrders"):
                orphan.add(o["symbol"])
        except BinanceError:
            pass
        for o in _request("GET", "/fapi/v1/openOrders"):
            orphan.add(o["symbol"])
        for sym in orphan - open_syms:
            _cancel_all(sym)
            print(f"  {sym}: pozisyon kapanmış, kalan emirler iptal edildi")
    except Exception as e:
        print(f"  emir temizliği yapılamadı: {e}")


# =========================================================================
# SL / TP emirleri (önce yeni Algo Order API, olmazsa eski uç nokta)
# =========================================================================
def _place_close_order(symbol, close_side, order_type, trigger, pos_side):
    params = {"algoType": "CONDITIONAL", "symbol": symbol, "side": close_side, "type": order_type,
              "triggerPrice": trigger, "closePosition": "true", "workingType": "MARK_PRICE",
              "positionSide": pos_side}
    try:
        return _request("POST", "/fapi/v1/algoOrder", params)
    except BinanceError as e:
        if e.status in (404, 405) or e.code in (-1102, -1104, -1116, -1000, -5000):
            legacy = {"symbol": symbol, "side": close_side, "type": order_type, "stopPrice": trigger,
                      "closePosition": "true", "workingType": "MARK_PRICE", "positionSide": pos_side}
            return _request("POST", "/fapi/v1/order", legacy)
        raise


# =========================================================================
# Ana fonksiyon
# =========================================================================
def open_trade(gate_symbol, alert):
    """
    alert: signal_bot.check_alert sonucu (side, entry, stop, target ...).
    Dönüş: (açıldı_mı: bool, Telegram mesajı: str)
    """
    side = alert["side"]                        # "AL" -> LONG, "SAT" -> SHORT
    symbol, mult = map_symbol(gate_symbol)
    tick, step, min_qty, max_qty, min_notional = _filters(symbol)
    sl = alert["stop"] * mult
    tp = alert["target"] * mult

    # 1) Açık pozisyon kontrolleri
    positions = _positions()
    if any(p["symbol"] == symbol for p in positions):
        return False, f"⏭ {symbol}: zaten açık pozisyon var, yeni işlem açılmadı."
    if len(positions) >= MAX_OPEN_POSITIONS:
        return False, f"⏭ {symbol}: {len(positions)} açık pozisyon var (sınır {MAX_OPEN_POSITIONS}), işlem açılmadı."
    avail = _available_usdt()
    if avail < MARGIN_USDT * 1.05:
        return False, f"⏭ {symbol}: kullanılabilir bakiye yetersiz ({avail:.0f} USDT)."

    # 2) Güncel fiyat ve SL/TP kontrolü
    mark = float(_request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol}, signed=False)["markPrice"])
    gate_entry = alert["entry"] * mult
    if abs(mark - gate_entry) / gate_entry * 100 > MAX_PRICE_DIFF_PCT:
        return False, (f"⏭ {symbol}: fiyat sinyalden çok uzaklaşmış (sinyal {gate_entry:.6g}, "
                       f"şimdi {mark:.6g}), işlem açılmadı.")
    if (side == "AL" and not (sl < mark < tp)) or (side == "SAT" and not (tp < mark < sl)):
        return False, f"⏭ {symbol}: fiyat SL/TP aralığının dışında, işlem açılmadı."

    # 3) Büyüklük ve kaldıraç
    stop_frac = abs(mark - sl) / mark
    target_loss = MARGIN_USDT * LOSS_PCT_OF_MARGIN
    notional = target_loss / stop_frac
    leverage = max(1, math.ceil(notional / MARGIN_USDT))
    allowed = min(MAX_LEVERAGE, _max_leverage_for(symbol, notional))
    if leverage > allowed:
        leverage = allowed
        notional = MARGIN_USDT * leverage            # zarar hedefin altında kalır
        allowed2 = min(MAX_LEVERAGE, _max_leverage_for(symbol, notional))
        if allowed2 < leverage:
            leverage = allowed2
            notional = MARGIN_USDT * leverage

    qty = float(_floor_qty(min(notional / mark, max_qty), step))
    if qty < min_qty or qty * mark < min_notional:
        return False, f"⏭ {symbol}: hesaplanan miktar çok küçük, işlem açılmadı."

    # 4) Hesap ayarları
    try:
        _request("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": "ISOLATED"})
    except BinanceError as e:
        if e.code != -4046:                          # -4046: zaten ISOLATED
            raise
    _request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})

    hedge = _hedge_mode()
    open_side = "BUY" if side == "AL" else "SELL"
    close_side = "SELL" if side == "AL" else "BUY"
    pos_side = ("LONG" if side == "AL" else "SHORT") if hedge else "BOTH"

    # 5) Giriş
    _cancel_all(symbol)                               # eski, artık emir kalmasın
    order = _request("POST", "/fapi/v1/order", {
        "symbol": symbol, "side": open_side, "type": "MARKET", "quantity": _floor_qty(qty, step),
        "positionSide": pos_side, "newOrderRespType": "RESULT"})
    fill = float(order.get("avgPrice") or 0) or mark
    filled_qty = float(order.get("executedQty") or qty)

    # 6) SL ve TP – SL konamazsa pozisyonu hemen kapat
    sl_str, tp_str = _round_price(sl, tick), _round_price(tp, tick)
    try:
        _place_close_order(symbol, close_side, "STOP_MARKET", sl_str, pos_side)
    except Exception as e:
        close = {"symbol": symbol, "side": close_side, "type": "MARKET",
                 "quantity": _floor_qty(filled_qty, step), "positionSide": pos_side}
        if not hedge:
            close["reduceOnly"] = "true"
        try:
            _request("POST", "/fapi/v1/order", close)
            note = "pozisyon güvenlik için hemen kapatıldı"
        except Exception as e2:
            note = f"POZİSYON KAPATILAMADI, ELLE KAPAT! ({e2})"
        return False, f"❌ {symbol}: SL emri konamadı ({e}) – {note}."

    tp_note = ""
    try:
        _place_close_order(symbol, close_side, "TAKE_PROFIT_MARKET", tp_str, pos_side)
    except Exception as e:
        tp_note = f"\n⚠️ TP emri konamadı ({e}) – TP'yi elle koy."

    notional_real = filled_qty * fill
    loss_at_sl = notional_real * abs(fill - float(sl_str)) / fill
    gain_at_tp = notional_real * abs(float(tp_str) - fill) / fill
    msg = (
        f"{'🟩 LONG' if side == 'AL' else '🟥 SHORT'} açıldı – {symbol}{' (DEMO)' if IS_DEMO else ''}\n"
        f"Giriş: {fill:.{_decimals(tick)}f} | Miktar: {filled_qty:,.{_decimals(step)}f}\n"
        f"Kaldıraç: {leverage}x ISOLATED | Teminat: ~{notional_real / leverage:.0f} USDT | "
        f"Pozisyon: {notional_real:,.0f} USDT\n"
        f"🛑 SL: {sl_str} → zarar ≈ -{loss_at_sl:.0f} USDT\n"
        f"🎯 TP: {tp_str} → kâr ≈ +{gain_at_tp:.0f} USDT\n"
        f"(komisyon hariç; giriş+çıkış ≈ {notional_real * 0.001:.0f} USDT)"
        + tp_note
    )
    return True, msg


def status_line():
    """Test mesajı için bağlantı ve hesap özeti."""
    try:
        avail = _available_usdt()
        pos = _positions()
        mode = "hedge" if _hedge_mode() else "one-way"
        names = ", ".join(p["symbol"] for p in pos) or "yok"
        return (f"✅ Binance{' DEMO' if IS_DEMO else ''} bağlantısı OK – kullanılabilir {avail:,.0f} USDT, "
                f"{len(pos)} açık pozisyon ({names}), mod: {mode}")
    except Exception as e:
        return f"❌ Binance bağlantısı başarısız: {e}"
