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
TRIM_TOLERANCE = 0.02         # gerçek SL zararı hedefi %2'den fazla aşarsa fazlası kapatılır
PARTIAL_AT = 0.5              # giriş→TP mesafesinin bu oranında (yarısında) kısmi kâr alınır
PARTIAL_SIZE = 0.5            # kısmi kârda pozisyonun ne kadarı kapanır (%50)

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
# Fiyat ve gerçek dolum
# =========================================================================
def _book_price(symbol, side):
    """LONG için en iyi satış (ask), SHORT için en iyi alış (bid) fiyatı."""
    try:
        t = _request("GET", "/fapi/v1/ticker/bookTicker", {"symbol": symbol}, signed=False)
        return float(t["askPrice"] if side == "AL" else t["bidPrice"]) or None
    except Exception:
        return None


def _real_fill(symbol, order, qty, fallback_price):
    """Piyasa emrinin gerçek ortalama fiyatı ve miktarı. Yanıtta yoksa emri ve pozisyonu sorgular."""
    price = float(order.get("avgPrice") or 0)
    filled = float(order.get("executedQty") or 0)
    order_id = order.get("orderId")
    for _ in range(4):
        if price > 0 and filled > 0:
            return price, filled
        time.sleep(0.5)
        try:
            if order_id is not None:
                o = _request("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
                price = float(o.get("avgPrice") or 0) or price
                filled = float(o.get("executedQty") or 0) or filled
        except Exception:
            pass
    if price <= 0 or filled <= 0:
        try:
            for p in _request("GET", "/fapi/v3/positionRisk", {"symbol": symbol}):
                if abs(float(p["positionAmt"])) > 0:
                    price = price or float(p["entryPrice"])
                    filled = filled or abs(float(p["positionAmt"]))
        except Exception:
            pass
    return (price if price > 0 else fallback_price), (filled if filled > 0 else qty)


# =========================================================================
# SL / TP emirleri (önce yeni Algo Order API, olmazsa eski uç nokta)
# =========================================================================
def _place_close_order(symbol, close_side, order_type, trigger, pos_side, qty=None, hedge=False):
    """
    Koşullu kapatma emri. qty=None ise pozisyonun TAMAMINI kapatır (closePosition),
    qty verilirse sadece o kadarını kapatır (kısmi kâr al).
    Dönüş: ("algo", algoId) ya da ("legacy", orderId) – emri sonra iptal edebilmek için.
    """
    params = {"algoType": "CONDITIONAL", "symbol": symbol, "side": close_side, "type": order_type,
              "triggerPrice": trigger, "workingType": "MARK_PRICE", "positionSide": pos_side}
    if qty is None:
        params["closePosition"] = "true"
    else:
        params["quantity"] = qty
        if not hedge:
            params["reduceOnly"] = "true"
    try:
        r = _request("POST", "/fapi/v1/algoOrder", params)
        return ("algo", r.get("algoId"))
    except BinanceError as e:
        if e.status in (404, 405) or e.code in (-1102, -1104, -1116, -1000, -5000):
            legacy = {k: v for k, v in params.items() if k not in ("algoType", "triggerPrice")}
            legacy["stopPrice"] = trigger
            r = _request("POST", "/fapi/v1/order", legacy)
            return ("legacy", r.get("orderId"))
        raise


def _cancel_order_ref(symbol, ref):
    kind, oid = ref
    if oid is None:
        return
    try:
        if kind == "algo":
            _request("DELETE", "/fapi/v1/algoOrder", {"algoId": oid})
        else:
            _request("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": oid})
    except BinanceError as e:
        print(f"  {symbol}: eski emir iptal edilemedi ({e}) – zaten tetiklenmiş/iptal olmuş olabilir")


def _market_close(symbol, close_side, qty_str, pos_side, hedge):
    close = {"symbol": symbol, "side": close_side, "type": "MARKET", "quantity": qty_str,
             "positionSide": pos_side}
    if not hedge:
        close["reduceOnly"] = "true"
    return _request("POST", "/fapi/v1/order", close)


# =========================================================================
# İşlem planları (yarı kâr + SL'yi girişe çekme için)
# =========================================================================
PLANS_FILE = "pozisyonlar.json"


def _load_plans():
    try:
        with open(PLANS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_plans(plans):
    tmp = PLANS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(plans, f, indent=1)
    os.replace(tmp, PLANS_FILE)


def _realized_since(symbol, start_ms):
    """Pozisyon açıldığından beri gerçekleşen net sonuç (kâr/zarar + komisyon + funding)."""
    try:
        rows = _request("GET", "/fapi/v1/income", {"symbol": symbol, "startTime": int(start_ms), "limit": 1000})
        return sum(float(r["income"]) for r in rows
                   if r.get("incomeType") in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE"))
    except Exception:
        return None


def manage_positions(notify):
    """
    Her çalıştırmada (dakikada bir) çağrılır:
      - Yarı kâr emri gerçekleştiyse eski SL'yi iptal edip SL'yi giriş fiyatına çeker.
      - Pozisyon tamamen kapandıysa sonucu Telegram'a bildirir ve planı siler.
    """
    if not API_KEY:
        return
    plans = _load_plans()
    if not plans:
        return
    try:
        amounts = {p["symbol"]: abs(float(p["positionAmt"])) for p in _positions()}
    except Exception as e:
        print(f"  pozisyon yönetimi: pozisyonlar okunamadı ({e})")
        return

    changed = False
    for symbol, plan in list(plans.items()):
        amt = amounts.get(symbol, 0.0)
        close_side = "SELL" if plan["side"] == "AL" else "BUY"

        # Pozisyon tamamen kapanmış
        if amt == 0:
            net = _realized_since(symbol, plan["opened_ms"] - 5000)
            _cancel_all(symbol)
            if net is None:
                res = "sonuç okunamadı – Binance'te Trade History'ye bak"
            else:
                res = f"net {'+' if net >= 0 else ''}{net:.0f} USDT (komisyon dahil)"
            icon = "✅" if (net or 0) > 0 else "🔻"
            notify(f"{icon} {symbol} pozisyonu kapandı – {res}")
            del plans[symbol]
            changed = True
            continue

        # Yarı kâr gerçekleşti mi? (kalan miktar, ilk miktardan yarı kadar azalmışsa)
        if not plan["be_done"] and plan.get("half", 0) > 0 and amt <= plan["qty"] - 0.9 * plan["half"]:
            tick = plan["tick"]
            entry = plan["entry"]
            # Giriş fiyatı, kâr tarafına yuvarlanır (LONG: yukarı, SHORT: aşağı)
            be = (math.ceil(entry / tick) if plan["side"] == "AL" else math.floor(entry / tick)) * tick
            be_str = f"{be:.{_decimals(tick)}f}"
            amt_str = f"{amt:.{_decimals(plan['step'])}f}"
            _cancel_order_ref(symbol, plan["sl_ref"])
            try:
                plan["sl_ref"] = list(_place_close_order(symbol, close_side, "STOP_MARKET", be_str,
                                                         plan["pos_side"]))
                profit = plan["half"] * abs(plan["mid"] - entry)
                notify(f"🔸 {symbol}: hedefin yarısına gelindi – pozisyonun yarısı kapandı "
                       f"(≈ +{profit:.0f} USDT).\nSL giriş fiyatına çekildi ({be_str}): kalan yarı artık risksiz. "
                       f"TP {plan['tp_str']} bekleniyor.")
            except Exception as e:
                # SL girişe konamadıysa (ör. fiyat zaten girişin altına döndü) kalanı hemen kapat
                try:
                    _market_close(symbol, close_side, amt_str, plan["pos_side"], plan["hedge"])
                    notify(f"🔸 {symbol}: yarısı kârla kapandı; SL girişe konamadı ({e}), "
                           f"kalan pozisyon piyasa fiyatından kapatıldı.")
                except Exception as e2:
                    notify(f"❌ {symbol}: SL girişe konamadı ve kalan pozisyon kapatılamadı – ELLE KONTROL ET! ({e2})")
            plan["be_done"] = True
            changed = True

    if changed:
        _save_plans(plans)


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

    # 3) Büyüklük ve kaldıraç – piyasa emri en iyi alış/satış fiyatından dolacağı için onu kullan
    px = _book_price(symbol, side) or mark
    stop_frac = abs(px - sl) / px
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

    qty = float(_floor_qty(min(notional / px, max_qty), step))
    if qty < min_qty or qty * px < min_notional:
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
    fill, filled_qty = _real_fill(symbol, order, qty, px)

    # 6) SL ve TP – SL konamazsa pozisyonu hemen kapat
    sl_str, tp_str = _round_price(sl, tick), _round_price(tp, tick)
    try:
        sl_ref = _place_close_order(symbol, close_side, "STOP_MARKET", sl_str, pos_side)
    except Exception as e:
        try:
            _market_close(symbol, close_side, _floor_qty(filled_qty, step), pos_side, hedge)
            note = "pozisyon güvenlik için hemen kapatıldı"
        except Exception as e2:
            note = f"POZİSYON KAPATILAMADI, ELLE KAPAT! ({e2})"
        return False, f"❌ {symbol}: SL emri konamadı ({e}) – {note}."

    # 7) Gerçek dolum fiyatıyla SL zararı hedefi aşıyorsa fazla miktarı hemen kapat
    trim_note = ""
    max_qty_for_target = target_loss / abs(fill - float(sl_str))
    excess = float(_floor_qty(filled_qty - max_qty_for_target, step))
    if filled_qty * abs(fill - float(sl_str)) > target_loss * (1 + TRIM_TOLERANCE) and excess >= min_qty:
        trim = {"symbol": symbol, "side": close_side, "type": "MARKET",
                "quantity": _floor_qty(excess, step), "positionSide": pos_side}
        if not hedge:
            trim["reduceOnly"] = "true"
        try:
            _request("POST", "/fapi/v1/order", trim)
            filled_qty = float(_floor_qty(filled_qty - excess, step))
            trim_note = f"\n(giriş kayması nedeniyle {excess:g} adet hemen kapatıldı, SL zararı ~{target_loss:.0f} USDT'ye çekildi)"
        except Exception as e:
            trim_note = f"\n⚠️ Fazla miktar kapatılamadı ({e})"

    tp_note = ""
    try:
        _place_close_order(symbol, close_side, "TAKE_PROFIT_MARKET", tp_str, pos_side)
    except Exception as e:
        tp_note = f"\n⚠️ TP emri konamadı ({e}) – TP'yi elle koy."

    # 8) Yarı kâr: hedeflenen hareketin yarısında pozisyonun %50'si kapanır
    mid = fill + (float(tp_str) - fill) * PARTIAL_AT
    mid_str = _round_price(mid, tick)
    half = float(_floor_qty(filled_qty * PARTIAL_SIZE, step))
    partial_note = ""
    if half >= min_qty and filled_qty - half >= min_qty and half * float(mid_str) >= min_notional:
        try:
            _place_close_order(symbol, close_side, "TAKE_PROFIT_MARKET", mid_str, pos_side,
                               qty=_floor_qty(half, step), hedge=hedge)
            partial_note = (f"\n🔸 Yarı kâr: {mid_str} → {half:,.{_decimals(step)}f} adet (%{PARTIAL_SIZE * 100:.0f}) "
                            f"kapanır ≈ +{half * abs(float(mid_str) - fill):.0f} USDT, SL girişe çekilir")
        except Exception as e:
            half = 0
            partial_note = f"\n⚠️ Yarı kâr emri konamadı ({e}) – sadece SL/TP aktif."
    else:
        half = 0
        partial_note = "\n(miktar çok küçük olduğu için yarı kâr emri konmadı)"

    plans = _load_plans()
    plans[symbol] = {
        "side": side, "entry": fill, "sl_str": sl_str, "tp_str": tp_str, "mid": float(mid_str),
        "qty": filled_qty, "half": half, "be_done": half == 0, "sl_ref": list(sl_ref),
        "pos_side": pos_side, "hedge": hedge, "tick": tick, "step": step,
        "opened_ms": int(time.time() * 1000),
    }
    _save_plans(plans)

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
        + partial_note + trim_note + tp_note
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
