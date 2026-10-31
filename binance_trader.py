"""
Binance USDⓈ-M Futures otomatik işlem modülü (varsayılan: DEMO hesap).

Stratejiden bağımsızdır: giriş, stop ve çıkış ayarları sinyalle birlikte strateji dosyasından
(strateji_<kod>.py → CIKIS) gelir. Bu dosyada sadece risk ve hesap ayarları vardır.

Sinyal gelince `open_trade(...)`:
  1. Aynı coinde pozisyon varsa ya da pozisyon / yön sınırı doluysa işlem açılmaz.
  2. Büyüklük: SL'ye gelirse MARGIN_USDT × LOSS_PCT_OF_MARGIN kadar zarar (1R).
       notional = 1R / (|giriş − SL| / giriş),  kaldıraç = yukarı yuvarla(notional / MARGIN_USDT)
  3. ISOLATED marjin, piyasa emriyle giriş; SL her zaman borsada STOP_MARKET olarak durur.
     SL konamazsa pozisyon güvenlik için hemen kapatılır.
  4. Stratejide varsa: TP emri, yarı kâr emri (sonra SL girişe çekilir).
Her çalıştırmada `manage_positions(...)`: yarı kâr → SL girişe, iz süren çıkış (stratejinin
iz_cikis kuralı, her mum kapanışında), süre sınırı, kapanış bildirimi ve rapor kaydı.

Ortam değişkenleri: BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_BASE_URL (varsayılan demo)
"""
import datetime
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
# AYARLAR (risk ve hesap – stratejiye özel ayarlar strateji dosyasında)
# =========================================================================
BASE_URL = os.environ.get("BINANCE_BASE_URL", "https://demo-fapi.binance.com").strip().rstrip("/")
API_KEY = os.environ.get("BINANCE_API_KEY", "").strip()
API_SECRET = os.environ.get("BINANCE_API_SECRET", "").strip()

MARGIN_USDT = 1000.0          # her işlemde kullanılacak teminat
LOSS_PCT_OF_MARGIN = 0.10     # SL'de teminatın %10'u kaybedilir (1000 $ → 100 $ = 1R)
MAX_OPEN_POSITIONS = 4        # aynı anda en fazla kaç pozisyon (tüm stratejiler toplamı)
MAX_SAME_DIRECTION = 2        # aynı yönde en fazla kaç pozisyon – coinler BTC ile birlikte hareket eder
MAX_LEVERAGE = 50             # coinin izni daha yüksek olsa bile bu sınırı aşma
MAX_PRICE_DIFF_PCT = 1.0      # Gate sinyal fiyatı ile Binance fiyatı bundan fazla farklıysa işlem açma
TRIM_TOLERANCE = 0.02         # gerçek SL zararı hedefi %2'den fazla aşarsa fazlası kapatılır
PARTIAL_SIZE = 0.5            # yarı kârda pozisyonun ne kadarı kapanır (strateji yari_kar tanımladıysa)
AFTER_PARTIAL_MULT = 2        # yarı kârdan sonra süre sınırı × 2 (strateji sure_mum tanımladıysa)
TF_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400,
              "8h": 28800, "1d": 86400}

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


def _last_close_price(symbol, start_ms, close_side):
    """Pozisyonu kapatan son işlemin fiyatı (çıkış türünü belirlemek için)."""
    try:
        rows = _request("GET", "/fapi/v1/userTrades", {"symbol": symbol, "startTime": int(start_ms), "limit": 100})
        rows = [r for r in rows if r.get("side") == close_side]
        return float(rows[-1]["price"]) if rows else None
    except Exception:
        return None


def _plan_cikis(plan):
    """Planın çıkış ayarları. Eski sürümün planları (cikis alanı olmayan) için uyumlu karşılık."""
    if plan.get("cikis"):
        return plan["cikis"]
    if plan.get("exit_mode") == "trend":                     # trend sürümü (strateji alanı yoktu)
        return {"iz": True}
    return {"sure_mum": {"15m": 16, "1h": 24}.get(plan.get("tf") or "1h", 24)}   # ilk sürüm (TP + yarı kâr)


def _plan_strateji(plan):
    if plan.get("strateji"):
        return plan["strateji"]
    return "trend_4h" if plan.get("exit_mode") == "trend" else "eski"


def _record_close(symbol, plan, net, exit_type, notify):
    """Kapanan işlemi rapor kaydına yazar; çıkış türü verilmediyse (SL/TP/GİRİŞ/ELLE) tahmin eder."""
    entry = plan["entry"]
    sl = plan.get("sl") or float(plan["sl_str"])
    tp = float(plan["tp_str"]) if plan.get("tp_str") else None
    risk = plan.get("risk_usdt") or plan["qty"] * abs(entry - sl)
    partial = bool(plan.get("be_done") and plan.get("half", 0) > 0)
    if exit_type is None:
        close_side = "SELL" if plan["side"] == "AL" else "BUY"
        px = _last_close_price(symbol, plan["opened_ms"] - 5000, close_side)
        tol = 0.15 * abs(entry - sl)
        if px is None:
            exit_type = "?"
        elif tp is not None and abs(px - tp) <= tol:
            exit_type = "TP"
        elif partial and abs(px - entry) <= tol:
            exit_type = "GİRİŞ"
        elif not partial and abs(px - sl) <= tol:
            exit_type = "SL"
        else:
            exit_type = "ELLE"
    try:
        import rapor
        now = datetime.datetime.now(rapor.TZ)
        opened = datetime.datetime.fromtimestamp(plan["opened_ms"] / 1000, rapor.TZ)
        rapor.record({
            "symbol": symbol, "side": plan["side"], "tf": plan.get("tf"), "strateji": _plan_strateji(plan),
            "opened": opened.isoformat(timespec="seconds"), "closed": now.isoformat(timespec="seconds"),
            "hours": round((now - opened).total_seconds() / 3600, 2),
            "entry": entry, "sl": sl, "tp": tp, "risk_usdt": round(risk, 2),
            "net": None if net is None else round(net, 2),
            "r": None if (net is None or not risk) else round(net / risk, 3),
            "partial": partial, "exit": exit_type,
        })
        rapor.check_stop_alerts(notify)
    except Exception as e:
        print(f"  {symbol}: rapor kaydı yazılamadı ({e})")
    return exit_type


WATCH_FILE = "izlenen_pozisyonlar.json"


def _watch_untracked(rows, plans, notify):
    """
    Botun planı olmayan pozisyonları da izler (eski pozisyonlar, elle açılanlar):
    kapandıklarında Telegram'a sonucu bildirir.
    """
    try:
        with open(WATCH_FILE) as f:
            watch = json.load(f)
    except Exception:
        watch = {}
    now_ms = int(time.time() * 1000)
    current = {p["symbol"]: p for p in rows}

    for symbol, info in list(watch.items()):
        if symbol in current:
            continue
        # Planlı pozisyonların kapanış bildirimini plan yönetimi yapıyor – burada tekrar bildirme
        if symbol not in plans and not info.get("planned"):
            net = _realized_since(symbol, info["since_ms"] - 5000)
            side = info.get("side", "")
            if net is None:
                res = "sonuç okunamadı – Binance'te Trade History'ye bak"
            else:
                res = f"net ≈ {'+' if net >= 0 else ''}{net:.0f} USDT"
                if info.get("seen_late"):
                    res += " (izlemeye sonradan alındı: açılış komisyonu hariç olabilir)"
            icon = "✅" if (net or 0) > 0 else "🔻"
            notify(f"{icon} {symbol} {side} pozisyonu kapandı – {res}")
            _log("untracked_close", symbol, net=None if net is None else round(net, 2))
            _cancel_all(symbol)
        del watch[symbol]

    first_run = not os.path.exists(WATCH_FILE)
    for symbol, p in current.items():
        if symbol not in watch:
            amt = float(p["positionAmt"])
            watch[symbol] = {"since_ms": now_ms, "side": "LONG" if amt > 0 else "SHORT",
                             "seen_late": first_run or symbol not in plans}
        if symbol in plans:
            watch[symbol]["planned"] = True

    tmp = WATCH_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(watch, f, indent=1)
    os.replace(tmp, WATCH_FILE)


def _iz_cikis_hit(symbol, plan):
    """
    İz süren çıkış: işlemin zaman diliminde YENİ kapanan mumda stratejinin iz_cikis() kuralı oluştu mu?
    Her mum için bir kez kontrol eder (plan["exit_checked"]). Dönüş: açıklama metni ya da None.
    """
    import pandas as pd
    import signal_bot as sb                     # indikatörler ve strateji kuralı girişteki ile aynı
    strat = sb.load_strategy(_plan_strateji(plan))
    tf = plan.get("tf") or strat.ZAMAN_DILIMI
    sec = TF_SECONDS.get(tf, 3600)
    last_open = int(time.time()) // sec * sec - sec           # son kapanmış mumun açılışı
    if plan.get("exit_checked", 0) >= last_open:
        return None
    if last_open + sec <= plan["opened_ms"] / 1000:          # mum, işlem açılmadan önce kapanmış
        plan["exit_checked"] = last_open
        return None
    rows = _request("GET", "/fapi/v1/klines", {"symbol": symbol, "interval": tf, "limit": 200}, signed=False)
    df = pd.DataFrame([[int(r[0]) // 1000, float(r[1]), float(r[2]), float(r[3]), float(r[4])] for r in rows],
                      columns=["ts", "open", "high", "low", "close"])
    hit = df.index[df["ts"] == last_open]
    if len(df) < 40 or not len(hit):
        return None                                           # mum henüz gelmedi: sonraki çalıştırmada tekrar
    plan["exit_checked"] = last_open
    sb.compute_indicators(df)
    strat.hazirla(df)
    return strat.iz_cikis(df, int(hit[0]), plan["side"])


def _close_now(symbol, plan, amt, close_side, notify, exit_type, text):
    """Pozisyonu piyasa fiyatından kapatır, bildirir ve kaydeder. Dönüş: kapandı mı."""
    amt_str = f"{amt:.{_decimals(plan['step'])}f}"
    try:
        _market_close(symbol, close_side, amt_str, plan["pos_side"], plan["hedge"])
    except Exception as e:
        if not plan.get("close_error"):
            notify(f"❌ {symbol}: {exit_type} çıkışı gerekiyordu ama pozisyon kapatılamadı – ELLE KONTROL ET! ({e})")
            plan["close_error"] = True
        return False
    time.sleep(1.5)
    _cancel_all(symbol)
    net = _realized_since(symbol, plan["opened_ms"] - 5000)
    res = "sonuç okunamadı" if net is None else f"net {'+' if net >= 0 else ''}{net:.0f} USDT (komisyon dahil)"
    if net is not None and plan.get("risk_usdt"):
        res += f" ({net / plan['risk_usdt']:+.2f}R)"
    hours = (time.time() - plan["opened_ms"] / 1000) / 3600
    sure = f"{hours / 24:.1f} gün" if hours >= 48 else f"{hours:.1f} saat"
    yon = "LONG" if plan["side"] == "AL" else "SHORT"
    notify(f"{text}, {yon} kapatıldı ({sure}) – {res}")
    _record_close(symbol, plan, net, exit_type, notify)
    return True


def manage_positions(notify):
    """
    Her çalıştırmada (dakikada bir) çağrılır:
      - Pozisyon kapandıysa sonucu bildirir, rapora yazar, planı siler.
      - Yarı kâr gerçekleştiyse SL'yi girişe çeker.
      - İz süren çıkış (strateji iz_cikis) ve süre sınırı.
    """
    if not API_KEY:
        return
    plans = _load_plans()
    try:
        rows = _positions()
    except Exception as e:
        print(f"  pozisyon yönetimi: pozisyonlar okunamadı ({e})")
        return
    amounts = {p["symbol"]: abs(float(p["positionAmt"])) for p in rows}
    _watch_untracked(rows, plans, notify)

    changed = False
    for symbol, plan in list(plans.items()):
        amt = amounts.get(symbol, 0.0)
        close_side = "SELL" if plan["side"] == "AL" else "BUY"
        cikis = _plan_cikis(plan)

        # 1) Pozisyon tamamen kapanmış (SL / TP borsada tetiklendi ya da elle kapatıldı)
        if amt == 0:
            net = _realized_since(symbol, plan["opened_ms"] - 5000)
            _cancel_all(symbol)
            res = ("sonuç okunamadı – Binance'te Trade History'ye bak" if net is None
                   else f"net {'+' if net >= 0 else ''}{net:.0f} USDT (komisyon dahil)")
            exit_type = _record_close(symbol, plan, net, None, notify)
            notify(f"{'✅' if (net or 0) > 0 else '🔻'} {symbol} pozisyonu kapandı ({exit_type}) – {res}")
            del plans[symbol]
            changed = True
            continue

        # 2) Yarı kâr gerçekleşti mi? (kalan miktar ilk miktardan yarı kadar azaldıysa) → SL girişe
        if not plan["be_done"] and plan.get("half", 0) > 0 and amt <= plan["qty"] - 0.9 * plan["half"]:
            tick, entry = plan["tick"], plan["entry"]
            be = (math.ceil(entry / tick) if plan["side"] == "AL" else math.floor(entry / tick)) * tick
            be_str = f"{be:.{_decimals(tick)}f}"
            _cancel_order_ref(symbol, plan["sl_ref"])
            try:
                plan["sl_ref"] = list(_place_close_order(symbol, close_side, "STOP_MARKET", be_str, plan["pos_side"]))
                profit = plan["half"] * abs(plan["mid"] - entry)
                _log("partial", symbol, profit=round(profit, 2))
                notify(f"🔸 {symbol}: yarı kâr alındı (≈ +{profit:.0f} USDT). SL giriş fiyatına çekildi ({be_str}), "
                       f"kalan yarı artık risksiz." + (f" TP {plan['tp_str']} bekleniyor." if plan.get("tp_str") else ""))
            except Exception as e:
                try:      # SL girişe konamadıysa (fiyat zaten girişin ötesine döndü) kalanı kapat
                    _market_close(symbol, close_side, f"{amt:.{_decimals(plan['step'])}f}",
                                  plan["pos_side"], plan["hedge"])
                    notify(f"🔸 {symbol}: yarısı kârla kapandı; SL girişe konamadı ({e}), kalan piyasa fiyatından kapatıldı.")
                except Exception as e2:
                    notify(f"❌ {symbol}: SL girişe konamadı ve kalan kapatılamadı – ELLE KONTROL ET! ({e2})")
            plan["be_done"] = True
            changed = True
            continue
        partial_taken = plan["be_done"] and plan.get("half", 0) > 0

        # 3) İz süren çıkış: stratejinin kuralı, her yeni mum kapanışında bir kez
        if cikis.get("iz"):
            before = plan.get("exit_checked")
            try:
                why = _iz_cikis_hit(symbol, plan)
            except Exception as e:
                print(f"  {symbol}: iz süren çıkış kontrolü yapılamadı ({e})")
                why = None
            if plan.get("exit_checked") != before:
                changed = True
            if why:
                if _close_now(symbol, plan, amt, close_side, notify, "İZ", f"🏁 {symbol}: çıkış sinyali – {why}"):
                    del plans[symbol]
                else:
                    plan["exit_checked"] = before or 0             # sonraki çalıştırmada tekrar dene
                changed = True
                continue

        # 4) Süre sınırı
        if cikis.get("sure_mum"):
            tf = plan.get("tf") or "1h"
            limit_s = cikis["sure_mum"] * TF_SECONDS.get(tf, 3600) * (AFTER_PARTIAL_MULT if partial_taken else 1)
            if time.time() - plan["opened_ms"] / 1000 >= limit_s:
                text = (f"⏰ {symbol}: kalan yarı süre sınırında TP'ye ulaşamadı" if partial_taken
                        else f"⏰ {symbol}: {cikis['sure_mum']} mumda ({tf}) hedefe ulaşamadı")
                if _close_now(symbol, plan, amt, close_side, notify, "SÜRE", text):
                    del plans[symbol]
                changed = True

    if changed:
        _save_plans(plans)


# =========================================================================
# İşlem açma
# =========================================================================
def open_trade(gate_symbol, alert, timeframe=None):
    """
    alert: signal_bot.check_alert sonucu (side, entry, stop, target, cikis, strateji ...).
    Dönüş: (açıldı_mı: bool, Telegram mesajı: str)
    """
    side = alert["side"]                        # "AL" -> LONG, "SAT" -> SHORT
    cikis = alert.get("cikis") or {}
    symbol, mult = map_symbol(gate_symbol)
    tick, step, min_qty, max_qty, min_notional = _filters(symbol)
    sl = alert["stop"] * mult
    tp = alert["target"] * mult if alert.get("target") else None

    # 1) Açık pozisyon kontrolleri
    positions = _positions()
    if any(p["symbol"] == symbol for p in positions):
        return False, f"⏭ {symbol}: zaten açık pozisyon var, yeni işlem açılmadı."
    if len(positions) >= MAX_OPEN_POSITIONS:
        return False, f"⏭ {symbol}: {len(positions)} açık pozisyon var (sınır {MAX_OPEN_POSITIONS}), işlem açılmadı."
    want_long = side == "AL"
    same = [p["symbol"] for p in positions if (float(p["positionAmt"]) > 0) == want_long]
    if MAX_SAME_DIRECTION and len(same) >= MAX_SAME_DIRECTION:
        yon = "LONG" if want_long else "SHORT"
        return False, (f"⏭ {symbol}: aynı yönde {len(same)} {yon} var ({', '.join(same)}; "
                       f"yön sınırı {MAX_SAME_DIRECTION}), işlem açılmadı.")
    avail = _available_usdt()
    if avail < MARGIN_USDT * 1.05:
        return False, f"⏭ {symbol}: kullanılabilir bakiye yetersiz ({avail:.0f} USDT)."

    # 2) Güncel fiyat kontrolü
    mark = float(_request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol}, signed=False)["markPrice"])
    gate_entry = alert["entry"] * mult
    if abs(mark - gate_entry) / gate_entry * 100 > MAX_PRICE_DIFF_PCT:
        return False, (f"⏭ {symbol}: fiyat sinyalden çok uzaklaşmış (sinyal {gate_entry:.6g}, "
                       f"şimdi {mark:.6g}), işlem açılmadı.")
    lo, hi = (sl, tp) if side == "AL" else (tp, sl)
    if not ((lo is None or lo < mark) and (hi is None or mark < hi)):
        return False, f"⏭ {symbol}: fiyat SL/TP aralığının dışında, işlem açılmadı."

    # 3) Büyüklük ve kaldıraç – piyasa emri en iyi alış/satış fiyatından dolacağı için onu kullan
    px = _book_price(symbol, side) or mark
    target_loss = MARGIN_USDT * LOSS_PCT_OF_MARGIN
    notional = target_loss / (abs(px - sl) / px)
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
    open_side, close_side = ("BUY", "SELL") if side == "AL" else ("SELL", "BUY")
    pos_side = ("LONG" if side == "AL" else "SHORT") if hedge else "BOTH"

    # 5) Giriş
    _cancel_all(symbol)                               # eski emir kalmasın
    order = _request("POST", "/fapi/v1/order", {
        "symbol": symbol, "side": open_side, "type": "MARKET", "quantity": _floor_qty(qty, step),
        "positionSide": pos_side, "newOrderRespType": "RESULT"})
    fill, filled_qty = _real_fill(symbol, order, qty, px)

    # 6) SL – konamazsa pozisyonu hemen kapat
    sl_str = _round_price(sl, tick)
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
    notes = []
    excess = float(_floor_qty(filled_qty - target_loss / abs(fill - float(sl_str)), step))
    if filled_qty * abs(fill - float(sl_str)) > target_loss * (1 + TRIM_TOLERANCE) and excess >= min_qty:
        try:
            _market_close(symbol, close_side, _floor_qty(excess, step), pos_side, hedge)
            filled_qty = float(_floor_qty(filled_qty - excess, step))
            notes.append(f"(giriş kayması nedeniyle {excess:g} adet hemen kapatıldı, SL zararı ~{target_loss:.0f} USDT)")
        except Exception as e:
            notes.append(f"⚠️ Fazla miktar kapatılamadı ({e})")

    # 8) TP ve yarı kâr (strateji tanımladıysa)
    tp_str, mid_str, half = None, None, 0.0
    if tp is not None:
        tp_str = _round_price(tp, tick)
        try:
            _place_close_order(symbol, close_side, "TAKE_PROFIT_MARKET", tp_str, pos_side)
        except Exception as e:
            notes.append(f"⚠️ TP emri konamadı ({e}) – TP'yi elle koy.")
        if cikis.get("yari_kar"):
            mid_str = _round_price(fill + (float(tp_str) - fill) * cikis["yari_kar"], tick)
            half = float(_floor_qty(filled_qty * PARTIAL_SIZE, step))
            if half >= min_qty and filled_qty - half >= min_qty and half * float(mid_str) >= min_notional:
                try:
                    _place_close_order(symbol, close_side, "TAKE_PROFIT_MARKET", mid_str, pos_side,
                                       qty=_floor_qty(half, step), hedge=hedge)
                    notes.append(f"🔸 Yarı kâr: {mid_str} → %{PARTIAL_SIZE * 100:.0f} kapanır "
                                 f"≈ +{half * abs(float(mid_str) - fill):.0f} USDT, SL girişe çekilir")
                except Exception as e:
                    half = 0.0
                    notes.append(f"⚠️ Yarı kâr emri konamadı ({e})")
            else:
                half = 0.0
                notes.append("(miktar çok küçük olduğu için yarı kâr emri konmadı)")

    plans = _load_plans()
    plans[symbol] = {
        "side": side, "entry": fill, "sl_str": sl_str, "tp_str": tp_str,
        "mid": float(mid_str) if mid_str else None, "qty": filled_qty, "half": half, "be_done": half == 0,
        "sl_ref": list(sl_ref), "pos_side": pos_side, "hedge": hedge, "tick": tick, "step": step,
        "opened_ms": int(time.time() * 1000), "tf": timeframe, "sl": float(sl_str),
        "risk_usdt": filled_qty * abs(fill - float(sl_str)),
        "strateji": alert.get("strateji"), "cikis": cikis,
    }
    _save_plans(plans)

    notional_real = filled_qty * fill
    lines = [
        f"{'🟩 LONG' if side == 'AL' else '🟥 SHORT'} açıldı – {symbol}{' (DEMO)' if IS_DEMO else ''}",
        f"🏆 {alert.get('strateji_ad') or alert.get('strateji') or '-'}",
        f"Giriş: {fill:.{_decimals(tick)}f} | Miktar: {filled_qty:,.{_decimals(step)}f}",
        f"Kaldıraç: {leverage}x ISOLATED | Teminat: ~{notional_real / leverage:.0f} USDT | Pozisyon: {notional_real:,.0f} USDT",
        f"🛑 SL: {sl_str} → zarar ≈ -{notional_real * abs(fill - float(sl_str)) / fill:.0f} USDT",
        (f"🎯 TP: {tp_str} → kâr ≈ +{notional_real * abs(float(tp_str) - fill) / fill:.0f} USDT" if tp_str
         else "🎯 TP yok"),
    ]
    if cikis.get("iz") and alert.get("cikis_metni"):
        lines.append(f"🚪 Çıkış: {alert['cikis_metni']}")
    if cikis.get("sure_mum"):
        lines.append(f"⏰ Süre sınırı: {cikis['sure_mum']} mum ({timeframe})")
    lines.append(f"(komisyon hariç; giriş+çıkış ≈ {notional_real * 0.001:.0f} USDT)")
    return True, "\n".join(lines + notes)


# =========================================================================
# Rapor yardımcıları
# =========================================================================
def _income_between(start_ms, end_ms):
    out, cur = [], int(start_ms)
    for _ in range(20):
        rows = _request("GET", "/fapi/v1/income", {"startTime": cur, "endTime": int(end_ms), "limit": 1000})
        out += rows
        if len(rows) < 1000:
            break
        cur = int(rows[-1]["time"]) + 1
    return out


def daily_numbers(start_ms, end_ms):
    """Günlük rapor için: günün gerçekleşen K/Z, komisyon, funding; bakiye; açık pozisyonlar."""
    parts = {"REALIZED_PNL": 0.0, "COMMISSION": 0.0, "FUNDING_FEE": 0.0}
    for r in _income_between(start_ms, end_ms):
        if r.get("incomeType") in parts:
            parts[r["incomeType"]] += float(r["income"])
    try:
        acct = _request("GET", "/fapi/v3/account")
        wallet = float(acct["totalWalletBalance"])
        upnl = float(acct["totalUnrealizedProfit"])
    except Exception:
        wallet = next((float(a["balance"]) for a in _request("GET", "/fapi/v3/balance") if a["asset"] == "USDT"), 0.0)
        upnl = 0.0
    plans = _load_plans()
    try:
        with open(WATCH_FILE) as f:
            watch = json.load(f)
    except Exception:
        watch = {}
    now = time.time()
    positions = []
    for p in _positions():
        amt = float(p["positionAmt"])
        opened = (plans.get(p["symbol"], {}).get("opened_ms") or watch.get(p["symbol"], {}).get("since_ms"))
        positions.append({"symbol": p["symbol"], "side": "LONG" if amt > 0 else "SHORT",
                          "upnl": float(p.get("unRealizedProfit") or 0),
                          "hours": (now - opened / 1000) / 3600 if opened else None})
    if not upnl:
        upnl = sum(x["upnl"] for x in positions)
    return {"realized": parts["REALIZED_PNL"], "commission": parts["COMMISSION"],
            "funding": parts["FUNDING_FEE"], "wallet": wallet, "upnl": upnl, "positions": positions}


def _log(kind, symbol=None, **data):
    try:
        import rapor
        rapor.log_event(kind, symbol, **data)
    except Exception as e:
        print(f"  olay kaydedilemedi ({e})")


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
