"""
İşlem kaydı ve performans raporu.

- Her kapanan işlem `islemler.jsonl` dosyasına bir satır olarak yazılır (sunucuda /opt/sinyalbot).
- Her pazar 21:00'den sonraki ilk çalıştırmada Telegram'a haftalık rapor gider.
- Durma noktası (düşüş / kayıp serisi) aşılırsa bir kez uyarı gönderilir. Bot durdurulmaz, karar senin.

Elle rapor: sunucuda  sinyalbot-rapor
"""
import datetime
import json
import os
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Istanbul")
JOURNAL_FILE = "islemler.jsonl"
STATUS_FILE = "rapor_durum.json"

REPORT_WEEKDAY = 6            # 0 = pazartesi ... 6 = pazar
REPORT_HOUR = 21              # bu saatten sonraki ilk çalıştırmada gönderilir
MIN_RELIABLE_TRADES = 50      # bunun altında "sonuçlar henüz güvenilir değil" notu
STOP_DRAWDOWN_USDT = 1250     # test başından beri zirveden bu kadar düşüş → uyarı (5000'in %25'i)
STOP_LOSING_STREAK = 8        # art arda bu kadar kayıp → uyarı


# =========================================================================
# Kayıt
# =========================================================================
def record(trade):
    """trade: dict (symbol, side, tf, opened, closed, hours, entry, sl, tp, risk_usdt, net, r, partial, exit)"""
    with open(JOURNAL_FILE, "a") as f:
        f.write(json.dumps(trade, ensure_ascii=False) + "\n")


def load(since=None):
    trades = []
    try:
        with open(JOURNAL_FILE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    t = json.loads(line)
                except ValueError:
                    continue
                if since and t.get("closed", "") < since:
                    continue
                trades.append(t)
    except FileNotFoundError:
        pass
    return trades


def _status():
    try:
        with open(STATUS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_status(st):
    with open(STATUS_FILE, "w") as f:
        json.dump(st, f)


# =========================================================================
# İstatistik
# =========================================================================
def stats(trades):
    t = [x for x in trades if x.get("net") is not None]
    n = len(t)
    if n == 0:
        return {"n": 0}
    nets = [x["net"] for x in t]
    rs = [x["r"] for x in t if x.get("r") is not None]
    wins = [v for v in nets if v > 0]
    losses = [v for v in nets if v <= 0]

    # Zirveden en büyük düşüş ve en uzun kayıp serisi (kapanış sırasına göre)
    eq = peak = max_dd = 0.0
    streak = max_streak = 0
    for v in nets:
        eq += v
        peak = max(peak, eq)
        max_dd = max(max_dd, peak - eq)
        streak = streak + 1 if v <= 0 else 0
        max_streak = max(max_streak, streak)

    partial = [x for x in t if x.get("partial")]
    exits = {}
    for x in t:
        exits[x.get("exit", "?")] = exits.get(x.get("exit", "?"), 0) + 1

    return {
        "n": n,
        "net": sum(nets),
        "avg_net": sum(nets) / n,
        "avg_r": (sum(rs) / len(rs)) if rs else None,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / n,
        "avg_win": (sum(wins) / len(wins)) if wins else 0.0,
        "avg_loss": (sum(losses) / len(losses)) if losses else 0.0,
        "profit_factor": (sum(wins) / -sum(losses)) if losses and sum(losses) < 0 else None,
        "partial_rate": len(partial) / n,
        "tp_after_partial": (sum(1 for x in partial if x.get("exit") == "TP") / len(partial)) if partial else None,
        "max_dd": max_dd,
        "max_streak": max_streak,
        "current_streak": streak,
        "current_dd": peak - eq,
        "exits": exits,
    }


def _usd(v):
    s = f"{abs(v):,.0f}".replace(",", ".")
    return ("+" if v > 0 else "−" if v < 0 else "") + s


def _pct(v):
    return "–" if v is None else f"%{v * 100:.0f}"


def _r(v):
    return "–" if v is None else f"{'+' if v > 0 else ''}{v:.2f}R".replace(".", ",")


def _line(label, s):
    if s["n"] == 0:
        return f"  {label}: işlem yok"
    return f"  {label}: {s['n']} işlem | {_r(s['avg_r'])}/işlem | net {_usd(s['net'])} USDT | kazanma {_pct(s['win_rate'])}"


def build_report(balance_line=None, now=None):
    now = now or datetime.datetime.now(TZ)
    week_start = (now - datetime.timedelta(days=7)).isoformat()
    all_t = load()
    week_t = [x for x in all_t if x.get("closed", "") >= week_start]
    s = stats(all_t)
    w = stats(week_t)

    lines = [f"📊 Bot raporu – {now.strftime('%d.%m.%Y %H:%M')}"]
    if s["n"] == 0:
        lines.append("Henüz kapanmış işlem yok.")
        if balance_line:
            lines.append(balance_line)
        return "\n".join(lines)

    first = min(x["closed"] for x in all_t)[:10]
    days = max(1, (now.date() - datetime.date.fromisoformat(first)).days)

    lines += [
        "",
        f"🗓 Son 7 gün: {w['n']} işlem | net {_usd(w.get('net', 0))} USDT | {_r(w.get('avg_r'))}/işlem"
        if w["n"] else "🗓 Son 7 gün: kapanan işlem yok",
        "",
        f"📈 Test başından beri ({first}, {days} gün):",
        f"  İşlem: {s['n']} | Net: {_usd(s['net'])} USDT",
        f"  İşlem başına: {_r(s['avg_r'])} ({_usd(s['avg_net'])} USDT)",
        f"  Kazanan/kaybeden: {s['wins']}/{s['losses']} ({_pct(s['win_rate'])})",
        f"  Ort. kazanç {_usd(s['avg_win'])} | ort. kayıp {_usd(s['avg_loss'])}"
        + (f" | kâr faktörü {s['profit_factor']:.2f}".replace(".", ",") if s["profit_factor"] else ""),
        f"  Yarı kâra ulaşma: {_pct(s['partial_rate'])} | yarı kârdan sonra TP: {_pct(s['tp_after_partial'])}",
        f"  En büyük düşüş: {_usd(-s['max_dd'])} USDT | en uzun kayıp serisi: {s['max_streak']}",
        "  Çıkışlar: " + " · ".join(f"{k} {v}" for k, v in sorted(s["exits"].items(), key=lambda kv: -kv[1])),
        "",
        "🔎 Döküm:",
    ]
    for tf in sorted({x.get("tf") or "?" for x in all_t}):
        lines.append(_line(tf, stats([x for x in all_t if (x.get("tf") or "?") == tf])))
    lines.append(_line("LONG", stats([x for x in all_t if x.get("side") == "AL"])))
    lines.append(_line("SHORT", stats([x for x in all_t if x.get("side") == "SAT"])))

    if balance_line:
        lines += ["", balance_line]
    if s["n"] < MIN_RELIABLE_TRADES:
        lines += ["", f"ℹ️ {s['n']}/{MIN_RELIABLE_TRADES} işlem: sonuçlar henüz güvenilir değil, şans payı yüksek."]
    if s["max_dd"] >= STOP_DRAWDOWN_USDT or s["max_streak"] >= STOP_LOSING_STREAK:
        lines += ["", "🛑 Durma noktası aşıldı (düşüş ya da kayıp serisi) – sonuçları birlikte değerlendirelim."]
    return "\n".join(lines)


# =========================================================================
# Otomatik gönderim
# =========================================================================
def maybe_send_weekly(notify, balance_fn=None, force=False):
    now = datetime.datetime.now(TZ)
    st = _status()
    week_id = now.strftime("%G-W%V")
    due = now.weekday() == REPORT_WEEKDAY and now.hour >= REPORT_HOUR and st.get("last_week") != week_id
    if not (due or force):
        return False
    balance_line = None
    if balance_fn:
        try:
            balance_line = balance_fn()
        except Exception as e:
            balance_line = f"Bakiye okunamadı: {e}"
    notify(build_report(balance_line, now))
    if due:
        st["last_week"] = week_id
        _save_status(st)
    return True


def check_stop_alerts(notify):
    """Her kapanıştan sonra çağrılır; eşik ilk aşıldığında bir kez uyarır."""
    s = stats(load())
    if s["n"] == 0:
        return
    st = _status()
    if s["current_dd"] >= STOP_DRAWDOWN_USDT and not st.get("dd_alert"):
        notify(f"🛑 Durma noktası: test başından beri zirveden düşüş {_usd(-s['current_dd'])} USDT "
               f"(sınır {STOP_DRAWDOWN_USDT}). Bot çalışmaya devam ediyor; sonuçlara birlikte bakalım.")
        st["dd_alert"] = True
    if s["current_streak"] >= STOP_LOSING_STREAK and not st.get("streak_alert"):
        notify(f"🛑 Durma noktası: art arda {s['current_streak']} kayıp (sınır {STOP_LOSING_STREAK}). "
               f"Bot çalışmaya devam ediyor; sonuçlara birlikte bakalım.")
        st["streak_alert"] = True
    _save_status(st)
