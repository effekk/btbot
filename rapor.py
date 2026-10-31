"""
İşlem kaydı ve performans raporu.

- Her kapanan işlem `islemler.jsonl` dosyasına bir satır olarak yazılır (sunucuda /opt/sinyalbot).
- Her gün 23:55'ten sonra günlük, her pazar 21:00'den sonra haftalık rapor Telegram'a gider.
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
# trend_4h (1R = 100 USDT). Eşikler backtest'teki 378 işlemden Monte Carlo ile (%95): işlem sayısı arttıkça büyür.
STRATEJI_BASLANGIC = "2026-10-31"   # bu tarihten önce AÇILAN işlemler (eski strateji) rapora ve uyarılara girmez
STOP_STAGES = [                     # (en fazla işlem, düşüş sınırı USDT, kayıp serisi sınırı)
    (30, 1240, 12),
    (60, 1790, 14),
    (None, 2250, 15),
]
STOP_DRAWDOWN_USDT = 2250     # son aşamadaki düşüş sınırı (STOP_STAGES'in son satırı)
STOP_LOSING_STREAK = 15       # son aşamadaki kayıp serisi sınırı


def stop_limits(n):
    """n kapanmış işlem için geçerli (düşüş, kayıp serisi) sınırı."""
    for upto, dd, streak in STOP_STAGES:
        if upto is None or n <= upto:
            return dd, streak
    return STOP_DRAWDOWN_USDT, STOP_LOSING_STREAK


def load_strategy():
    """Sadece şu anki stratejinin işlemleri (STRATEJI_BASLANGIC'tan sonra açılanlar)."""
    return [t for t in load() if t.get("opened", "") >= STRATEJI_BASLANGIC]


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
    all_t = load_strategy()
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
        f"📈 Strateji başından beri ({first}, {days} gün; eski işlemler hariç):",
        f"  İşlem: {s['n']} | Net: {_usd(s['net'])} USDT",
        f"  İşlem başına: {_r(s['avg_r'])} ({_usd(s['avg_net'])} USDT)",
        f"  Kazanan/kaybeden: {s['wins']}/{s['losses']} ({_pct(s['win_rate'])})",
        f"  Ort. kazanç {_usd(s['avg_win'])} | ort. kayıp {_usd(s['avg_loss'])}"
        + (f" | kâr faktörü {s['profit_factor']:.2f}".replace(".", ",") if s["profit_factor"] else ""),
    ] + ([
        f"  Yarı kâra ulaşma: {_pct(s['partial_rate'])} | yarı kârdan sonra TP: {_pct(s['tp_after_partial'])}",
    ] if any(x.get("partial") for x in all_t) else []) + [
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
    dd_lim, streak_lim = stop_limits(s["n"])
    lines.append(f"🛡 Durma sınırı ({s['n']} işlem): düşüş {_usd(-dd_lim)} USDT · art arda {streak_lim} kayıp")
    if s["max_dd"] >= dd_lim or s["max_streak"] >= streak_lim:
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
    s = stats(load_strategy())
    if s["n"] == 0:
        return
    dd_lim, streak_lim = stop_limits(s["n"])
    st = _status()
    k_dd, k_st = f"dd_alert_{STRATEJI_BASLANGIC}", f"streak_alert_{STRATEJI_BASLANGIC}"
    if s["current_dd"] >= dd_lim and not st.get(k_dd):
        notify(f"🛑 Durma noktası: strateji başından beri zirveden düşüş {_usd(-s['current_dd'])} USDT "
               f"({s['n']} işlemde sınır {dd_lim}). Bot çalışmaya devam ediyor; sonuçlara birlikte bakalım.")
        st[k_dd] = True
    if s["current_streak"] >= streak_lim and not st.get(k_st):
        notify(f"🛑 Durma noktası: art arda {s['current_streak']} kayıp ({s['n']} işlemde sınır {streak_lim}). "
               f"Bot çalışmaya devam ediyor; sonuçlara birlikte bakalım.")
        st[k_st] = True
    _save_status(st)


# =========================================================================
# Günlük rapor
# =========================================================================
EVENTS_FILE = "olaylar.jsonl"     # sinyal / açılan / atlanan / yarı kâr olayları
DAILY_REPORT_HOUR = 23            # her gün bu saatten ...
DAILY_REPORT_MINUTE = 55          # ... ve dakikadan sonraki ilk çalıştırmada gönderilir
EVENTS_KEEP_DAYS = 60


def log_event(kind, symbol=None, **data):
    """kind: signal | open | skip | error | partial | untracked_close"""
    try:
        rec = {"ts": datetime.datetime.now(TZ).isoformat(timespec="seconds"), "kind": kind, "symbol": symbol}
        rec.update(data)
        with open(EVENTS_FILE, "a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"  olay kaydedilemedi ({e})")


def _load_events(start_iso, end_iso):
    out = []
    try:
        with open(EVENTS_FILE) as f:
            for line in f:
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if start_iso <= e.get("ts", "") < end_iso:
                    out.append(e)
    except FileNotFoundError:
        pass
    return out


def _prune_events():
    cutoff = (datetime.datetime.now(TZ) - datetime.timedelta(days=EVENTS_KEEP_DAYS)).isoformat()
    try:
        with open(EVENTS_FILE) as f:
            keep = [line for line in f if line.strip() and json.loads(line).get("ts", "") >= cutoff]
        with open(EVENTS_FILE + ".tmp", "w") as f:
            f.writelines(keep)
        os.replace(EVENTS_FILE + ".tmp", EVENTS_FILE)
    except Exception:
        pass


def _skip_reason(msg):
    m = (msg or "").lower()
    if "yön sınırı" in m:
        return "aynı yön sınırı"
    if "sınır" in m:
        return "pozisyon sınırı"
    if "zaten açık" in m:
        return "aynı coinde pozisyon"
    if "uzaklaşmış" in m or "aralığının dışında" in m:
        return "fiyat kaçtı"
    if "bakiye" in m:
        return "bakiye"
    if "küçük" in m:
        return "miktar küçük"
    return "diğer"


GUNLER = ["Pazartesi", "Salı", "Çarşamba", "Perşembe", "Cuma", "Cumartesi", "Pazar"]


def build_daily(day_start, day_end, bn=None):
    """
    day_start/day_end: TZ'li datetime. bn: binance_trader.daily_numbers(...) sonucu (yoksa None).
    """
    s_iso, e_iso = day_start.isoformat(), day_end.isoformat()
    trades = [t for t in load() if s_iso <= t.get("closed", "") < e_iso]
    events = _load_events(s_iso, e_iso)

    lines = [f"📅 Günlük rapor – {day_start.strftime('%d.%m.%Y')} {GUNLER[day_start.weekday()]} "
             f"({day_start.strftime('%H:%M')}–{day_end.strftime('%H:%M')})"]

    # Günün net sonucu (Binance kayıtları: botun ve elle yapılan tüm işlemler)
    if bn and not bn.get("error"):
        net = bn["realized"] + bn["commission"] + bn["funding"]
        start_bal = bn["wallet"] - net
        pct = f" (%{net / start_bal * 100:+.1f})".replace(".", ",") if start_bal > 0 else ""
        lines += ["", f"💰 Günün net sonucu: {_usd(net)} USDT{pct}",
                  f"   gerçekleşen K/Z {_usd(bn['realized'])} · komisyon {_usd(bn['commission'])} · "
                  f"funding {_usd(bn['funding'])}"]
    elif bn and bn.get("error"):
        lines += ["", f"💰 Binance rakamları okunamadı: {bn['error']}"]

    # Kapanan bot işlemleri
    s = stats(trades)
    untracked = [e for e in events if e["kind"] == "untracked_close"]
    lines.append("")
    if s["n"]:
        lines.append(f"🔁 Kapanan bot işlemi: {s['n']} (✅ {s['wins']} / 🔻 {s['losses']}) | "
                     f"net {_usd(s['net'])} USDT | {_r(s['avg_r'])}/işlem")
        best = max(trades, key=lambda t: t.get("net") or 0)
        worst = min(trades, key=lambda t: t.get("net") or 0)
        if s["n"] > 1:
            lines.append(f"   En iyi: {best['symbol']} {_usd(best.get('net') or 0)} ({best.get('exit')}) · "
                         f"En kötü: {worst['symbol']} {_usd(worst.get('net') or 0)} ({worst.get('exit')})")
        lines.append("   Çıkışlar: " + " · ".join(f"{k} {v}" for k, v in
                                                  sorted(s["exits"].items(), key=lambda kv: -kv[1])))
    else:
        lines.append("🔁 Kapanan bot işlemi yok")
    partials = [e for e in events if e["kind"] == "partial"]
    if partials:
        lines.append(f"   🔸 Yarı kâr alınan: {len(partials)}")
    if untracked:
        tot = sum(e.get("net") or 0 for e in untracked)
        lines.append(f"   Elle/eski pozisyon kapanışı: {len(untracked)} (net ≈ {_usd(tot)} USDT)")

    # Sinyal ve işlem açma
    sig = [e for e in events if e["kind"] == "signal"]
    opened = [e for e in events if e["kind"] == "open"]
    skipped = [e for e in events if e["kind"] == "skip"]
    errors = [e for e in events if e["kind"] == "error"]
    by_tf = {}
    for e in sig:
        by_tf[e.get("tf", "?")] = by_tf.get(e.get("tf", "?"), 0) + 1
    tf_txt = " · ".join(f"{k} {v}" for k, v in sorted(by_tf.items()))
    lines.append(f"🆕 Sinyal: {len(sig)}" + (f" ({tf_txt})" if tf_txt else "") +
                 f" | Açılan: {len(opened)} | Atlanan: {len(skipped)}" + (f" | Hata: {len(errors)}" if errors else ""))
    if skipped:
        reasons = {}
        for e in skipped:
            r = _skip_reason(e.get("msg"))
            reasons[r] = reasons.get(r, 0) + 1
        lines.append("   Atlanma sebebi: " + " · ".join(f"{k} {v}" for k, v in
                                                     sorted(reasons.items(), key=lambda kv: -kv[1])))

    # Açık pozisyonlar ve bakiye
    if bn and not bn.get("error"):
        pos = bn.get("positions", [])
        lines.append("")
        if pos:
            lines.append(f"📂 Açık pozisyonlar ({len(pos)}):")
            for p in pos:
                age = f" · {p['hours']:.1f} saat".replace(".", ",") if p.get("hours") is not None else ""
                lines.append(f"   {p['symbol']} {p['side']} {_usd(p['upnl'])} USDT{age}")
        else:
            lines.append("📂 Açık pozisyon yok")
        lines.append(f"💼 Bakiye: {bn['wallet']:,.0f} USDT".replace(",", ".") +
                     f" | açık K/Z {_usd(bn['upnl'])} USDT")
    return "\n".join(lines)


def maybe_send_daily(notify, numbers_fn=None, force=False):
    """Her gün DAILY_REPORT_HOUR:MINUTE'dan sonraki ilk çalıştırmada (ya da force ile hemen) gönderir."""
    now = datetime.datetime.now(TZ)
    st = _status()
    today = now.date().isoformat()
    due_time = now.replace(hour=DAILY_REPORT_HOUR, minute=DAILY_REPORT_MINUTE, second=0, microsecond=0)
    due = now >= due_time and st.get("last_day") != today
    if not (due or force):
        return False
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    bn = None
    if numbers_fn:
        try:
            bn = numbers_fn(int(start.timestamp() * 1000), int(now.timestamp() * 1000))
        except Exception as e:
            bn = {"error": str(e)}
    notify(build_daily(start, now, bn))
    if due:
        st = _status()
        st["last_day"] = today
        _save_status(st)
        _prune_events()
    return True
