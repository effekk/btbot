"""Kurulum grafikleri ve özsermaye eğrisi (matplotlib, Agg)."""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.patches import Rectangle  # noqa: E402

YESIL, KIRMIZI = "#26a69a", "#ef5350"
GUN_MS = 86_400_000


def _mumlar(ax, x, o, h, l, c, gen=0.6):
    for xi, oi, hi, li, ci in zip(x, o, h, l, c):
        renk = YESIL if ci >= oi else KIRMIZI
        ax.vlines(xi, li, hi, color=renk, lw=0.8, zorder=2)
        ax.add_patch(Rectangle((xi - gen / 2, min(oi, ci)), gen, max(abs(ci - oi), 1e-12),
                               facecolor=renk, edgecolor=renk, lw=0.5, zorder=3))


def _zaman(ms, gun=False):
    s = str(np.datetime64(int(ms), "ms"))
    return s[:10] if gun else s[5:16].replace("T", " ")


def kurulum_grafigi(dosya, V, G, x, bias_bilgi):
    """V: 5m numpy dict, G: günlük numpy dict (+ 'sh_piv','sl_piv','bias','son_sh','son_sl')."""
    t, O, H, L, C = V["t"], V["o"], V["h"], V["l"], V["c"]
    a = max(0, x["m1"] - 20)
    b = min(len(C) - 1, x["x"] + 8)
    sl = slice(a, b + 1)
    xs = np.arange(a, b + 1)
    fig = plt.figure(figsize=(16, 7.5), dpi=110)
    ax = fig.add_axes([0.05, 0.10, 0.66, 0.80])
    axg = fig.add_axes([0.76, 0.10, 0.22, 0.80])
    _mumlar(ax, xs, O[sl], H[sl], L[sl], C[sl])
    yon = x["yon"]
    alt, ust = sorted([x["bolge_stop"], x["bolge_giris"]])
    # FVG bölgesi: m1'den girişe kadar
    ax.add_patch(Rectangle((x["m1"] - 0.5, alt), x["e"] - x["m1"] + 1, ust - alt,
                           facecolor="orange", alpha=0.18, edgecolor="darkorange", lw=1, zorder=1))
    ax.axvspan(x["m1"] - 0.5, x["m3"] + 0.5, color="gold", alpha=0.10, zorder=0)
    ax.text(x["m1"] - 0.4, ust if yon > 0 else alt, "FVG m1–m3", fontsize=8, color="darkorange",
            va="bottom" if yon > 0 else "top")
    # inversion
    k = x["inv"]
    yk = H[k] if yon > 0 else L[k]
    ax.annotate("inversion", (k, yk), xytext=(k, yk + (1 if yon > 0 else -1) * (H[sl].max() - L[sl].min()) * 0.06),
                ha="center", fontsize=8, color="purple",
                arrowprops=dict(arrowstyle="->", color="purple"))
    if x["senaryo"] == "A":
        ax.hlines(x["maks3"], x["m1"], x["tetik"] + 0.5, colors="purple", linestyles=":", lw=1.2)
        ax.text(x["m1"], x["maks3"], " A tetik seviyesi (3 mumun en uç değeri)", fontsize=7, color="purple",
                va="bottom" if yon > 0 else "top")
        ax.plot([x["tetik"]], [C[x["tetik"]]], marker="s", ms=6, mfc="none", mec="purple")
    if x["tip"] == "B-piyasa":
        g = x["geri_cekilme"]
        ax.plot([g], [L[g] if yon > 0 else H[g]], marker="o", ms=7, mfc="none", mec="blue")
        ax.text(g, L[g] if yon > 0 else H[g], " geri çekilme", fontsize=7, color="blue",
                va="top" if yon > 0 else "bottom")
        ax.hlines(x["kirilim"], k, x["tetik"] + 0.5, colors="blue", linestyles=":", lw=1.2)
        ax.text(k, x["kirilim"], " kırılım seviyesi", fontsize=7, color="blue", va="bottom")
        ax.plot([x["tetik"]], [C[x["tetik"]]], marker="s", ms=6, mfc="none", mec="blue")
    if x["tip"] == "B-limit":
        ax.hlines(x["giris"], k + 0.5, x["e"], colors="teal", linestyles="--", lw=1.2)
        ax.text(k + 0.5, x["giris"], " limit (bölge kenarı)", fontsize=7, color="teal", va="bottom")
    # giriş, stop, hedef, çıkış
    ax.plot([x["e"]], [x["giris"]], marker="^" if yon > 0 else "v", ms=11, color="black", zorder=5)
    ax.hlines(x["stop"], x["e"] - 0.5, x["x"] + 0.5, colors=KIRMIZI, lw=1.6)
    ax.hlines(x["hedef"], x["e"] - 0.5, x["x"] + 0.5, colors=YESIL, lw=1.6)
    ax.hlines(x["giris"], x["e"] - 0.5, x["x"] + 0.5, colors="black", lw=0.8, linestyles="--")
    ax.text(x["x"] + 0.7, x["stop"], f"stop {x['stop']:.2f}", fontsize=8, color=KIRMIZI, va="center")
    ax.text(x["x"] + 0.7, x["hedef"], f"hedef {x['hedef']:.2f} ({x['hedef_tip']})", fontsize=8, color=YESIL, va="center")
    ax.text(x["x"] + 0.7, x["giris"], f"giriş {x['giris']:.2f}", fontsize=8, va="center")
    ax.plot([x["x"]], [x["cikis"]], marker="X", ms=10, color="dimgray", zorder=6)
    ax.annotate(f"çıkış: {x['cikis_tipi']}", (x["x"], x["cikis"]), xytext=(0, -16 if yon > 0 else 16),
                textcoords="offset points", ha="center", fontsize=8, color="dimgray")
    # zaman ekseni
    adim = max(1, (b - a) // 12)
    ax.set_xticks(xs[::adim])
    ax.set_xticklabels([_zaman(t[i]) for i in xs[::adim]], rotation=30, fontsize=7)
    ax.set_xlim(a - 1, b + 9)
    ax.grid(alpha=0.2)
    yon_ad = "LONG" if yon > 0 else "SHORT"
    bias_ad = {1: "bullish", -1: "bearish", 0: "yok"}[x["bias"]]
    ax.set_title(f"{x['sembol']} {x['varyant']} | Senaryo {x['senaryo']} ({x['tip']}) | {yon_ad} | "
                 f"giriş {_zaman(x['giris_zaman'])} UTC | bias: {bias_ad}", fontsize=10)
    ax.text(0.01, 0.98,
            f"stop mesafesi %{x['stop_yuzde']:.3f}   hedef {x['hedef_R']:.2f}R   giriş tipi: {x['giris_tipi']}\n"
            f"FVG {_zaman(t[x['m1']])}–{_zaman(t[x['m3']])}   inversion {_zaman(t[k])}   "
            f"tetik/dolum {_zaman(t[x['tetik']])}",
            transform=ax.transAxes, fontsize=8, va="top", bbox=dict(fc="white", alpha=0.8, ec="0.8"))

    # günlük panel: bias kontrolü
    gt = G["t"]
    gi = int(np.searchsorted(gt, (x["giris_zaman"] // GUN_MS) * GUN_MS))
    ga = max(0, gi - 30)
    gx = np.arange(ga, gi + 1)
    _mumlar(axg, gx, G["o"][ga:gi + 1], G["h"][ga:gi + 1], G["l"][ga:gi + 1], G["c"][ga:gi + 1], 0.7)
    axg.axvspan(gi - 0.5, gi + 0.5, color="gray", alpha=0.15)
    for p in G["sh_piv"]:
        if ga <= p and p + 2 < gi:          # işlem gününden önce kesinleşmiş
            axg.plot([p], [G["h"][p] * 1.003], marker="v", color="purple", ms=5)
    for p in G["sl_piv"]:
        if ga <= p and p + 2 < gi:
            axg.plot([p], [G["l"][p] * 0.997], marker="^", color="purple", ms=5)
    if not np.isnan(G["son_sh"][gi]):
        axg.axhline(G["son_sh"][gi], color=YESIL, lw=0.8, ls="--")
    if not np.isnan(G["son_sl"][gi]):
        axg.axhline(G["son_sl"][gi], color=KIRMIZI, lw=0.8, ls="--")
    axg.set_title(f"Günlük — işlem günü bias: {bias_ad}\n(▼▲ kesinleşmiş swing; kesik: son SH/SL)", fontsize=8)
    gadim = max(1, len(gx) // 6)
    axg.set_xticks(gx[::gadim])
    axg.set_xticklabels([_zaman(gt[i], True)[5:] for i in gx[::gadim]], fontsize=7, rotation=30)
    axg.tick_params(axis="y", labelsize=7)
    axg.grid(alpha=0.2)
    fig.savefig(dosya)
    plt.close(fig)


def ozsermaye_grafigi(dosya, egri_t, egri, baslik, baslangic=5000.0):
    fig, ax = plt.subplots(figsize=(11, 4.5), dpi=110)
    tt = [np.datetime64(int(z), "ms") for z in egri_t]
    if tt:
        ax.plot(tt, egri, lw=1.2, color="#2b6cb0")
    ax.axhline(baslangic, color="gray", lw=0.8, ls="--")
    ax.set_title(baslik, fontsize=10)
    ax.set_ylabel("USDT")
    ax.grid(alpha=0.25)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(dosya)
    plt.close(fig)
