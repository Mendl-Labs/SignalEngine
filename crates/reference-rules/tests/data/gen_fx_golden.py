#!/usr/bin/env python3
"""Generate the FX time-series-momentum goldens by RUNNING the reference `decide_s2` (stage1-record/tool/ticket.py).

Nothing here re-derives the rule: the reference function is IMPORTED BY PATH from ticket.py (not copied), after
its sha256 is checked against the pinned value below, and the candles are pivoted with the reference's own
`ticket.load_csv`. The only logic in this script is (a) choosing which dates to ask the reference about and
(b) writing its answers with full float precision (`repr`, which round-trips a float64 exactly).

Usage (from anywhere):
    python3 gen_fx_golden.py --ticket /path/to/stage1-record/tool/ticket.py --candles ladder_candles.csv \
        --out golden_fx_tsmom.csv --clip-candles-out fx_clip_candles.csv --clip-out golden_fx_clip.csv

Inputs are pinned by sha256 (the script refuses otherwise):
    ticket.py           62947cfc87356e49475f71df48bcc4adfadaf0f5166940166de391b94c8cb563
    ladder_candles.csv  d5cea5f25a4d0d4ecdf4f3c48ba0bd8d50f72fe208a7a6b5ace1399bb644a365

golden_fx_tsmom.csv: two history windows for every eligible month-end, because `decide_s2` computes the
annualisation factor `ppy` over WHATEVER history it is handed (a reference quirk the Rust port makes an explicit
input):
    full   the whole file (what the backtest / shadow validation saw): history_start = first bar in the file
    lb520  the live run's window: ticket.py `load_api` fetches asof - 520 days (config.json lookback_days = 520),
           here with asof = the decision date: history_start = decision date - 520 days
Eligible decision dates: month-ends of the reference's joint (dropna) calendar at which the reference does not
raise AND every one of the 7 pairs has a bar on that date AND no pair has a later bar in the same month (the Rust
port refuses NotMonthEnd there; the reference silently accepts any date, e.g. 2019-09-13 in a 23-day data hole).
Columns: variant,date,history_start,symbol,sign,sigma,weight,ppy_aux
    sign, sigma, weight: the reference's return value (sign int, sigma float, weight float), verbatim.
    ppy_aux: AUXILIARY, computed here with the reference's own formula from the same frame (the reference does not
             return it); the Rust golden test uses it only to show the window quirk, never as an expected weight.

golden_fx_clip.csv (+ fx_clip_candles.csv): the ladder history never produces |weight| = 3 (its maximum is about
1.87), so a deterministic synthetic panel is built in which the cap binds, and the reference is run on it.
"""
import argparse
import hashlib
import importlib.util
import sys

TICKET_SHA = "62947cfc87356e49475f71df48bcc4adfadaf0f5166940166de391b94c8cb563"
CANDLES_SHA = "d5cea5f25a4d0d4ecdf4f3c48ba0bd8d50f72fe208a7a6b5ace1399bb644a365"
LOOKBACK_DAYS = 520


def sha256(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def write_clip_case(ticket, np, pd, candles_out, golden_out, pyver):
    """EURUSD is far quieter than the rest and trends up (raw = sign/sigma is huge, so k*raw > 3 -> +3), GBPUSD is
    equally quiet and trends down (-3); the other five are ordinary. The candles are written to a CSV (so the Rust
    test does not depend on numpy's RNG) and the reference is run on the CSV re-read through its own load_csv."""
    syms = ticket.SLEEVE_SYMS["S2"]
    rng = np.random.default_rng(20260923)
    dates = pd.bdate_range("2018-01-01", periods=420)
    spec = {  # symbol: (daily drift, daily vol)
        "EURUSD": (0.0006, 0.0004), "GBPUSD": (-0.0006, 0.0004), "USDJPY": (0.0002, 0.006),
        "AUDUSD": (-0.0003, 0.007), "USDCAD": (0.0001, 0.006), "USDCHF": (-0.0002, 0.005), "NZDUSD": (0.0003, 0.007),
    }
    with open(candles_out, "w", newline="\n") as f:
        f.write("symbol,date_utc,close\n")
        for s in syms:
            mu, sd = spec[s]
            px = 1.0 + 0.5 * rng.random()
            for d in dates:
                px *= 1.0 + mu + sd * rng.standard_normal()
                f.write(f"{s},{d.date()},{px!r}\n")
    closes = ticket.load_csv(candles_out, syms)
    joint = closes.dropna().index
    out = []
    for d in ticket.month_ends(joint)[12:]:  # 13 month-ends needed
        if d == joint[-1]:  # the data ends mid-month: not a real month-end
            continue
        res = ticket.decide_s2(closes, d)
        c = closes.loc[:d].dropna()
        ppy = (len(c) - 1) / ((c.index[-1] - c.index[0]).days / 365.25)
        for s in syms:
            r = res[s]
            out.append((str(d.date()), s, r["sign"], repr(r["sigma"]), repr(r["weight"]), repr(float(ppy))))
    with open(golden_out, "w", newline="\n") as f:
        f.write("# GOLDEN (synthetic, cap-binding): reference decide_s2 run on fx_clip_candles.csv by gen_fx_golden.py\n")
        f.write(f"# ticket.py sha256 {TICKET_SHA}\n# python {pyver} numpy {np.__version__} pandas {pd.__version__}\n")
        f.write("# history window: the whole synthetic file (history_start = its first bar)\n")
        f.write("date,symbol,sign,sigma,weight,ppy_aux\n")
        for r in out:
            f.write(",".join(str(x) for x in r) + "\n")
    nclip = sum(abs(float(r[4])) == 3.0 for r in out)
    print(f"clip case: {len(out)} rows, {nclip} weights at exactly +-3")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticket", required=True)
    ap.add_argument("--candles", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--clip-candles-out", required=True, help="synthetic candles that make the +-3 weight cap bind")
    ap.add_argument("--clip-out", required=True, help="the reference's answers on those synthetic candles")
    a = ap.parse_args()
    for path, want in ((a.ticket, TICKET_SHA), (a.candles, CANDLES_SHA)):
        got = sha256(path)
        if got != want:
            sys.exit(f"REFUSING: {path} sha256 {got} != pinned {want}")

    import numpy as np
    import pandas as pd

    spec = importlib.util.spec_from_file_location("ticket", a.ticket)
    ticket = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ticket)

    syms = ticket.SLEEVE_SYMS["S2"]
    closes = ticket.load_csv(a.candles, syms)
    own = {s: closes[s].dropna().index for s in syms}  # each pair's own bars
    joint = closes.dropna().index
    file_start = min(ix[0] for ix in own.values())
    me_all = ticket.month_ends(joint)

    def true_month_end(d):
        for s in syms:
            if d not in own[s]:
                return False
            nxt = own[s][own[s] > d]
            if len(nxt) and (nxt[0].year, nxt[0].month) == (d.year, d.month):
                return False
        return True

    rows, skipped = [], []
    for d in me_all:
        if not true_month_end(d):
            skipped.append((str(d.date()), "not a month-end of every pair's own series"))
            continue
        for variant, start in (("full", file_start), ("lb520", d - pd.Timedelta(days=LOOKBACK_DAYS))):
            frame = closes if variant == "full" else closes.loc[start:]
            try:
                res = ticket.decide_s2(frame, d)
            except ValueError as e:
                skipped.append((f"{d.date()} {variant}", f"reference raised: {e}"))
                continue
            c = frame[syms].loc[:d].dropna()
            ppy = (len(c) - 1) / ((c.index[-1] - c.index[0]).days / 365.25)
            for s in syms:
                r = res[s]
                rows.append((variant, str(d.date()), str(start.date()), s, r["sign"], repr(r["sigma"]), repr(r["weight"]), repr(float(ppy))))

    with open(a.out, "w", newline="\n") as f:
        f.write("# GOLDEN: output of the reference decide_s2 (stage1-record/tool/ticket.py), generated by gen_fx_golden.py\n")
        f.write(f"# ticket.py sha256 {TICKET_SHA}\n# ladder_candles.csv sha256 {CANDLES_SHA}\n")
        f.write(f"# python {sys.version.split()[0]} numpy {np.__version__} pandas {pd.__version__}\n")
        try:
            import bottleneck  # noqa
            f.write("# bottleneck present (pandas may use it for std)\n")
        except ImportError:
            f.write("# bottleneck NOT installed (pandas uses its own two-pass nanvar, numpy pairwise sums)\n")
        f.write("# variant full: history_start = first bar of the file; lb520: history_start = decision date - 520 days\n")
        f.write(f"# eligible month-ends: {len({r[1] for r in rows})} (each in both variants); skipped {len(skipped)} (variant, date) cases:\n")
        f.write("#   the reference raised for lack of 13 month-ends (2009-09-30 .. 2010-08-31, both variants), and 2 dates that are\n")
        f.write("#   month-ends of the joint calendar only because of data holes (2019-09-13, 2020-10-12): the Rust port refuses those\n")
        f.write("variant,date,history_start,symbol,sign,sigma,weight,ppy_aux\n")
        for r in rows:
            f.write(",".join(str(x) for x in r) + "\n")
    write_clip_case(ticket, np, pd, a.clip_candles_out, a.clip_out, sys.version.split()[0])
    print(f"wrote {len(rows)} rows, {len({r[1] for r in rows})} dates, skipped {len(skipped)}")


if __name__ == "__main__":
    main()
