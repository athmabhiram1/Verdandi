"""TDD red step (D0-RED): twin duty-cycle bar tests (MINIPRO-24 T9, plan v4 §5).

Name contract (BAR-SUITE: `-m k2` + `test_duty_*` selection depend on it —
never rename without updating the plan): every test here is named test_duty_*.

RED STEP: the M1–M4 rebalance has not landed — every test MUST fail on the
current code for a bar reason (D0: 777-clean STARVED 29.46% pooled / 23.98%
primary, RUN 68.82%/76.02%, xfer_open 4–5 stranded, conservation off by
45–77 parts, B12 at-cap dwell x21, clean-42/95 BLOCKED residue), never for
import/syntax errors. Each test calls twin.run_episode FIRST and asserts on
OBSERVABLES only (states matrix, flow_stats, sbuf_stats, buffers, digests).
Split buckets (FEED_WAIT / KIT_MISS / CELL_WAIT / RWK_IDLE) land in D1 —
nothing here asserts on them.

Pinned probes: seed 777 default; 287 congestion pin; calibration seeds
7/42/95. Fault shapes byte-identical to tests/test_twin_flow.py (_DELAY_A5)
and src/twin.py _F21_SHAPE (_F21): DELAY_A5 t0=150 dur=15 d=5; F-21 drift B5
t0=150 dur=12 mag=5.2. cap_dwell adds one documented stress pin,
_DELAY_A5_EARLY (same shape, t0=120 — earlier fault → longer cascade → B12
at-cap x21 @287; delay-class slowdown with both sides running, so the M3
early-stop can relieve it; breakdown pileups are NOT used — a DOWN drain
stall is invariant to early-stop). Natural breakdowns ON everywhere (T9-6).

Denominators: primary post-CAL[120,300) x 31 machines (RWK0 excluded) x
DOWN-excluded cells; legacy pooled 32x300 continuity in digest_stable.
Line filters always use the explicit A0–A9 / B0–B9 / C0–C7 lists (never
`name[0] in 'ABC'` — that predicate also matches ASM*).

Traceability: .omo/plans/minipro-24-t9-final.md v4 §3 (T9-1..T9-7 bars +
denominators), §5 (test list + seeds), §7 BAR-SUITE/BAR-DENOM-SCOPE;
docs/SIM_SPEC.md Table 3.1 (caps), §2.2 (SBUF), §4.1 (states).
"""

import warnings

import pytest

from src import twin
from src.config import BUFFERS, CAL_WIN, MACHINE_INDEX, T

pytestmark = pytest.mark.k2

_SEED = 777
_SEED_CONGEST = 287
_CLEAN_SEEDS = (7, 42, 95, 287, 777)

_DELAY_A5 = {
    "id": "F-T2-delay",
    "class": "delay",
    "origin": "A5",
    "t0": 150,
    "dur": 15,
    "mag_sigma": 0.0,
    "extra": {"d": 5},
}

# Dwell stress pin: same delay shape as _DELAY_A5, earlier t0 so the cascade
# has room to pin B12 at cap (measured x21 @287). t0=150 stays reserved for
# the F1/F2 congestion pins — this pin is cap_dwell's own, documented here.
_DELAY_A5_EARLY = {
    "id": "F-T9-dwell",
    "class": "delay",
    "origin": "A5",
    "t0": 120,
    "dur": 15,
    "mag_sigma": 0.0,
    "extra": {"d": 5},
}

# Byte-identical to src/twin.py _F21_SHAPE (no "extra" key — keep it so).
_F21 = {
    "id": "F-21",
    "class": "drift",
    "origin": "B5",
    "t0": 150,
    "dur": 12,
    "mag_sigma": 5.2,
}

# D0 digest anchors (natural breakdown ON): F-21 lives on 777+F-21, never on
# 777-clean. Full-hex pins; rerun-identity is asserted separately.
# D7 signed re-baseline (BAR-DIGEST, once): D0 clean 60320697… -> c73c1b2c…,
# D0 F-21 d2b4fb23… -> ddfec603… (M1–M4 rebalance by design; old logged here).
# D9 signed re-baseline #2 (BAR-DIGEST, owner waiver TAKT5+AGV3 by design):
# D7 clean c73c1b2c… -> 46d14062…, D7 F-21 ddfec603… -> ec968f09…
# (TAKT5-only interim dc8716d4…/4a3db3f1… measured, never committed).
_DIGEST_CLEAN777 = (
    "46d14062df44435a0cffcedb3689ffa666fc716828cb95b25b621f1b43c3c585"
)
_DIGEST_F21_777 = (
    "ec968f0910d9ba5fb10d551cb8535639b157839b3c50c4474b51ac9a244459ae"
)

_LINE_NAMES = (
    [f"A{i}" for i in range(10)]
    + [f"B{i}" for i in range(10)]
    + [f"C{i}" for i in range(8)]
)
_LINE_IDX = [MACHINE_INDEX[n] for n in _LINE_NAMES]
_RWK0 = MACHINE_INDEX["RWK0"]
_PRIMARY_IDX = [i for i in range(32) if i != _RWK0]  # 31 machines, RWK0 out
_POSTCAL = range(CAL_WIN, T)


def _duty(rec, idx):
    """Primary-style duty counts over idx x post-CAL, DOWN-excluded cells."""
    st = rec["states"]
    cells = [(m, t) for m in idx for t in _POSTCAL if st[m][t] != "DOWN"]
    denom = len(cells)
    n_run = sum(1 for m, t in cells if st[m][t] == "RUN")
    n_starved = sum(1 for m, t in cells if st[m][t] == "STARVED")
    n_blocked = sum(1 for m, t in cells if st[m][t] == "BLOCKED")
    return denom, n_run, n_starved, n_blocked


def _pct(n, d):
    return 100.0 * n / d


def _pooled_starved(rec):
    st = rec["states"]
    n = sum(1 for m in range(32) for t in range(T) if st[m][t] == "STARVED")
    return 100.0 * n / (32 * T)


def _dwell_band(n):
    """T9-3 cap-dwell bands: <=6 transient OK, 7–11 WARN+note, >=12 FAIL."""
    if n <= 6:
        return "OK"
    if n <= 11:
        return "WARN"
    return "FAIL"


def _max_dwells(rec):
    """Per-buffer longest consecutive at-cap run (caps from BUFFERS)."""
    order = list(BUFFERS)
    out = {}
    for name in order:
        row = rec["buffers"][order.index(name)]
        cap = BUFFERS[name]
        cur = best = 0
        for lvl in row:
            cur = cur + 1 if lvl == cap else 0
            best = max(best, cur)
        out[name] = (cap, best)
    return out


def _conservation(rec):
    """T9 conservation (triple-aware, D4-proven exact): every line-created
    part is either embodied in a sunk assembly (x3) or still in the plant.

    Rationale: sunk/scrapped co-increment on ONE ASM-kit part (twin.py:924-925),
    so scrapped ⊆ sunk — never add both. Each sunk assembly, each ASM-stage
    holding (held_asm0_batch/held_asm12/held_rwk0), and each ASM01/ASM12/RWK_RET
    store slot embodies an A+B+C triple, hence x3. Line-gap stores, kit_A/B/C,
    held_line, xfer_open, and SBUF final hold single line parts (x1).
    xfer_drained is excluded (drained parts already counted in kit_*);
    rejected/reworked are flows, absent from a stock census.
    Proof: seed-7 clean 180 == 72+73+11+18+6+0+0
    (3*sunk + storeline + kit + heldline + 3*heldasm + 3*asmstores + xfer + sbuf).
    """
    fs = rec["flow_stats"]
    ss = rec["sbuf_stats"]
    sf = fs["store_final"]
    store_line = sum(
        v for k, v in sf.items() if k not in ("ASM01", "ASM12", "RWK_RET", "SBUF")
    )
    lhs = fs["line_created"]
    rhs = (
        3 * fs["sunk"]
        + store_line
        + fs["kit_A"]
        + fs["kit_B"]
        + fs["kit_C"]
        + fs["held_line"]
        + 3 * (fs["held_asm0_batch"] + fs["held_asm12"] + fs["held_rwk0"])
        + 3 * (sf["ASM01"] + sf["ASM12"] + sf["RWK_RET"])
        + fs["xfer_open"]
        + ss["final"]
    )
    return lhs, rhs


def _tag(fault):
    return "clean" if fault is None else fault["id"]


def test_duty_starved_bar():
    rows = []
    for seed in _CLEAN_SEEDS:
        rec = twin.run_episode(seed, None)
        denom, _, n_stv, _ = _duty(rec, _PRIMARY_IDX)
        ldenom, _, ln_stv, _ = _duty(rec, _LINE_IDX)  # diagnostic only
        rows.append((seed, _pct(n_stv, denom), _pct(ln_stv, ldenom)))
    bad = [(s, round(p, 2)) for s, p, _ in rows if p > 15.0]
    assert not bad, (
        "T9-2 STARVED<=15% primary violated "
        f"(D3b: 777-clean 17.48%, 287-clean 18.12%): {bad} "
        f"full={{seed: (primary%, line-only%)}}: "
        f"{[(s, round(p, 2), round(q, 2)) for s, p, q in rows]}"
    )


def test_duty_run_bar():
    rows = []
    for seed in _CLEAN_SEEDS:
        rec = twin.run_episode(seed, None)
        denom, n_run, _, _ = _duty(rec, _PRIMARY_IDX)
        rows.append((seed, _pct(n_run, denom)))
    bad = [(s, round(p, 2)) for s, p in rows if p < 80.0]
    assert not bad, (
        "T9-1 RUN>=80% primary violated "
        f"(D3b: 777-clean 82.52%): {bad} "
        f"full={{seed: run%}}: {[(s, round(p, 2)) for s, p in rows]}"
    )


def test_duty_xfer_drained():
    episodes = [(s, None) for s in _CLEAN_SEEDS] + [
        (_SEED_CONGEST, _DELAY_A5),
        (_SEED, _F21),
    ]
    bad = []
    for seed, fault in episodes:
        rec = twin.run_episode(seed, dict(fault) if fault else None)
        xfer = rec["flow_stats"]["xfer_open"]
        sbuf_final = rec["sbuf_stats"]["final"]
        if xfer != 0 or sbuf_final != 0:
            bad.append((_tag(fault), seed, xfer, sbuf_final))
    assert not bad, (
        "T9-4 drain closeout violated (xfer_open==0 AND SBUF final==0 every "
        f"episode; D0: xfer 4-5 stranded): {bad} as "
        "(fault, seed, xfer_open, sbuf_final)"
    )


def test_duty_blocked_bound():
    rows = []
    for seed in _CLEAN_SEEDS:
        rec = twin.run_episode(seed, None)
        denom, _, _, n_blk = _duty(rec, _PRIMARY_IDX)
        rows.append((seed, _pct(n_blk, denom)))
    over_minimum = [(s, round(p, 3)) for s, p in rows if p > 1.0]
    assert not over_minimum, (
        f"T9-3 minimum BLOCKED<=1% clean violated: {over_minimum}"
    )
    # F1 pin: clean-287 zero-BLOCKED immutable — scoped to seed 287 only.
    # T9-3's minimum for every clean seed is <=1% (asserted above); the ==0
    # target lives here solely as the F1 pin. D0: 287-clean shows 0 BLOCKED.
    pin287 = [(s, round(p, 3)) for s, p in rows if s == _SEED_CONGEST and p > 0.0]
    assert not pin287, (
        "F1 pin broken: clean-287 must show zero BLOCKED "
        f"(D0: 0 BLOCKED): {pin287} as (seed, blocked%)"
    )


def test_duty_cap_dwell():
    episodes = [(s, None) for s in _CLEAN_SEEDS] + [
        (_SEED_CONGEST, _DELAY_A5),
        (_SEED, _F21),
        (_SEED_CONGEST, _DELAY_A5_EARLY),
    ]
    bad = []
    for seed, fault in episodes:
        rec = twin.run_episode(seed, dict(fault) if fault else None)
        for name, (cap, best) in _max_dwells(rec).items():
            band = _dwell_band(best)
            if band == "WARN":
                warnings.warn(
                    f"cap-dwell WARN-note: {_tag(fault)} seed={seed} buffer={name} "
                    f"cap={cap} at-cap x{best} steps (7-11 band)",
                    stacklevel=2,
                )
            if band == "FAIL":
                bad.append((_tag(fault), seed, name, cap, best, band))
    assert not bad, (
        "T9-3 cap-dwell violated (<=6 OK / 7-11 WARN+note / >=12 FAIL; "
        "D0: B12 at-cap x21 under 287+F-T9-dwell): "
        f"{bad} as (fault, seed, buffer, cap, dwell, band)"
    )


def test_duty_conservation():
    episodes = [(s, None) for s in _CLEAN_SEEDS] + [
        (_SEED_CONGEST, _DELAY_A5),
        (_SEED, _F21),
    ]
    imbalanced = []
    for seed, fault in episodes:
        rec = twin.run_episode(seed, dict(fault) if fault else None)
        lhs, rhs = _conservation(rec)
        if lhs != rhs:
            imbalanced.append((_tag(fault), seed, lhs, rhs, lhs - rhs))
    assert not imbalanced, (
        "T9 conservation violated: line_created == 3*sunk+Σstore_line"
        "+Σkit+held_line+3*held_asm+3*asmstores+xfer_open+sbuf "
        "(D0: unit-mismatched form, diffs +45..+77): "
        f"{imbalanced} as (fault, seed, LHS, RHS, diff)"
    )
    for seed, fault in episodes:
        rec = twin.run_episode(seed, dict(fault) if fault else None)
        assert all(p.get("passes", 0) <= 2 for p in rec["parts"]), (
            f"rework passes<=2 violated: {_tag(fault)} seed={seed}"
        )
    for seed in _CLEAN_SEEDS:
        rec = twin.run_episode(seed, None)
        ss = rec["sbuf_stats"]
        if ss["diverted"] > 0:
            assert ss["diverted"] == ss["drained"], (
                f"clean SBUF must end drained when it diverted (D0: 287-clean "
                f"diverted={ss['diverted']} drained={ss['drained']}): seed={seed}"
            )


def test_duty_digest_stable():
    r1 = twin.run_episode(_SEED, None)
    r2 = twin.run_episode(_SEED, None)
    assert twin.replay_digest(r1) == twin.replay_digest(r2), (
        "same-seed 777-clean rerun must be digest-identical"
    )
    f1 = twin.run_episode(_SEED, dict(_F21))
    f2 = twin.run_episode(_SEED, dict(_F21))
    assert twin.replay_digest(f1) == twin.replay_digest(f2), (
        "same-seed 777+F-21 rerun must be digest-identical"
    )
    assert twin.replay_digest(r1) == _DIGEST_CLEAN777, (
        "777-clean digest pin moved (legacy 60320697…)"
    )
    assert twin.replay_digest(f1) == _DIGEST_F21_777, (
        "777+F-21 digest pin moved (D0 d2b4fb23…, breakdown ON)"
    )
    pooled = _pooled_starved(r1)
    # D7 signed re-baseline: D0 continuity 29.5% -> 25.82% measured pooled
    # @777-clean (STARVED drop by design, M1–M4; old value logged here).
    # D9 signed re-baseline #2: 25.82% -> 15.30% measured pooled @777-clean
    # (STARVED drop by design, TAKT5+AGV3 retime; old value logged here).
    assert abs(pooled - 15.30) <= 0.3, (
        f"legacy 15.30%@777-clean pooled continuity broken: {pooled:.2f}%"
    )
    # Anchor duty: the stable anchor episode must also meet the primary bar.
    denom, _, n_stv, _ = _duty(r1, _PRIMARY_IDX)
    primary = _pct(n_stv, denom)
    assert primary <= 15.0, (
        "anchor stable (rerun+pins+29.5% continuity hold) but anchor duty "
        f"violates T9-2: 777-clean primary STARVED {primary:.2f}% > 15%"
    )
