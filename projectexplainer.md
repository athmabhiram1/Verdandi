This file explains Verdandi in plain words. Read it top to bottom before you touch code.

## 1 What the project is

Verdandi is a factory twin that runs on one normal CPU laptop. It copies a small factory with 32 machines: Line A has 10, Line B has 10, Line C has 8, plus assembly stations ASM0 to ASM2, plus rework station RWK0. Between machines there are 31 buffers that hold waiting parts. Two carts (AGV cap 2) move parts around. Each run lasts T=300 ticks. Faults are seeded, so the same seed gives the same story. The twin is free ground truth: we always know what really broke, so we can check our answers.

## 2 End goal and viva

The end goal is simple. Every alarm must point to a ranked cause that is short and checked. Short means 3 steps or less plus one gateway hop. Checked means at least 95% of the why sentences point to real proof in the logs, and every alarm can be replayed from its seed to show the same timeline.

Right now the project sits at CONDITIONAL GO. F1 is about 0.73, below the 0.85 bar. Top-guess accuracy AC@1 is 0.80 or higher, which passes. The demo runs in under 10 minutes.

For the viva, the examiner picks any alarm and asks for five things: the waterfall view, the proof triples, two replays of the same seed, and the JSON trail that ties them together.

## 3 Your task MINIPRO-24

You are athmabhiram on MINIPRO-24. Your job is twin duty, not diagnosis tuning. Bring STARVED down from 29.5% to 15% or less, and bring RUN up to 80% or more. Get AGV xfer_open to 0 with the SBUF carts drained, and keep pile-up inside its caps. Add the new duty test at tests/test_twin_duty.py, and freeze the twin when it turns green.

Do drain-bucket C first. Demand-pull B is conditional, only if it stays safe. Do not touch src/config.py, it stays frozen.

Honest gap line: MINIPRO-24/M0.2a/test_twin_duty.py is absent on disk; anchors MINIPRO-10/M0b/T9 overlay.

## 4 Architecture in plain words

Twin: mimics the 32 machines with seeded faults, which gives free ground truth.
Detect: spots a fault early per machine with max(q0.99, Q3 plus 1.5 IQR), with no single global threshold.
Veto: stops a bad move, with only one rule live, VETO_ASM2 at 2x margin.
Walk: traces stations in order, depth 3 or less, fan-out 8 inside one partition plus 1 gateway hop.
Narrate: tells the story with a plain template, about $0.005 and 2.5k tokens in 8 seconds.
Verify: checks each sentence against the logs, needs 95% or more, else it falls back to chain-cards.
Replay: reruns the small subgraph timeline with SeedSequence 5 times identical.
UI: shows the flow as a waterfall plus the /sim topology view.
Trail: exports one JSON file per alarm with the full click trail.

## 5 Bars and milestones

STARVED 15% or less: now 29.5%, FAIL, fixed by M0.2a.
RUN 80% or more: target of M0.2a.
AGV xfer_open 0 plus drained: target of M0.2a.
Pile-up caps hold: target of M0.2a.
F1 0.85 or more: now about 0.73, FAIL, fixed by M0b.
AC@1 70% or more: now 0.80 plus, PASS.
Flip below 40%: PASS.
Cause in 3 steps or less: PASS.
Grounding 95% or more: PASS.
Replay 5 times identical: PASS.
Demo under 600 seconds: PASS.
Never break safety clearance: always on.

Order of work: M0.2a duty fix with T9 first, then freeze the twin, then M0b and M1 diagnosis work.

## 6 Glossary

Twin: a live copy of the factory, tick by tick.
Detect: spot a fault early.
Veto: stop a bad move.
Walk: trace stations in order.
Narrate plus Verify: tell the story, then check it against the logs.
Replay: rerun the same timeline.
UI plus Trail: show the flow with a click trail you can export.
STARVED: machine idle, waiting for parts from upstream.
RUN: machine busy, parts flowing.
BLOCKED: machine done, but nowhere to send parts because downstream is full.
AGV drain: slow carts strand parts, so clear them out.
Pile-up bound: buffer capped so parts cannot pile forever.
Drain-bucket C: count stranded parts apart from normal flow.
Demand-pull B: build only on ask, like kanban.
F1: one score that blends misses and false alarms.
AC@1: top guess is right.
CONDITIONAL GO: pass on everything except F1.
Seeded replay: same seed gives the same timeline.
Triple-grounded: each sentence points to proof.

## 7 What done looks like + next step

Done means all of these are true: STARVED at 15% or less, RUN at 80% or more, xfer_open at 0, SBUF drained, pile-up inside caps, tests/test_twin_duty.py green 7 of 7, config untouched, T9 green, everything logged.

Next step: freeze the twin, run the under 10 minute demo, then move to M0b to push F1 to 0.85.
