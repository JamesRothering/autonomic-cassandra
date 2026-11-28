#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Verdict rules of the gauntlet (docs/plan/eval-harness.md, "Verdicts").

The service level comes from the run's own baseline phase: per 10 s interval of the probe, p99 latency at most
max(3 x baseline p99, 20 ms) and errors below 0.5% of the interval's operations.
"""

INTERVAL_S = 10
ERROR_LIMIT = 0.005
P99_FACTOR = 3.0
P99_FLOOR_MS = 20.0


def percentile(values, q):
    if not values:
        return 0.0
    v = sorted(values)
    i = min(len(v) - 1, max(0, int(round(q * len(v) + 0.5)) - 1))
    return v[i]


def evaluate(rows, tl, ledger, end_state, misconfig, recovery_deadline_s, tail_s):
    """rows: [(t_rel, op, key, latency_ms, outcome)]; tl: warmup, baseline, fault, recovery, total (seconds).

    ledger: result of Probe.verify; end_state: {"nodes_up": n, "nodes_expected": 3, "down": [...]}.
    """
    w, b = tl["warmup"], tl["baseline"]
    fault_start = w + b
    total = tl["total"]
    fault_end = total if misconfig else fault_start + tl["fault"]

    base = [r for r in rows if w <= r[0] < fault_start]
    base_p99 = percentile([r[3] for r in base], 0.99)
    threshold = max(P99_FACTOR * base_p99, P99_FLOOR_MS)

    intervals = []
    t = w
    while t + INTERVAL_S <= total + 1e-9:
        rs = [r for r in rows if t <= r[0] < t + INTERVAL_S]
        errs = [r for r in rs if r[4] != "ok"]
        p99 = percentile([r[3] for r in rs], 0.99)
        rate = (len(errs) / float(len(rs))) if rs else 1.0
        kinds = {}
        for r in errs:
            kinds[r[4]] = kinds.get(r[4], 0) + 1
        intervals.append({"start": t, "end": t + INTERVAL_S, "ops": len(rs), "p99_ms": round(p99, 1),
                          "errors": len(errs), "error_rate": round(rate, 4), "error_kinds": kinds,
                          "outside": bool(p99 > threshold or rate >= ERROR_LIMIT or not rs),
                          "phase": "baseline" if t + INTERVAL_S <= fault_start else ("fault" if t + INTERVAL_S <= fault_end else "recovery")})
        t += INTERVAL_S

    after = [iv for iv in intervals if iv["end"] > fault_start]   # an interval straddling the fault start counts as fault
    viol = [iv for iv in after if iv["outside"]]
    reasons = []
    if ledger.get("lost_writes"):
        reasons.append("%d lost write(s)" % ledger["lost_writes"])
    if ledger.get("zombies"):
        reasons.append("%d zombie(s)" % ledger["zombies"])
    if ledger.get("unverified_keys"):
        reasons.append("%d ledger key(s) could not be read at the end" % ledger["unverified_keys"])
    if end_state["nodes_up"] < end_state["nodes_expected"] or end_state.get("down"):
        reasons.append("node(s) not UN at the end: %s" % (end_state.get("down") or "view shows %d UN" % end_state["nodes_up"]))

    last_violation_end = max([iv["end"] for iv in viol], default=None)
    if misconfig:
        tail_start = total - tail_s
        tail_viol = [iv for iv in viol if iv["end"] > tail_start]
        if tail_viol:
            reasons.append("outside the service level in the last %d s (%d intervals)" % (tail_s, len(tail_viol)))
        base_verdict = "needs_operator" if tail_viol else ("recovered" if viol else "sailed")
    else:
        deadline_t = fault_end + recovery_deadline_s
        base_verdict = "sailed" if not viol else "recovered"
        if viol and last_violation_end > deadline_t:
            reasons.append("still outside the service level %d s after the fault ended (deadline %d s)"
                           % (last_violation_end - fault_end, recovery_deadline_s))
            base_verdict = "needs_operator"
    verdict = "needs_operator" if reasons else base_verdict

    kinds_total, ops_total = {}, 0
    for r in rows:
        if r[0] >= w:
            ops_total += 1
            if r[4] != "ok":
                kinds_total[r[4]] = kinds_total.get(r[4], 0) + 1
    return {
        "verdict": verdict,
        "reasons": reasons,
        "baseline_p99_ms": round(base_p99, 1),
        "p99_threshold_ms": round(threshold, 1),
        "worst_p99_ms": max([iv["p99_ms"] for iv in after], default=0.0),
        "worst_p99_baseline_phase_ms": max([iv["p99_ms"] for iv in intervals if iv["phase"] == "baseline"], default=0.0),
        "intervals_total": len(after),
        "intervals_outside": len(viol),
        "seconds_outside": len(viol) * INTERVAL_S,
        "seconds_to_recover_after_fault_end": (None if last_violation_end is None else max(0, last_violation_end - fault_end)),
        "baseline_intervals_outside": len([iv for iv in intervals if iv["phase"] == "baseline" and iv["outside"]]),
        "probe_ops_measured": ops_total,
        "errors_by_kind": kinds_total,
        "intervals": intervals,
    }
