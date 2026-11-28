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

"""The gauntlet: run one scenario against one subject on a real 3-node cluster, or a matrix of them.

  gauntlet.py run    --subject stock|autonomic --scenario scenarios/S0.yaml --out build/gauntlet/<run-id> [--quick]
  gauntlet.py matrix --subjects stock,autonomic --scenarios S0,S1,... --out build/gauntlet/<run-id> [--quick]

See README.md and docs/plan/eval-harness.md. Standard library plus the driver bundled with cqlsh.
"""

import argparse
import atexit
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import cluster as cl          # noqa: E402
import miniyaml               # noqa: E402
import probe as pr            # noqa: E402
import verdict as vd          # noqa: E402

HOME = os.path.abspath(os.path.join(HERE, "..", ".."))
STRESS = os.path.join(HOME, "tools", "bin", "cassandra-stress")

# Baseline load: a constant, not a calibration. 500 ops/s of 50/50 read/write on RF 3 is about 1500 replica operations
# per second across three 512 MiB nodes on a 4-core box: busy enough to show latency under faults, far from saturation
# (checked in the smoke runs, see README.md). The spike scenario adds 2x this rate for the fault window.
DEFAULT_RATE = 500

TIMELINES = {
    "full": {"warmup": 60, "baseline": 90, "fault": 90, "recovery": 240, "deadline": 120},
    "quick": {"warmup": 30, "baseline": 45, "fault": 45, "recovery": 90, "deadline": 45},
}

SUBJECT_AUTONOMIC = {
    "stock": {"enabled": False},
    "autonomic": {"enabled": True, "dry_run": False, "startup_grace": "30s", "safety": {"cluster_approval": "manual"}},
    # The layer with its in-node actuators on (all are off by default, AutonomicConfig.defaultActuators); option values
    # repeat the defaults because a YAML map replaces the whole default map. Scale-out stays off: cluster actions
    # wait for manual approval, which nobody gives during a run.
    "autonomic-armed": {"enabled": True, "dry_run": False, "startup_grace": "30s", "safety": {"cluster_approval": "manual"},
                        "actuators": {"compaction_throughput": {"enabled": True, "min_factor": 0.25, "max_factor": 4.0},
                                      "request_pools": {"enabled": True, "min_factor": 0.5, "max_factor": 2.0},
                                      "request_throttle": {"enabled": True, "min_requests_per_second": 1000},
                                      "scale_out": {"enabled": False},
                                      "partition_split": {"enabled": True},
                                      "partition_unsplit": {"enabled": True}}},
}

_live = {"clusters": [], "stress": []}


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def cleanup_all():
    for p in list(_live["stress"]):
        kill_stress(p)
    for c in list(_live["clusters"]):
        try:
            c.teardown()
        except Exception:
            pass


atexit.register(cleanup_all)


def _sig(signum, frame):
    raise KeyboardInterrupt("signal %d" % signum)


def kill_stress(p):
    try:
        os.killpg(p.pid, signal.SIGKILL)
    except OSError:
        pass
    try:
        p.wait(timeout=5)
    except Exception:
        pass
    if p in _live["stress"]:
        _live["stress"].remove(p)


# ---- load

def start_stress(spec, duration, out_path):
    prof = os.path.join(HERE, "profiles", spec["profile"] + ".yaml")
    rate = int(spec.get("rate", DEFAULT_RATE))
    threads = max(16, rate // 10)
    cmd = [STRESS, "user", "profile=" + prof, spec["ops"], "cl=QUORUM", "no-warmup", "duration=%ds" % duration,
           "-rate", "threads=%d" % threads, "fixed=%d/s" % rate, "-errors", "ignore", "-log", "interval=5",
           "-node", "127.0.0.1,127.0.0.2,127.0.0.3"]
    env = dict(os.environ, CASSANDRA_HOME=HOME, JVM_OPTS="-Xmx256M -Xms64M")
    env.pop("CASSANDRA_CONF", None)
    f = open(out_path, "w")
    p = subprocess.Popen(cmd, env=env, stdout=f, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, start_new_session=True)
    _live["stress"].append(p)
    return p


def parse_stress(path, target_rate):
    """Summarise cassandra-stress interval lines: 'total, op/s, pk/s, row/s, mean, med, .95, .99, .999, max, time, stderr, errors, ...'."""
    rows = []
    try:
        with open(path, errors="replace") as f:
            for line in f:
                m = re.match(r"^total,\s*(\d+),\s*(\d+),\s*\d+,\s*\d+,\s*([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*(\d+)", line)
                if m:
                    rows.append({"time_s": float(m.group(9)), "ops_s": int(m.group(2)), "p99_ms": float(m.group(6)),
                                 "max_ms": float(m.group(8)), "errors": int(m.group(11))})
    except OSError:
        pass
    out = {"intervals": len(rows)}
    if rows:
        out.update({"mean_ops_s": round(sum(r["ops_s"] for r in rows) / float(len(rows))),
                    "min_ops_s": min(r["ops_s"] for r in rows), "worst_p99_ms": max(r["p99_ms"] for r in rows),
                    "errors_total": sum(r["errors"] for r in rows),
                    "intervals_below_half_target": len([r for r in rows if r["ops_s"] < target_rate / 2.0])})
    return out


# ---- faults

def apply_fault(c, f, events, t0):
    a, nd = f["action"], c.node(int(f.get("node", 1)))
    rc, out = 0, ""
    if a == "freeze":
        nd.freeze()
    elif a == "thaw":
        nd.thaw()
    elif a == "kill":
        nd.kill9()
    elif a == "start":
        nd.start(wait=False)
    elif a == "cpu_quota":
        rc, out = nd.set_cpu_quota(int(f["percent"]))
    elif a == "memory_max":
        rc, out = nd.set_memory_max(int(f["mb"]))
    elif a == "nodetool":
        rc, out = nd.nodetool(*str(f["args"]).split())
    else:
        raise ValueError("unknown fault action %r" % a)
    events.append({"t": round(time.time() - t0, 1), "action": a, "node": nd.i, "detail": {k: v for k, v in f.items() if k not in ("at", "quick_at", "action", "node")}, "rc": rc, "out": out.strip()[-200:]})
    log("fault %s node %d (rc %s)" % (a, nd.i, rc))


def disk_free_mb(path):
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize // (1024 * 1024)


def mem_used_mb():
    d = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":")
            d[k] = int(v.split()[0])
    return (d["MemTotal"] - d["MemAvailable"]) // 1024


class Sampler(threading.Thread):
    def __init__(self, c):
        super().__init__(daemon=True)
        self.c, self.stop_flag = c, threading.Event()
        self.peak_used, self.peak_node_rss = 0, 0

    def run(self):
        while not self.stop_flag.wait(5):
            try:
                self.peak_used = max(self.peak_used, mem_used_mb())
                self.peak_node_rss = max(self.peak_node_rss, self.c.rss_total_mb())
            except Exception:
                pass


# ---- one run

def per_node_config(sc, subject):
    cfg = {0: {"yaml": dict((sc.get("node_config") or {}).get("all") or {})}}
    for k, v in (sc.get("node_config") or {}).items():
        m = re.fullmatch(r"n(\d)", k)
        if m:
            cfg[int(m.group(1))] = {"yaml": dict(v or {})}
    cfg[0]["autonomic"] = SUBJECT_AUTONOMIC[subject]
    return cfg


def run_one(subject, scenario_path, out, quick=False, probe_rate=40, heap="512M", new_size="128M", keep_data=False):
    sc = miniyaml.load_file(scenario_path)
    name = sc["name"]
    base = TIMELINES["quick" if quick else "full"]
    misconfig = bool(sc.get("misconfig"))
    tl = {"warmup": base["warmup"], "baseline": base["baseline"],
          "fault": base["fault"] if quick else int(sc.get("fault_len", base["fault"])), "recovery": base["recovery"]}
    if misconfig:
        # the misconfiguration is in place from the start; the "fault" phase is the rest of the run
        tl["fault"] = tl["recovery"] = 0
        tl["rest"] = base["fault"] + base["recovery"] if quick else 330
    tl["total"] = tl["warmup"] + tl["baseline"] + tl["fault"] + tl["recovery"] + tl.get("rest", 0)
    deadline = int(sc.get("recovery_deadline", base["deadline"]))
    tail_s = int(0.4 * tl["total"]) if quick else 120

    out = os.path.abspath(out)
    os.makedirs(out, exist_ok=True)
    result = {"subject": subject, "scenario": name, "title": sc.get("title"), "category": sc.get("category"),
              "quick": quick, "timeline": tl, "recovery_deadline_s": deadline, "probe_rate": probe_rate,
              "heap": heap, "load": sc.get("load"), "started": time.strftime("%Y-%m-%dT%H:%M:%S"), "events": []}
    free0 = disk_free_mb(out)
    c = cl.Cluster(HOME, os.path.join(out, "cluster"), heap=heap, new_size=new_size)
    _live["clusters"].append(c)
    sampler = Sampler(c)
    p = None
    stress_procs = []
    t_run = time.time()
    try:
        log("%s/%s: creating cluster (heap %s)" % (subject, name, heap))
        c.create(per_node_config(sc, subject))
        for n in c.nodes:
            t = time.time()
            n.start()
            log("node %d UN after %d s" % (n.i, time.time() - t))
        result["cluster_ready_s"] = round(time.time() - t_run)
        sampler.start()
        p = pr.Probe(HOME, rate=probe_rate)
        p.connect()

        stress_log = []
        spec = sc["load"]
        base_p = start_stress(spec, tl["total"] + 5, os.path.join(out, "stress-base.log"))
        stress_procs.append(("base", base_p, os.path.join(out, "stress-base.log"), spec))
        p.start()
        t0 = p.t0
        log("probe and load started; timeline %s" % json.dumps(tl))

        fault_start = tl["warmup"] + tl["baseline"]
        events = []
        todo = []
        for f in sc.get("faults") or []:
            at = f.get("quick_at", f["at"]) if quick else f["at"]
            todo.append((fault_start + at, "f", f))
        if sc.get("extra_load"):
            todo.append((fault_start, "x", sc["extra_load"]))
            todo.append((fault_start + tl["fault"], "xs", None))
        todo.sort(key=lambda x: x[0])
        for at, kind, f in todo:
            d = t0 + at - time.time()
            if d > 0:
                time.sleep(d)
            if kind == "f":
                apply_fault(c, f, events, t0)
            elif kind == "x":
                path = os.path.join(out, "stress-extra.log")
                xp = start_stress(f, tl["fault"], path)
                stress_procs.append(("extra", xp, path, f))
                events.append({"t": round(time.time() - t0, 1), "action": "extra_load_start", "rate": f["rate"]})
                log("extra load %s ops/s started" % f["rate"])
            else:
                events.append({"t": round(time.time() - t0, 1), "action": "extra_load_end"})
        d = t0 + tl["total"] - time.time()
        if d > 0:
            time.sleep(d)
        result["events"] = events
        for n in c.nodes:
            n.thaw()                                  # never leave a node frozen for the final checks
        p.stop()
        log("probe stopped: %d ops logged; checking cluster" % len(p.log))
        for _, sp, _, _ in stress_procs:
            kill_stress(sp)

        # end state: a node that is running but still starting gets up to 90 s to be UN
        end = {"nodes_expected": cl.NODE_COUNT, "down": []}
        deadline_t = time.time() + 90
        while True:
            down = [n.i for n in c.nodes if not (n.alive() and n.up_normal_in_view())]
            if not down or time.time() > deadline_t or all(not c.node(i).alive() for i in down):
                break
            time.sleep(5)
        end["down"] = down
        end["nodes_up"] = cl.NODE_COUNT - len(down)
        result["end_state"] = end

        if down and len(down) < cl.NODE_COUNT:
            log("nodes down at the end: %s" % down)
        ledger = p.verify() if len(down) < 2 else {"lost_writes": 0, "zombies": 0, "unverified_keys": len(p.ledger),
                                                    "ledger_keys": len(p.ledger), "note": "QUORUM not possible at the end"}
        result["ledger"] = ledger
        p.write_log(os.path.join(out, "probe.csv"))
        v = vd.evaluate(p.log, tl, ledger, end, misconfig, deadline, tail_s)
        result.update({"verdict": v.pop("verdict"), "service": v})

        result["stress"] = {nm: parse_stress(path, int(sp_spec.get("rate", DEFAULT_RATE))) for nm, _, path, sp_spec in stress_procs}

        if subject.startswith("autonomic"):
            result["layer"] = collect_layer(c, out)
        result["resources"] = {"peak_machine_used_mb": sampler.peak_used, "peak_node_rss_total_mb": sampler.peak_node_rss}
        log("%s/%s verdict: %s %s" % (subject, name, result["verdict"], "; ".join(v["reasons"])))
    except BaseException as e:
        result["error"] = "%s: %s" % (type(e).__name__, e)
        result["verdict"] = "harness_error"
        log("ERROR %s" % result["error"])
        if isinstance(e, KeyboardInterrupt):
            raise
    finally:
        sampler.stop_flag.set()
        for _, sp, _, _ in stress_procs:
            kill_stress(sp)
        if p:
            p.close()
        c.teardown()
        if not keep_data:
            c.remove_data()
        if c in _live["clusters"]:
            _live["clusters"].remove(c)
        result["run_seconds"] = round(time.time() - t_run)
        result["disk_free_mb_before"] = free0
        result["disk_free_mb_after"] = disk_free_mb(out)
        with open(os.path.join(out, "result.json"), "w") as f:
            json.dump(result, f, indent=1, default=str)
    return result


def collect_layer(c, out):
    summary = {"nodes": {}}
    for n in c.nodes:
        if not n.alive():
            summary["nodes"][n.i] = {"note": "node not running at the end"}
            continue
        rc, hist = n.nodetool("autonomic", "history", timeout=60)
        rc2, diag = n.nodetool("autonomic", "history", "--diagnoses", timeout=60)
        with open(os.path.join(out, "autonomic-history-node%d.txt" % n.i), "w") as f:
            f.write("# nodetool autonomic history (rc %d)\n%s\n# nodetool autonomic history --diagnoses (rc %d)\n%s\n" % (rc, hist, rc2, diag))
        actions = [l for l in hist.splitlines() if "actuator=" in l]
        diags = [l for l in diag.splitlines() if l.strip() and not l.startswith(("#", "Diagnoses"))]
        kinds = {}
        for l in actions:
            m = re.search(r"actuator=(\S+)", l)
            kinds[m.group(1)] = kinds.get(m.group(1), 0) + 1
        dk = {}
        for l in diag.splitlines():
            m = re.search(r"\b(type|diagnosis)=(\S+)", l)
            if m:
                dk[m.group(2)] = dk.get(m.group(2), 0) + 1
        summary["nodes"][n.i] = {"history_rc": rc, "actions": len(actions), "actions_by_actuator": kinds,
                                 "diagnosis_lines": len(diags), "diagnoses_by_type": dk}
    summary["actions_total"] = sum(v.get("actions", 0) for v in summary["nodes"].values())
    return summary


# ---- report

def fmt_cell(r):
    if r.get("verdict") == "harness_error":
        return "HARNESS ERROR"
    s = r["service"]
    lg = r["ledger"]
    bad = ""
    if lg.get("lost_writes") or lg.get("zombies"):
        bad = " LOST %d ZOMBIE %d" % (lg.get("lost_writes", 0), lg.get("zombies", 0))
    return "%s (p99 %s/%s ms, %ds out%s)" % (r["verdict"], s["worst_p99_ms"], s["baseline_p99_ms"], s["seconds_outside"], bad)


def write_report(results, out):
    scs = []
    subs = []
    for r in results:
        if r["scenario"] not in scs:
            scs.append(r["scenario"])
        if r["subject"] not in subs:
            subs.append(r["subject"])
    by = {(r["subject"], r["scenario"]): r for r in results}
    L = ["# Gauntlet report", "", "Run directory: `%s`. Quick timeline: %s. Cell: verdict (worst p99 / baseline p99, seconds outside the service level, ledger problems)." % (out, any(r.get("quick") for r in results)), "",
         "| scenario | " + " | ".join(subs) + " |", "|---|" + "---|" * len(subs)]
    for s in scs:
        t = next(r for r in results if r["scenario"] == s).get("title") or ""
        L.append("| %s %s | " % (s, t) + " | ".join(fmt_cell(by[(u, s)]) if (u, s) in by else "-" for u in subs) + " |")
    L += ["", "## Numbers", "", "| scenario | subject | verdict | baseline p99 ms | worst p99 ms | intervals outside | errors by kind | lost | zombie | unknown (present/absent) | stale reads | layer actions | run s |", "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in results:
        if r.get("verdict") == "harness_error":
            L.append("| %s | %s | HARNESS ERROR: %s |||||||||||" % (r["scenario"], r["subject"], r.get("error")))
            continue
        s, lg = r["service"], r["ledger"]
        L.append("| %s | %s | %s | %s | %s | %d/%d | %s | %s | %s | %s/%s | %s | %s | %s |" % (
            r["scenario"], r["subject"], r["verdict"], s["baseline_p99_ms"], s["worst_p99_ms"], s["intervals_outside"], s["intervals_total"],
            json.dumps(s["errors_by_kind"]) if s["errors_by_kind"] else "none", lg.get("lost_writes"), lg.get("zombies"),
            lg.get("unknown_ops_present"), lg.get("unknown_ops_absent"), lg.get("stale_reads_midrun"),
            (r.get("layer") or {}).get("actions_total", "-"), r.get("run_seconds")))
    L += ["", "## What differed", ""]
    diffs = 0
    for s in scs:
        rs = [by[(u, s)] for u in subs if (u, s) in by and by[(u, s)].get("verdict") != "harness_error"]
        if len(rs) < 2:
            continue
        a, b = rs[0], rs[1]
        notes = []
        if a["verdict"] != b["verdict"]:
            notes.append("verdict %s=%s, %s=%s" % (a["subject"], a["verdict"], b["subject"], b["verdict"]))
        for k, label in (("seconds_outside", "seconds outside the service level"), ("worst_p99_ms", "worst p99 ms")):
            x, y = a["service"][k], b["service"][k]
            if x != y and (abs(x - y) > 0.25 * max(x, y, 1)):
                notes.append("%s: %s=%s, %s=%s" % (label, a["subject"], x, b["subject"], y))
        for r in (a, b):
            if r["ledger"].get("lost_writes") or r["ledger"].get("zombies"):
                notes.append("%s has lost writes/zombies" % r["subject"])
            if (r.get("layer") or {}).get("actions_total"):
                notes.append("%s layer actions: %s" % (r["subject"], json.dumps({k: v["actions_by_actuator"] for k, v in r["layer"]["nodes"].items() if v.get("actions")})))
        if notes:
            diffs += 1
            L.append("- **%s**: " % s + "; ".join(notes))
    if not diffs:
        L.append("Nothing differed beyond noise (one run each; a single run is evidence, not proof).")
    L += ["", "A single run is evidence, not proof: repeat each pair at least 3 times before reporting a difference as a finding."]
    with open(os.path.join(out, "report.md"), "w") as f:
        f.write("\n".join(L) + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for nm in ("run", "matrix"):
        s = sub.add_parser(nm)
        s.add_argument("--out", required=True)
        s.add_argument("--quick", action="store_true", help="short timeline: warmup 30 s, baseline 45 s, fault 45 s, recovery 90 s")
        s.add_argument("--probe-rate", type=int, default=40)
        s.add_argument("--heap", default="512M")
        s.add_argument("--new-size", default="128M")
        s.add_argument("--keep-data", action="store_true")
        if nm == "run":
            s.add_argument("--subject", required=True, choices=sorted(SUBJECT_AUTONOMIC))
            s.add_argument("--scenario", required=True)
        else:
            s.add_argument("--subjects", default="stock,autonomic")
            s.add_argument("--scenarios", default="S0,S1,S2,S3,S4,S5,S6,S7")
    a = ap.parse_args()
    signal.signal(signal.SIGTERM, _sig)
    kw = dict(quick=a.quick, probe_rate=a.probe_rate, heap=a.heap, new_size=a.new_size, keep_data=a.keep_data)
    if a.cmd == "run":
        r = run_one(a.subject, a.scenario, a.out, **kw)
        write_report([r], os.path.abspath(a.out))
        print(json.dumps({k: r.get(k) for k in ("subject", "scenario", "verdict", "error")}))
        return 0 if r.get("verdict") != "harness_error" else 1
    results = []
    out = os.path.abspath(a.out)
    for sc in a.scenarios.split(","):
        for sj in a.subjects.split(","):
            path = sc if os.path.exists(sc) else os.path.join(HERE, "scenarios", sc + ".yaml")
            results.append(run_one(sj, path, os.path.join(out, "%s-%s" % (sj, os.path.splitext(os.path.basename(path))[0])), **kw))
            write_report(results, out)
    return 0


if __name__ == "__main__":
    rc = 1
    try:
        rc = main()
    finally:
        cleanup_all()
    sys.exit(rc)
