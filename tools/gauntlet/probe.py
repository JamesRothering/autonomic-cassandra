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

"""Probe and ledger client for the gauntlet.

A fixed-rate open-loop client (default 40 ops/s: 50% insert of a new ledger key, 30% read of an earlier acknowledged
key, 20% delete of an earlier acknowledged key) at QUORUM with a 2 s client timeout. Every operation is logged with its
submit time, latency and outcome class. The ledger records what the cluster acknowledged; at the end every ledger key
is read at QUORUM: an acknowledged insert that is missing is a lost write, an acknowledged delete whose key returns is a
zombie. An insert or delete that did not complete is "unknown", reported separately, never counted as lost.

Uses the driver bundled with cqlsh (lib/cassandra-driver-internal-only-*.zip), put on sys.path as bin/cqlsh.py does.
"""

import glob
import os
import random
import sys
import threading
import time

OUTCOMES = ("ok", "client_timeout", "server_read_timeout", "server_write_timeout", "unavailable", "overloaded", "other")


def import_driver(home):
    """Put the bundled driver and its pure-python dependencies on sys.path (as bin/cqlsh.py does)."""
    lib = os.path.join(home, "lib")
    z = max(glob.glob(os.path.join(lib, "cassandra-driver-internal-only-*.zip")))
    ver = os.path.splitext(os.path.basename(z))[0][len("cassandra-driver-internal-only-"):]
    sys.path.insert(0, os.path.join(z, "cassandra-driver-" + ver))
    for prefix in ("pure_sasl-", "wcwidth-", "pyasyncore-"):
        zs = glob.glob(os.path.join(lib, prefix + "*.zip"))
        if zs:
            sys.path.insert(0, max(zs))
    import cassandra  # noqa: F401  (fails loudly here if the driver cannot be imported)
    return cassandra


def classify(exc):
    n = type(exc).__name__
    s = str(exc)
    if n == "OperationTimedOut":
        return "client_timeout"
    if n == "ReadTimeout":
        return "server_read_timeout"
    if n == "WriteTimeout":
        return "server_write_timeout"
    if n == "Unavailable":
        return "unavailable"
    if "Overloaded" in n or "overloaded" in s.lower():
        return "overloaded"
    if n == "NoHostAvailable":
        errs = getattr(exc, "errors", {}) or {}
        kinds = {classify(e) for e in errs.values() if isinstance(e, BaseException)}
        for k in ("client_timeout", "overloaded", "unavailable", "server_read_timeout", "server_write_timeout"):
            if k in kinds:
                return k
        return "other"
    return "other"


class Probe:
    def __init__(self, home, rate=40, timeout=2.0, seed=1, hosts=("127.0.0.1", "127.0.0.2", "127.0.0.3")):
        self.cassandra = import_driver(home)
        self.rate, self.timeout, self.hosts = rate, timeout, list(hosts)
        self.rng = random.Random(seed)
        self.lock = threading.Lock()
        self.log = []                      # (t_submit_rel, op, key, latency_ms, outcome)
        self.ledger = {}                   # key -> state: acked | insert_unknown | deleted | delete_unknown
        self.readable = []                 # acked, not yet touched by a delete
        self.stale_reads = 0               # acknowledged key read at QUORUM and not found mid-run
        self.counter = 0
        self.t0 = None
        self.stop_flag = threading.Event()
        self.thread = None
        self.cluster = self.session = None
        self.prefix = "k%d-" % int(time.time())

    def connect(self):
        from cassandra import ConsistencyLevel
        from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
        prof = ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM, request_timeout=self.timeout)
        self.cluster = Cluster(self.hosts, port=9042, execution_profiles={EXEC_PROFILE_DEFAULT: prof},
                               connect_timeout=10, control_connection_timeout=10, protocol_version=5)
        self.session = self.cluster.connect()
        s = self.session
        s.execute("CREATE KEYSPACE IF NOT EXISTS gauntlet_probe WITH replication = "
                  "{'class': 'NetworkTopologyStrategy', 'datacenter1': 3}", timeout=30)
        s.execute("CREATE TABLE IF NOT EXISTS gauntlet_probe.ledger (k text PRIMARY KEY, v text)", timeout=30)
        self.ins = s.prepare("INSERT INTO gauntlet_probe.ledger (k, v) VALUES (?, ?)")
        self.sel = s.prepare("SELECT k, v FROM gauntlet_probe.ledger WHERE k = ?")
        self.dele = s.prepare("DELETE FROM gauntlet_probe.ledger WHERE k = ?")

    # ---- operations

    def _record(self, t_rel, op, key, t_start, outcome):
        with self.lock:
            self.log.append((t_rel, op, key, (time.time() - t_start) * 1000.0, outcome))

    def _submit(self, op, key, stmt, args, on_ok, on_unknown):
        t_start = time.time()
        t_rel = t_start - self.t0
        try:
            fut = self.session.execute_async(stmt, args, timeout=self.timeout)
        except Exception as e:                       # synchronous failure (e.g. no connection)
            self._record(t_rel, op, key, t_start, classify(e))
            on_unknown()
            return

        def ok(rows):
            self._record(t_rel, op, key, t_start, "ok")
            on_ok(rows)

        def err(e):
            self._record(t_rel, op, key, t_start, classify(e))
            on_unknown()
        fut.add_callbacks(ok, err)

    def _one(self):
        r = self.rng.random()
        with self.lock:
            have = len(self.readable)
        if r < 0.5 or have < 10:
            self.counter += 1
            key = "%s%d" % (self.prefix, self.counter)
            with self.lock:
                self.ledger[key] = "insert_unknown"

            def ok(_):
                with self.lock:
                    self.ledger[key] = "acked"
                    self.readable.append(key)
            self._submit("insert", key, self.ins, (key, "v" * 32), ok, lambda: None)
        elif r < 0.8:
            with self.lock:
                key = self.readable[self.rng.randrange(len(self.readable))]

            def ok(rows):
                if not list(rows):
                    with self.lock:
                        self.stale_reads += 1
            self._submit("read", key, self.sel, (key,), ok, lambda: None)
        else:
            with self.lock:
                i = self.rng.randrange(len(self.readable))
                self.readable[i], self.readable[-1] = self.readable[-1], self.readable[i]
                key = self.readable.pop()
                self.ledger[key] = "delete_unknown"

            def ok(_):
                with self.lock:
                    self.ledger[key] = "deleted"
            self._submit("delete", key, self.dele, (key,), ok, lambda: None)

    def _loop(self):
        n = 0
        while not self.stop_flag.is_set():
            due = self.t0 + n / float(self.rate)
            delay = due - time.time()
            if delay > 0:
                if self.stop_flag.wait(delay):
                    break
            self._one()
            n += 1

    def start(self):
        self.t0 = time.time()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_flag.set()
        if self.thread:
            self.thread.join(10)
        time.sleep(self.timeout + 1.0)             # let in-flight operations finish or time out

    # ---- final check

    def verify(self, deadline_s=240, parallel=32):
        """Read every ledger key at QUORUM (with retries). Returns the ledger verdict numbers."""
        keys = list(self.ledger.items())
        found, errors = {}, {}
        pending = [k for k, _ in keys]
        t_end = time.time() + deadline_s
        attempt = 0
        while pending and time.time() < t_end and attempt < 6:
            attempt += 1
            nxt = []
            sem = threading.Semaphore(parallel)
            res = {}
            done = threading.Event()
            remaining = [len(pending)]
            lk = threading.Lock()

            def finish(k, val):
                res[k] = val
                with lk:
                    remaining[0] -= 1
                    if remaining[0] == 0:
                        done.set()
                sem.release()
            for k in pending:
                sem.acquire()
                try:
                    f = self.session.execute_async(self.sel, (k,), timeout=10.0)
                except Exception as e:
                    finish(k, e)
                    continue
                f.add_callbacks(lambda rows, k=k: finish(k, bool(list(rows))), lambda e, k=k: finish(k, e))
            done.wait(max(5, t_end - time.time()))
            for k in pending:
                v = res.get(k)
                if isinstance(v, bool):
                    found[k] = v
                else:
                    nxt.append(k)
                    errors[k] = str(v)[:100]
            pending = nxt
            if pending:
                time.sleep(3)
        lost, zombies, unknown_present, unknown_absent = [], [], 0, 0
        for k, st in keys:
            if k not in found:
                continue
            f = found[k]
            if st == "acked" and not f:
                lost.append(k)
            elif st == "deleted" and f:
                zombies.append(k)
            elif st in ("insert_unknown", "delete_unknown"):
                if f:
                    unknown_present += 1
                else:
                    unknown_absent += 1
        counts = {}
        for _, st in keys:
            counts[st] = counts.get(st, 0) + 1
        return {"ledger_keys": len(keys), "ledger_states": counts, "lost_writes": len(lost), "lost_examples": lost[:5],
                "zombies": len(zombies), "zombie_examples": zombies[:5],
                "unknown_ops_present": unknown_present, "unknown_ops_absent": unknown_absent,
                "unverified_keys": len(pending), "stale_reads_midrun": self.stale_reads}

    def write_log(self, path):
        with open(path, "w") as f:
            f.write("t_rel_s,op,key,latency_ms,outcome\n")
            for t, op, key, lat, out in sorted(self.log):
                f.write("%.3f,%s,%s,%.2f,%s\n" % (t, op, key, lat, out))

    def close(self):
        try:
            if self.cluster:
                self.cluster.shutdown()
        except Exception:
            pass
