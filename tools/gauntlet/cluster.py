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

"""Cluster control for the gauntlet: a real 3-node Cassandra cluster on 127.0.0.1-3 from scratch conf copies.

Each node runs inside its own systemd user scope so CPU quota and memory limits can be changed at run time
(`systemctl --user set-property`), which needs no root. Teardown is idempotent and is called from every exit path.
"""

import os
import re
import signal
import subprocess
import time

NODE_COUNT = 3
JMX_BASE = 7199          # node i listens for JMX on JMX_BASE + 100 * (i - 1)


def addr(i):
    return "127.0.0.%d" % i


def jmx_port(i):
    return JMX_BASE + 100 * (i - 1)


def run(cmd, timeout=120, env=None, check=False):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout, env=env)
    if check and p.returncode != 0:
        raise RuntimeError("%s failed (%d): %s" % (cmd, p.returncode, p.stdout[-2000:]))
    return p.returncode, p.stdout


def strip_top_level_keys(text, keys):
    """Remove the active top-level blocks named in keys from a cassandra.yaml text (comments are kept)."""
    out, skipping = [], False
    for line in text.splitlines():
        m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*:", line)
        if m:
            skipping = m.group(1) in keys
        elif skipping and line.strip() and not line.startswith((" ", "\t", "#", "-")):
            skipping = False
        if not skipping:
            out.append(line)
    return "\n".join(out) + "\n"


def yaml_value(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, dict):
        return "{" + ", ".join("%s: %s" % (k, yaml_value(x)) for k, x in v.items()) + "}"
    if isinstance(v, list):
        return "[" + ", ".join(yaml_value(x) for x in v) + "]"
    return str(v)


class Node:
    def __init__(self, i, cluster):
        self.i = i
        self.c = cluster
        self.addr = addr(i)
        self.jmx = jmx_port(i)
        self.dir = os.path.join(cluster.root, "node%d" % i)
        self.conf = os.path.join(self.dir, "conf")
        self.logs = os.path.join(self.dir, "logs")
        self.storage = os.path.join(self.dir, "storage")
        self.pidfile = os.path.join(self.dir, "cassandra.pid")
        self.unit = "%s-n%d" % (cluster.unit_prefix, i)
        self.proc = None
        self.starts = 0

    # ---- setup

    def write_conf(self, overrides):
        c = self.c
        os.makedirs(self.logs, exist_ok=True)
        os.makedirs(self.storage, exist_ok=True)
        os.makedirs(self.conf, exist_ok=True)
        for f in os.listdir(os.path.join(c.home, "conf")):
            src = os.path.join(c.home, "conf", f)
            if os.path.isfile(src):
                with open(src, "rb") as a, open(os.path.join(self.conf, f), "wb") as b:
                    b.write(a.read())
        env_sh = os.path.join(self.conf, "cassandra-env.sh")
        with open(env_sh) as f:
            t = f.read()
        t = t.replace("configure_jmx 7199", "configure_jmx %d" % self.jmx)
        with open(env_sh, "w") as f:
            f.write(t)
        s = self.storage
        base = {
            "cluster_name": "gauntlet",
            "listen_address": self.addr,
            "rpc_address": self.addr,
            "data_file_directories": [s + "/data"],
            "commitlog_directory": s + "/commitlog",
            "saved_caches_directory": s + "/saved_caches",
            "hints_directory": s + "/hints",
            "cdc_raw_directory": s + "/cdc_raw",
            "native_transport_port": 9042,
            "endpoint_snitch": "SimpleSnitch",
            "seed_provider": [{"class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                               "parameters": [{"seeds": "127.0.0.1:7000"}]}],
        }
        base.update(overrides.get("yaml", {}))
        scalars = {k: v for k, v in base.items() if k not in ("seed_provider", "autonomic")}
        path = os.path.join(self.conf, "cassandra.yaml")
        with open(path) as f:
            text = strip_top_level_keys(f.read(), set(base) | {"accord", "autonomic"})
        lines = ["", "# gauntlet overrides"]
        for k, v in scalars.items():
            lines.append("%s: %s" % (k, yaml_value(v)))
        lines.append("seed_provider:\n  - class_name: org.apache.cassandra.locator.SimpleSeedProvider\n    parameters:\n      - seeds: \"127.0.0.1:7000\"")
        lines.append("accord:\n  journal_directory: %s/accord_journal" % s)
        a = overrides.get("autonomic")
        if a:
            lines.append("autonomic:")
            for k, v in a.items():
                lines.append("  %s: %s" % (k, yaml_value(v)))
        with open(path, "w") as f:
            f.write(text + "\n".join(lines) + "\n")

    # ---- process control

    def env(self):
        e = dict(os.environ)
        e.update({"CASSANDRA_HOME": self.c.home, "CASSANDRA_CONF": self.conf, "CASSANDRA_LOG_DIR": self.logs,
                  "MAX_HEAP_SIZE": self.c.heap, "HEAP_NEWSIZE": self.c.new_size,
                  "JVM_EXTRA_OPTS": "-Dcassandra.ring_delay_ms=10000"})
        return e

    def pid(self):
        try:
            with open(self.pidfile) as f:
                return int(f.read().strip())
        except (OSError, ValueError):
            return self.proc.pid if self.proc else None

    def alive(self):
        p = self.pid()
        if not p:
            return False
        try:
            os.kill(p, 0)
            with open("/proc/%d/stat" % p) as f:
                return f.read().split(") ")[-1][0] != "Z"
        except (OSError, ValueError):
            return False

    def start(self, wait=True, timeout=240):
        """Start (or restart) the node in a fresh systemd scope."""
        self.starts += 1
        self.unit = "%s-n%d-%d" % (self.c.unit_prefix, self.i, self.starts)
        try:
            os.remove(self.pidfile)
        except OSError:
            pass
        out = open(os.path.join(self.dir, "stdout-%d.log" % self.starts), "w")
        cmd = ["systemd-run", "--user", "--scope", "-q", "--unit=" + self.unit,
               os.path.join(self.c.home, "bin", "cassandra"), "-f", "-p", self.pidfile]
        self.proc = subprocess.Popen(cmd, env=self.env(), stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                     start_new_session=True)
        if wait:
            self.wait_ready(timeout)

    def wait_ready(self, timeout=240):
        t0 = time.time()
        log = os.path.join(self.logs, "system.log")
        while time.time() - t0 < timeout:
            if self.proc.poll() is not None and not self.alive():
                raise RuntimeError("node %d exited during start (rc %s); see %s" % (self.i, self.proc.returncode, self.dir))
            try:
                with open(log, errors="replace") as f:
                    txt = f.read()
                # a restart appends to the same log: look only past the last start marker
                idx = txt.rfind("Cassandra version")
                if "Starting listening for CQL clients" in txt[idx if idx >= 0 else 0:]:
                    break
            except OSError:
                pass
            time.sleep(2)
        else:
            raise RuntimeError("node %d not listening for CQL within %d s" % (self.i, timeout))
        while time.time() - t0 < timeout:
            if self.up_normal_in_view():
                return time.time() - t0
            time.sleep(2)
        raise RuntimeError("node %d never reached UN" % self.i)

    def up_normal_in_view(self):
        rc, out = self.nodetool("status", timeout=60)
        return rc == 0 and re.search(r"^UN\s+%s\b" % re.escape(self.addr), out, re.M) is not None

    def signal(self, sig):
        p = self.pid()
        if p:
            try:
                os.kill(p, sig)
            except OSError:
                pass

    def freeze(self):
        self.signal(signal.SIGSTOP)

    def thaw(self):
        self.signal(signal.SIGCONT)

    def kill9(self):
        self.signal(signal.SIGCONT)
        self.signal(signal.SIGKILL)

    def stop(self):
        """Orderly stop, then kill; never raises."""
        p = self.pid()
        if p and self.alive():
            self.signal(signal.SIGCONT)
            self.signal(signal.SIGTERM)
            for _ in range(40):
                if not self.alive():
                    break
                time.sleep(0.5)
            self.kill9()
        subprocess.run(["systemctl", "--user", "stop", self.unit + ".scope"], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)

    def set_property(self, **props):
        args = ["%s=%s" % (k, v) for k, v in props.items()]
        return run(["systemctl", "--user", "set-property", "--runtime", self.unit + ".scope"] + args)

    def set_cpu_quota(self, pct):
        return self.set_property(CPUQuota=("%d%%" % pct) if pct else "")

    def set_memory_max(self, mb):
        return self.set_property(MemoryMax=("%dM" % mb) if mb else "infinity")

    def nodetool(self, *args, timeout=120):
        try:
            return run([os.path.join(self.c.home, "bin", "nodetool"), "-h", "127.0.0.1", "-p", str(self.jmx)] + list(args),
                       timeout=timeout)
        except subprocess.TimeoutExpired:
            return 124, "timeout"

    def rss_mb(self):
        p = self.pid()
        try:
            with open("/proc/%d/status" % p) as f:
                for line in f:
                    if line.startswith("VmRSS"):
                        return int(line.split()[1]) // 1024
        except (OSError, TypeError):
            pass
        return 0


class Cluster:
    def __init__(self, home, root, heap="512M", new_size="128M"):
        self.home = home
        self.root = os.path.abspath(root)
        self.heap, self.new_size = heap, new_size
        self.unit_prefix = "gauntlet-%d" % os.getpid()
        self.nodes = [Node(i, self) for i in range(1, NODE_COUNT + 1)]
        self.torn_down = False

    def node(self, i):
        return self.nodes[i - 1]

    def create(self, per_node):
        """per_node: {i: {"yaml": {...}, "autonomic": {...}}} with key 0 for all nodes."""
        for n in self.nodes:
            o = {"yaml": {}, "autonomic": None}
            for key in (0, n.i):
                src = per_node.get(key) or {}
                o["yaml"].update(src.get("yaml") or {})
                if src.get("autonomic"):
                    o["autonomic"] = src["autonomic"]
            n.write_conf(o)

    def start_all(self):
        for n in self.nodes:
            n.start()          # one at a time, each waits for UN

    def status(self):
        for n in self.nodes:
            if n.alive():
                rc, out = n.nodetool("status", timeout=60)
                if rc == 0:
                    return out
        return ""

    def up_count(self):
        return len(re.findall(r"^UN\s", self.status(), re.M))

    def teardown(self):
        if self.torn_down:
            return
        self.torn_down = True
        for n in self.nodes:
            try:
                n.stop()
            except Exception:
                pass

    def remove_data(self):
        """Delete data, commit log and the other storage; keep logs."""
        import shutil
        for n in self.nodes:
            shutil.rmtree(n.storage, ignore_errors=True)

    def rss_total_mb(self):
        return sum(n.rss_mb() for n in self.nodes)
