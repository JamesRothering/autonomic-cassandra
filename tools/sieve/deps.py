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

"""Package dependency structure with Robert C. Martin's package metrics (the sieve, metric 5).

Reads class-level dependencies from `jdeps -verbose:class` on compiled classes, keeps the packages under --root, and
reports per package:

  Ca  afferent coupling: classes outside the package (under --root) that depend on classes inside it
  Ce  efferent coupling: classes inside the package that depend on classes outside it (under --root)
  I   instability Ce / (Ca + Ce)
  A   abstractness: abstract classes and interfaces / all top-level types, from the sources under --src
  D   distance from the main sequence |A + I - 1|

and every cycle between packages (strongly connected components of the package graph with more than one package).

  deps.py --classes build/classes/main --src src/java [--root org.apache.cassandra.autonomic] [--json out]
"""

import argparse
import collections
import json
import os
import re
import subprocess
import sys

LINE = re.compile(r"^\s+(\S+)\s+->\s+(\S+)")


def class_edges(classes, root):
    out = subprocess.run(["jdeps", "-verbose:class", "-filter:none", "-include", re.escape(root) + r"\..*", classes],
                         capture_output=True, text=True, check=True).stdout
    edges = set()
    for line in out.splitlines():
        m = LINE.match(line)
        if m:
            a, b = m.group(1), m.group(2)
            if a.startswith(root + ".") and b.startswith(root + "."):
                edges.add((a.split("$")[0], b.split("$")[0]))
    return edges


def pkg(cls):
    return cls.rsplit(".", 1)[0]


def abstractness(src, root):
    base = os.path.join(src, *root.split("."))
    counts = collections.defaultdict(lambda: [0, 0])
    for d, _, files in os.walk(base):
        for f in files:
            if not f.endswith(".java"):
                continue
            p = os.path.relpath(d, src).replace(os.sep, ".")
            text = open(os.path.join(d, f), encoding="utf-8", errors="replace").read()
            name = f[:-5]
            m = re.search(r"^(?:public\s+|final\s+|abstract\s+|sealed\s+|non-sealed\s+)*(class|interface|enum|record|@interface)\s+" + re.escape(name) + r"\b", text, re.M)
            if not m:
                continue
            counts[p][1] += 1
            head = text[max(0, m.start() - 0):m.end()]
            if m.group(1) in ("interface", "@interface") or re.search(r"\babstract\b", head):
                counts[p][0] += 1
    return counts


def sccs(nodes, adj):
    index, low, onstack, stack, out, i = {}, {}, set(), [], [], [0]

    def visit(v):
        index[v] = low[v] = i[0]
        i[0] += 1
        stack.append(v)
        onstack.add(v)
        for w in adj.get(v, ()):
            if w not in index:
                visit(w)
                low[v] = min(low[v], low[w])
            elif w in onstack:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            comp = []
            while True:
                w = stack.pop()
                onstack.discard(w)
                comp.append(w)
                if w == v:
                    break
            out.append(sorted(comp))
    sys.setrecursionlimit(10000)
    for v in nodes:
        if v not in index:
            visit(v)
    return [c for c in out if len(c) > 1]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--classes", required=True)
    ap.add_argument("--src", required=True)
    ap.add_argument("--root", default="org.apache.cassandra.autonomic")
    ap.add_argument("--json")
    a = ap.parse_args()
    edges = class_edges(a.classes, a.root)
    ca, ce = collections.defaultdict(set), collections.defaultdict(set)
    padj = collections.defaultdict(set)
    pkgs = set()
    for x, y in edges:
        px, py = pkg(x), pkg(y)
        pkgs.update((px, py))
        if px != py:
            ce[px].add(x)
            ca[py].add(x)
            padj[px].add(py)
    ab = abstractness(a.src, a.root)
    pkgs.update(ab)
    rows = []
    for p in sorted(pkgs):
        n_ca, n_ce = len(ca[p]), len(ce[p])
        inst = n_ce / float(n_ca + n_ce) if n_ca + n_ce else 0.0
        abst, tot = ab.get(p, (0, 0))
        av = abst / float(tot) if tot else 0.0
        rows.append({"package": p[len(a.root):].lstrip(".") or "(root)", "types": tot, "Ca": n_ca, "Ce": n_ce,
                     "I": round(inst, 2), "A": round(av, 2), "D": round(abs(av + inst - 1), 2)})
    cycles = sccs(sorted(pkgs), padj)
    print("%-28s %5s %4s %4s %5s %5s %5s" % ("package", "types", "Ca", "Ce", "I", "A", "D"))
    for r in rows:
        print("%-28s %5d %4d %4d %5.2f %5.2f %5.2f" % (r["package"], r["types"], r["Ca"], r["Ce"], r["I"], r["A"], r["D"]))
    print("package cycles: %d" % len(cycles))
    for c in cycles:
        print("  " + " <-> ".join(x[len(a.root):].lstrip(".") or "(root)" for x in c))
    if a.json:
        with open(a.json, "w") as f:
            json.dump({"packages": rows, "cycles": cycles}, f, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
