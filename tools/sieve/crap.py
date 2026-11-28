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

"""Complexity, coverage and CRAP per method from a JaCoCo XML report (the sieve, metrics 1, 2 and 6).

  CRAP(m) = comp(m)^2 * (1 - cov(m))^3 + comp(m)

comp is JaCoCo's cyclomatic complexity of the method (COMPLEXITY counter, missed + covered); cov is its line
coverage (LINE counter). Branch coverage is reported beside it.

  crap.py report.xml [--package org/apache/cassandra/autonomic] [--crap 6,12] [--complexity 6] [--top 30] [--json out]
"""

import argparse
import json
import sys
import xml.etree.ElementTree as ET


def counters(el):
    out = {}
    for c in el.findall("counter"):
        out[c.get("type")] = (int(c.get("missed")), int(c.get("covered")))
    return out


def ratio(mc):
    if not mc:
        return None
    m, c = mc
    return c / float(m + c) if m + c else None


def methods(report, prefix):
    root = ET.parse(report).getroot()
    for pkg in root.iter("package"):
        if not pkg.get("name", "").startswith(prefix):
            continue
        for cls in pkg.findall("class"):
            src = cls.get("sourcefilename")
            for m in cls.findall("method"):
                cs = counters(m)
                if "COMPLEXITY" not in cs:
                    continue
                comp = sum(cs["COMPLEXITY"])
                lcov = ratio(cs.get("LINE"))
                bcov = ratio(cs.get("BRANCH"))
                lines = sum(cs.get("LINE", (0, 0)))
                cov = lcov if lcov is not None else 0.0
                crap = comp * comp * (1.0 - cov) ** 3 + comp
                yield {"package": pkg.get("name"), "class": cls.get("name").split("/")[-1], "file": src,
                       "method": m.get("name"), "line": int(m.get("line") or 0), "complexity": comp,
                       "lines": lines, "line_cov": None if lcov is None else round(lcov, 3),
                       "branch_cov": None if bcov is None else round(bcov, 3), "crap": round(crap, 1)}


def totals(report, prefix):
    root = ET.parse(report).getroot()
    agg = {}
    for pkg in root.iter("package"):
        if pkg.get("name", "").startswith(prefix):
            for t, (m, c) in counters(pkg).items():
                a = agg.setdefault(t, [0, 0])
                a[0] += m
                a[1] += c
    return {t: round(c / float(m + c), 3) if m + c else None for t, (m, c) in agg.items()}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("report")
    ap.add_argument("--package", default="org/apache/cassandra/autonomic")
    ap.add_argument("--crap", default="6,12", help="thresholds to count against, comma-separated")
    ap.add_argument("--complexity", type=int, default=6)
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--json")
    a = ap.parse_args()
    ms = [m for m in methods(a.report, a.package) if not m["method"].startswith("lambda$")]
    lams = [m for m in methods(a.report, a.package) if m["method"].startswith("lambda$")]
    th = [float(x) for x in a.crap.split(",")]
    tot = totals(a.report, a.package)
    print("%d methods (%d lambdas listed apart) under %s" % (len(ms), len(lams), a.package))
    print("coverage of the package tree: line %s, branch %s, instruction %s"
          % (tot.get("LINE"), tot.get("BRANCH"), tot.get("INSTRUCTION")))
    print("complexity > %d: %d methods" % (a.complexity, len([m for m in ms if m["complexity"] > a.complexity])))
    for t in th:
        over = [m for m in ms if m["crap"] > t]
        print("CRAP > %g: %d methods (%.1f%%)" % (t, len(over), 100.0 * len(over) / max(1, len(ms))))
    print("worst by CRAP:")
    for m in sorted(ms, key=lambda x: -x["crap"])[:a.top]:
        print("  CRAP %7.1f  comp %3d  line cov %-5s  branch cov %-5s  %s.%s (%s:%d)"
              % (m["crap"], m["complexity"], m["line_cov"], m["branch_cov"], m["class"], m["method"], m["file"], m["line"]))
    if a.json:
        with open(a.json, "w") as f:
            json.dump({"totals": tot, "methods": ms, "lambdas": lams}, f, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
