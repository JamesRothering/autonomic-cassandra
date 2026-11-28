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

"""Duplicate-code detector for Java (the sieve, docs/plan/code-quality-harness.md, metric 4).

Tokenises Java sources (comments, imports and package lines dropped), then finds every window of at least
--min-tokens tokens that occurs more than once. Two modes:

  exact       tokens as written (copy and paste)
  structural  identifiers and literals replaced by placeholders (the same code with renamed variables or values)

Overlapping windows are merged into blocks; a block is reported once per pair of locations. Output: a summary
(duplicated tokens as a share of all tokens, per mode) and the largest blocks with file:line ranges.

  dupes.py [--min-tokens 60] [--mode exact|structural|both] [--top 30] [--json out.json] PATH...
"""

import argparse
import json
import os
import re
import sys

TOKEN = re.compile(r"""
    (?P<ws>\s+)
  | (?P<lc>//[^\n]*)
  | (?P<bc>/\*.*?\*/)
  | (?P<str>"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')
  | (?P<num>\b\d[\d_]*(?:\.\d+)?(?:[eE][+-]?\d+)?[lLfFdD]?\b|\b0[xX][0-9a-fA-F_]+[lL]?\b)
  | (?P<id>[A-Za-z_$][\w$]*)
  | (?P<op>>>>=|<<=|>>=|>>>|->|::|\+\+|--|&&|\|\||[=!<>+\-*/%&|^]=|[{}()\[\];,.@?:~!<>=+\-*/%&|^])
""", re.S | re.X)

KEYWORDS = set("""abstract assert boolean break byte case catch char class const continue default do double else enum
extends final finally float for goto if implements import instanceof int interface long native new package private
protected public return short static strictfp super switch synchronized this throw throws transient try void volatile
while var record yield sealed permits true false null""".split())


def tokens(path):
    """[(token_exact, token_structural, line)] for one file, without package and import statements."""
    src = open(path, encoding="utf-8", errors="replace").read()
    out = []
    line = 1
    pos = 0
    skip_stmt = False
    while pos < len(src):
        m = TOKEN.match(src, pos)
        if not m:
            pos += 1
            continue
        kind = m.lastgroup
        text = m.group(kind)
        if kind not in ("ws", "lc", "bc"):
            if skip_stmt:
                skip_stmt = text != ";"                    # drop the rest of a package or import statement
            elif text in ("package", "import") and (not out or out[-1][0] in (";", "}")):
                skip_stmt = True
            else:
                if kind == "id" and text not in KEYWORDS:
                    st = "ID"
                elif kind in ("str", "num"):
                    st = "LIT"
                else:
                    st = text
                out.append((text, st, line))
        line += text.count("\n")
        pos = m.end()
    return out


def java_files(paths):
    for p in paths:
        if os.path.isfile(p):
            yield p
        for root, _, files in os.walk(p):
            for f in sorted(files):
                if f.endswith(".java"):
                    yield os.path.join(root, f)


def find(files_tokens, n, mode):
    idx = 0 if mode == "exact" else 1
    seen = {}
    dup_marks = {f: bytearray(len(t)) for f, t in files_tokens.items()}
    pairs = {}                                            # alignment -> its latest block
    found = []
    for f, toks in files_tokens.items():
        seq = [t[idx] for t in toks]
        for i in range(0, len(seq) - n + 1):
            h = hash(tuple(seq[i:i + n]))
            prev = seen.get(h)
            if prev is None:
                seen[h] = (f, i)
                continue
            pf, pi = prev
            if pf == f and abs(pi - i) < n:
                continue                                   # overlapping occurrence in the same place
            for j in range(n):
                dup_marks[f][i + j] = 1
                dup_marks[pf][pi + j] = 1
            key = (pf, f, pi - i)                         # same pair of files at the same alignment = same block
            blk = pairs.get(key)
            if blk and blk["b_end"] >= i - 1 + n - 1:     # this window overlaps or touches the block's last window
                blk["b_end"] = i + n - 1
                blk["a_end"] = pi + n - 1
            else:
                blk = {"a": pf, "a_start": pi, "a_end": pi + n - 1, "b": f, "b_start": i, "b_end": i + n - 1}
                pairs[key] = blk
                found.append(blk)
    total = sum(len(t) for t in files_tokens.values())
    duplicated = sum(sum(m) for m in dup_marks.values())
    blocks = []
    for b in found:
        ta, tb = files_tokens[b["a"]], files_tokens[b["b"]]
        blocks.append({"tokens": b["b_end"] - b["b_start"] + 1,
                       "a": "%s:%d-%d" % (b["a"], ta[b["a_start"]][2], ta[b["a_end"]][2]),
                       "b": "%s:%d-%d" % (b["b"], tb[b["b_start"]][2], tb[b["b_end"]][2])})
    blocks.sort(key=lambda x: -x["tokens"])
    per_file = {f: sum(m) for f, m in dup_marks.items() if sum(m)}
    return {"mode": mode, "min_tokens": n, "total_tokens": total, "duplicated_tokens": duplicated,
            "duplicated_share": round(duplicated / float(total or 1), 4), "blocks": len(blocks),
            "top_blocks": blocks, "files_with_duplication": len(per_file),
            "top_files": sorted(per_file.items(), key=lambda x: -x[1])[:20]}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--min-tokens", type=int, default=60)
    ap.add_argument("--mode", choices=("exact", "structural", "both"), default="both")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--json")
    a = ap.parse_args()
    files_tokens = {f: tokens(f) for f in java_files(a.paths)}
    modes = ("exact", "structural") if a.mode == "both" else (a.mode,)
    results = [find(files_tokens, a.min_tokens, m) for m in modes]
    for r in results:
        print("%s: %d files, %d tokens, %d duplicated (%.1f%%), %d blocks of >= %d tokens"
              % (r["mode"], len(files_tokens), r["total_tokens"], r["duplicated_tokens"], 100 * r["duplicated_share"],
                 r["blocks"], a.min_tokens))
        for b in r["top_blocks"][:a.top]:
            print("  %5d tokens  %s  ==  %s" % (b["tokens"], b["a"], b["b"]))
    if a.json:
        with open(a.json, "w") as f:
            json.dump(results, f, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
