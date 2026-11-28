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

"""A small YAML subset reader (the harness is stdlib only, and PyYAML is not installed).

Supports: comments, nested block mappings by indentation, block sequences of scalars or flow values or mappings,
flow mappings and sequences ({a: 1, b: [x, y]}), and scalars (int, float, bool, null, quoted and plain strings).
"""

import re


def _scalar(s):
    s = s.strip()
    if s == "" or s in ("~", "null"):
        return None
    if s in ("true", "True"):
        return True
    if s in ("false", "False"):
        return False
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        return s[1:-1]
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    if re.fullmatch(r"-?\d+\.\d*", s):
        return float(s)
    return s


def _split_top(s):
    parts, depth, cur, q = [], 0, "", None
    for ch in s:
        if q:
            cur += ch
            if ch == q:
                q = None
        elif ch in "\"'":
            q = ch
            cur += ch
        elif ch in "[{":
            depth += 1
            cur += ch
        elif ch in "]}":
            depth -= 1
            cur += ch
        elif ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    return parts


def _key_value(s):
    """Split 'k: v' at the first colon outside quotes/brackets; None if there is no key."""
    depth, q = 0, None
    for i, ch in enumerate(s):
        if q:
            if ch == q:
                q = None
        elif ch in "\"'":
            q = ch
        elif ch in "[{":
            depth += 1
        elif ch in "]}":
            depth -= 1
        elif ch == ":" and depth == 0 and (i + 1 == len(s) or s[i + 1] == " "):
            return s[:i].strip(), s[i + 1:].strip()
    return None


def _flow(s):
    s = s.strip()
    if s.startswith("{") and s.endswith("}"):
        d = {}
        for part in _split_top(s[1:-1]):
            kv = _key_value(part.strip())
            d[kv[0]] = _flow(kv[1]) if kv else None
        return d
    if s.startswith("[") and s.endswith("]"):
        return [_flow(p) for p in _split_top(s[1:-1])]
    return _scalar(s)


def _strip_comment(line):
    q = None
    for i, ch in enumerate(line):
        if q:
            if ch == q:
                q = None
        elif ch in "\"'":
            q = ch
        elif ch == "#" and (i == 0 or line[i - 1] in " \t"):
            return line[:i]
    return line


def load(text):
    lines = []
    for raw in text.splitlines():
        s = _strip_comment(raw).rstrip()
        if s.strip():
            lines.append((len(s) - len(s.lstrip()), s.strip()))
    pos = [0]

    def block(indent):
        if lines[pos[0]][1].startswith("- ") or lines[pos[0]][1] == "-":
            out = []
            while pos[0] < len(lines) and lines[pos[0]][0] == indent and lines[pos[0]][1].startswith("-"):
                item = lines[pos[0]][1][1:].strip()
                pos[0] += 1
                kv = _key_value(item) if not item.startswith(("{", "[")) else None
                if kv:
                    d = {kv[0]: _flow(kv[1]) if kv[1] else None}
                    while pos[0] < len(lines) and lines[pos[0]][0] > indent:
                        k2 = _key_value(lines[pos[0]][1])
                        pos[0] += 1
                        d[k2[0]] = _flow(k2[1])
                    out.append(d)
                else:
                    out.append(_flow(item))
            return out
        d = {}
        while pos[0] < len(lines) and lines[pos[0]][0] == indent:
            kv = _key_value(lines[pos[0]][1])
            if kv is None:
                raise ValueError("cannot parse: %r" % lines[pos[0]][1])
            pos[0] += 1
            if kv[1] == "" and pos[0] < len(lines) and lines[pos[0]][0] > indent:
                d[kv[0]] = block(lines[pos[0]][0])
            elif kv[1] == "" and pos[0] < len(lines) and lines[pos[0]][0] == indent and lines[pos[0]][1].startswith("-"):
                d[kv[0]] = block(indent)
            else:
                d[kv[0]] = _flow(kv[1])
        return d

    return block(lines[0][0]) if lines else {}


def load_file(path):
    with open(path) as f:
        return load(f.read())
