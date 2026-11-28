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

"""Summarises the GC logs and bench output written by gc-suite.sh.

Per run it extracts:
  pauses      stop-the-world GC pauses: G1 "Pause ..." lines, ZGC "Pause Mark Start/Mark End/Relocate Start"
  total, max  sum and maximum pause time (ms)
  stalls      ZGC allocation stalls (count, total ms): threads blocked waiting for memory
  promoted    G1: MiB of old-gen regions added by young pauses (old regions after - before, times the region size)
              ZGC: MiB reported as promoted by minor (young) collections
  old/full    G1: mixed and full collections; ZGC: major collections
  ops/s       from the bench's result line

Then, per configuration, the median and the min-max spread over repeats.
"""

import collections
import glob
import os
import re
import statistics
import sys

UNITS = {'B': 1 / (1 << 20), 'K': 1 / 1024, 'M': 1, 'G': 1024}


def to_mib(value, unit):
    return float(value) * UNITS[unit]


def parse_gc(path):
    s = dict(pauses=0, total=0.0, max=0.0, stalls=0, stall_ms=0.0, promoted=0.0, old=0, full=0)
    region_mib = None
    for line in open(path, errors='replace'):
        m = re.search(r'Heap Region Size: (\d+)([BKMG])', line)
        if m:
            region_mib = to_mib(*m.groups())
        # G1 pauses: "Pause Young (Normal) (G1 Evacuation Pause) 600M->200M(2048M) 12.345ms", Remark, Cleanup, Full
        m = re.search(r'\[gc\s*\] GC\(\d+\) Pause (Young|Remark|Cleanup|Full)([^\d]*).* (\d+\.\d+)ms$', line)
        if m:
            ms = float(m.group(3))
            s['pauses'] += 1
            s['total'] += ms
            s['max'] = max(s['max'], ms)
            if m.group(1) == 'Full':
                s['full'] += 1
            if '(Mixed)' in m.group(2):
                s['old'] += 1
            continue
        # G1 old-gen growth across a young pause
        m = re.search(r'GC\(\d+\) Old regions: (\d+)->(\d+)', line)
        if m and region_mib:
            before, after = int(m.group(1)), int(m.group(2))
            s['promoted'] += max(0, after - before) * region_mib
            continue
        # Generational ZGC pauses: "y: Pause Mark Start 0.012ms" (also "Pause Mark Start (Major)")
        m = re.search(r'\[gc,phases\s*\] GC\(\d+\) [yYoO]: Pause [A-Za-z ()]+? (\d+\.\d+)ms', line)
        if m:
            ms = float(m.group(1))
            s['pauses'] += 1
            s['total'] += ms
            s['max'] = max(s['max'], ms)
            continue
        m = re.search(r'Allocation Stall \(.*\) (\d+\.\d+)ms', line)
        if m:
            s['stalls'] += 1
            s['stall_ms'] += float(m.group(1))
            continue
        m = re.search(r'\[gc\s*\] GC\(\d+\) Major Collection', line)
        if m:
            s['old'] += 1
            continue
        # ZGC young collection summary: "y: Promoted: 123M ..." (format varies by JDK; best effort)
        m = re.search(r'[yY]: Promoted:\s+(\d+)([BKMG])', line)
        if m:
            s['promoted'] += to_mib(*m.groups())
    return s


def parse_result(path):
    for line in open(path, errors='replace'):
        if 'result: ' in line:
            return dict(kv.split('=', 1) for kv in line.split('result: ', 1)[1].split())
    return None


def main(out):
    runs = collections.defaultdict(list)
    for gc_log in sorted(glob.glob(os.path.join(out, '*.gc.log'))):
        name = os.path.basename(gc_log)[:-len('.gc.log')]
        config = re.sub(r'-r\d+$', '', name)
        result = parse_result(os.path.join(out, name + '.out'))
        if result is None:
            print(f'{name}: no result line (run failed?)')
            continue
        s = parse_gc(gc_log)
        s['ops'] = float(result['ops_per_sec'])
        s['flushed_mib'] = int(result['memtable_bytes_at_flush']) / (1 << 20)
        s['hwm_mib'] = int(result['vm_hwm_kb']) / 1024
        runs[config].append(s)
        print(f"{name:45s} ops/s={s['ops']:9.0f} pauses={s['pauses']:4d} total={s['total']:8.1f}ms "
              f"max={s['max']:7.2f}ms stalls={s['stalls']:3d}/{s['stall_ms']:.0f}ms promoted={s['promoted']:7.0f}MiB "
              f"old/major={s['old']:3d} full={s['full']} memtable_at_flush={s['flushed_mib']:.0f}MiB rss_hwm={s['hwm_mib']:.0f}MiB")
    print()
    print('median [min-max] over repeats')
    keys = ['ops', 'pauses', 'total', 'max', 'stalls', 'stall_ms', 'promoted', 'old', 'full', 'flushed_mib', 'hwm_mib']
    print(f"{'config':40s} " + ' '.join(f'{k:>22s}' for k in keys))
    for config, ss in sorted(runs.items()):
        cells = []
        for k in keys:
            vals = [x[k] for x in ss]
            cells.append(f'{statistics.median(vals):9.1f} [{min(vals):.0f}-{max(vals):.0f}]'.rjust(22))
        print(f'{config:40s} ' + ' '.join(cells) + f'  (n={len(ss)})')


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'build/rust-gc')
