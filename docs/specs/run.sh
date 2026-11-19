#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work. Licensed
# under the Apache License, Version 2.0; see http://www.apache.org/licenses/LICENSE-2.0
#
# Runs TLC on layout-migration.tla or layout-reverse.tla with one configuration;
# the spec is chosen by the configuration's name (layout-reverse*.cfg runs the
# reverse model).
# usage: TLA_JAR=/path/to/tla2tools.jar sh run.sh [layout-migration.cfg] [extra TLC options]
set -e
here=$(cd "$(dirname "$0")" && pwd)
cfg=${1:-layout-migration.cfg}
[ $# -gt 0 ] && shift
case "$cfg" in
    layout-reverse*) spec=layout_reverse ;;
    *)               spec=layout_migration ;;
esac
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
# A TLA+ module name cannot contain a hyphen, and TLC needs file name = module name.
for f in layout-core layout-migration layout-reverse; do
    [ -f "$here/$f.tla" ] && cp "$here/$f.tla" "$work/$(echo $f | tr - _).tla"
done
cp "$here/$cfg" "$work/$spec.cfg"
cd "$work"
java "-Xmx${TLC_HEAP:-1500m}" -XX:+UseParallelGC -cp "${TLA_JAR:?set TLA_JAR to tla2tools.jar}" tlc2.TLC \
     -workers "${TLC_WORKERS:-2}" -config $spec.cfg "$@" $spec.tla
