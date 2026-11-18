#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work. Licensed
# under the Apache License, Version 2.0; see http://www.apache.org/licenses/LICENSE-2.0
#
# Runs TLC on layout-migration.tla with one configuration.
# usage: TLA_JAR=/path/to/tla2tools.jar sh run.sh [layout-migration.cfg] [extra TLC options]
set -e
here=$(cd "$(dirname "$0")" && pwd)
cfg=${1:-layout-migration.cfg}
[ $# -gt 0 ] && shift
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
# A TLA+ module name cannot contain a hyphen, and TLC needs file name = module name.
cp "$here/layout-migration.tla" "$work/layout_migration.tla"
cp "$here/$cfg" "$work/layout_migration.cfg"
cd "$work"
java "-Xmx${TLC_HEAP:-1500m}" -XX:+UseParallelGC -cp "${TLA_JAR:?set TLA_JAR to tla2tools.jar}" tlc2.TLC \
     -workers "${TLC_WORKERS:-2}" -config layout_migration.cfg "$@" layout_migration.tla
