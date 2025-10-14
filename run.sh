#!/bin/bash
cd "$(dirname ${BASH_SOURCE[0]})"
cmd="$1"
set -x
set -e

clj -M:run $cmd sqlite
clj -M:run $cmd postgres
clj -M:xtdb1:run $cmd xtdb1
clj -M:xtdb2:run $cmd xtdb2
