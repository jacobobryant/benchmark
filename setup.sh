#!/bin/bash
set -x
set -e
for alias in xtdb2 xtdb1 sqlite postgres; do
  time clj -M:$alias setup
done
