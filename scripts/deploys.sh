#!/bin/sh
# Pushes N deploys of a repository in a row, each with a one-line change so
# every push is a new commit. Used with scripts/loadloop to measure a deploy's
# effect on live traffic:
#
#   go run ./scripts/loadloop -host slowstart.localhost -rate 200 &
#   sh scripts/deploys.sh /path/to/slowstart-checkout 10
#   kill -INT %1; wait
#
# The repository must already have the ashore remote and a first push behind it.
set -eu
repo=${1:?usage: deploys.sh <repo> [count]}
count=${2:-10}
cd "$repo"
i=1
while [ "$i" -le "$count" ]; do
  echo "deploy $i/$count"
  date +%s.%N > deploy.txt
  git add deploy.txt
  git commit -q -m "deploy $i"
  git push ashore main 2>&1 | grep -E 'remote: ----->|rejected' || true
  i=$((i + 1))
done
