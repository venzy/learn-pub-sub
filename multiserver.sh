#!/usr/bin/env bash
set -euo pipefail

if [ -z "${1:-}" ]; then
  echo "Usage: $0 <number-of-instances>"
  exit 1
fi
num_instances="$1"

# Build once
echo "Building..."
go build -o peril_server ./cmd/server

pids=()

cleanup() {
  echo "Terminating ${#pids[@]} instances..."
  if ((${#pids[@]})); then
    kill -TERM "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
}
trap cleanup INT TERM EXIT

mkdir -p logs
for ((i=1; i<=num_instances; i++)); do
  ./peril_server >"logs/peril_${i}.log" 2>&1 &
  pids+=($!)
done

wait