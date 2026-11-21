#!/usr/bin/env bash
# Usage: cpu_budget.sh
#
# The CPUs the nightly's tools size their threads and jobs by: those this
# process may run on (nproc), or fewer under a CPU quota, the tightest
# cgroup v2 cpu.max ("<quota> <period>", not "max") from this process's
# cgroup up, as whole CPUs (quota / period, at least 1). The runner reports
# 88 CPUs to a job whose quota is 64 (cpu.max 6400000 100000): threads sized
# by the 88 outnumber the CPUs the quota lets run, and are throttled.
#
# Env: CPU_BUDGET_ROOT, a dir that stands in for / (tests).
set -u

root=${CPU_BUDGET_ROOT:-}
budget=$(nproc)
at=$(sed -n 's/^0:://p' "$root/proc/self/cgroup" 2> /dev/null)
at=${at%/}
while :; do
  file=$root/sys/fs/cgroup$at/cpu.max
  quota="" period=""
  [ -r "$file" ] && read -r quota period < "$file"
  if [[ "$quota" =~ ^[0-9]+$ && "$period" =~ ^[1-9][0-9]*$ ]]; then
    cpus=$((quota / period))
    [ "$cpus" -ge 1 ] || cpus=1
    [ "$cpus" -lt "$budget" ] && budget=$cpus
  fi
  [ -z "$at" ] && break
  at=${at%/*}
done
echo "$budget"
