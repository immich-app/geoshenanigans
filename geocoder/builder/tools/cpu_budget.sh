#!/usr/bin/env bash
# Usage: cpu_budget.sh
#
# The CPUs the nightly's tools size their threads and jobs by: those this
# process may run on (nproc), or fewer under a CPU quota, the tightest from
# this process's cgroup up, as whole CPUs (quota / period, at least 1): cgroup
# v2's cpu.max ("<quota> <period>", not "max"), or on a host whose CPU
# controller is cgroup v1's, cpu.cfs_quota_us over cpu.cfs_period_us (a quota
# of -1 is none) under /sys/fs/cgroup/<its controllers>. The runner reports 88
# CPUs to a job whose quota is 64 (cpu.max 6400000 100000): threads sized by
# the 88 outnumber the CPUs the quota lets run, and are throttled.
#
# Env: CPU_BUDGET_ROOT, a dir that stands in for / (tests).
set -u

root=${CPU_BUDGET_ROOT:-}
budget=$(nproc)
# Each line of /proc/self/cgroup is "<id>:<controllers>:<path>": v2's has no
# controllers, its files under /sys/fs/cgroup.
while IFS=: read -r _ controllers at; do
  if [ -z "$controllers" ]; then dir=$root/sys/fs/cgroup
  elif [[ ",$controllers," == *,cpu,* ]]; then dir=$root/sys/fs/cgroup/$controllers
  else continue
  fi
  at=${at%/}
  while :; do
    quota="" period=""
    if [ -z "$controllers" ]; then
      [ -r "$dir$at/cpu.max" ] && read -r quota period < "$dir$at/cpu.max"
    elif [ -r "$dir$at/cpu.cfs_quota_us" ] && [ -r "$dir$at/cpu.cfs_period_us" ]; then
      read -r quota < "$dir$at/cpu.cfs_quota_us"
      read -r period < "$dir$at/cpu.cfs_period_us"
    fi
    if [[ "$quota" =~ ^[0-9]+$ && "$period" =~ ^[1-9][0-9]*$ ]]; then
      cpus=$((quota / period))
      [ "$cpus" -ge 1 ] || cpus=1
      [ "$cpus" -lt "$budget" ] && budget=$cpus
    fi
    [ -z "$at" ] && break
    at=${at%/*}
  done
done < <(cat "$root/proc/self/cgroup" 2> /dev/null)
echo "$budget"
