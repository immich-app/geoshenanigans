#!/usr/bin/env bash
# Usage: job_sampler.sh <log> <dir on the work volume> [seconds]
#
# The nightly's CPU, memory and disk use over the whole job, a line every
# <seconds> (default 30) appended to <log>, the raw counters for differences
# between lines: the job cgroup's CPU time (cpu.stat usage_usec) and memory
# (memory.current), the node's /proc/stat cpu line and the stat line of the
# work volume's block device (/sys/class/block/<dev>/stat; an RBD on the
# runner). The job cgroup is the nearest one up from this process's with a
# memory limit, as memwatch takes it. The first line gives the CPU count, that
# cgroup, the CPU quota (the first cpu.max up from it that isn't "max", with
# where it is set: the runner reports 88 CPUs, but its phases never pass ~64
# cores) and the device. Runs until killed.
#
# Env: JOB_SAMPLER_ROOT, a dir that stands in for / (tests).
set -u

log=$1
work_dir=$2
every=${3:-30}
root=${JOB_SAMPLER_ROOT:-}
stamp() { date -u +%H:%M:%S; }

path=$(sed -n 's/^0:://p' "$root/proc/self/cgroup")
path=${path%/}
job=$path
until [ -z "$job" ] || { [ -r "$root/sys/fs/cgroup$job/memory.max" ] && [ "$(cat "$root/sys/fs/cgroup$job/memory.max")" != max ]; }; do
  job=${job%/*}
done
cgroup=$root/sys/fs/cgroup$job
quota=max
at=$job
while :; do
  if [ -r "$root/sys/fs/cgroup$at/cpu.max" ] && [ "$(cut -d' ' -f1 "$root/sys/fs/cgroup$at/cpu.max")" != max ]; then
    quota="$(cat "$root/sys/fs/cgroup$at/cpu.max") (${at:-/})"
    break
  fi
  [ -z "$at" ] && break
  at=${at%/*}
done
device=$(df --output=source "$work_dir" 2> /dev/null | tail -1)
device=${device##*/}
echo "[jobstat $(stamp)] cpus=$(nproc) cgroup=${job:-/} cpu.max=$quota device=$device" >> "$log"

while :; do
  usage=$(sed -n 's/^usage_usec //p' "$cgroup/cpu.stat" 2> /dev/null)
  memory=$(cat "$cgroup/memory.current" 2> /dev/null)
  cpu=$(sed -n 's/^cpu  *//p' "$root/proc/stat")
  disk=$(sed "s/^ *//; s/  */ /g" "$root/sys/class/block/$device/stat" 2> /dev/null)
  echo "[jobstat $(stamp)] usage_usec=${usage:-?} memory=${memory:-?} cpu=$cpu $device=$disk" >> "$log"
  sleep "$every"
done
