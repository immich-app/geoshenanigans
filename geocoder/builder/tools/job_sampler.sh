#!/usr/bin/env bash
# Usage: job_sampler.sh <log> <dir on the work volume> [seconds] [file...]
#
# The nightly's CPU, memory and disk use over the whole job, a line every
# <seconds> (default 30) appended to <log>, the raw counters for differences
# between lines: the job cgroup's CPU time (cpu.stat usage_usec) and memory
# (memory.current; and of memory.stat its page cache and anonymous memory,
# their dirty and writeback parts, and its reclaim counters: file, anon,
# file_dirty, file_writeback, pgscan, pgsteal, workingset_refault_file),
# the node's /proc/stat cpu line, the stat line of the work volume's block
# device (/sys/class/block/<dev>/stat; an RBD on the runner) and, for each
# <file> that exists, its bytes in the page cache (page_cache.py), as
# "cached=<name>:<bytes>,...": what of the PBFs the build reads is cached,
# and whether pages that leave do so by reclaim. The job cgroup is the
# nearest one up from this process's with a memory limit, as memwatch takes
# it. The first line gives the CPU count, that cgroup, the CPU quota (the
# tightest cpu.max from this process's cgroup up, quota over period, with
# where it is set: the one cpu_budget.sh sizes the tools by; the runner
# reports 88 CPUs, but its phases never pass ~64 cores) and the device. Runs
# until killed.
#
# Env: JOB_SAMPLER_ROOT, a dir that stands in for / (tests).
set -u

log=$1
work_dir=$2
every=${3:-30}
files=("${@:4}")
root=${JOB_SAMPLER_ROOT:-}
tools=$(dirname "$(readlink -f "$0")")
stamp() { date -u +%H:%M:%S; }

path=$(sed -n 's/^0:://p' "$root/proc/self/cgroup")
path=${path%/}
job=$path
until [ -z "$job" ] || { [ -r "$root/sys/fs/cgroup$job/memory.max" ] && [ "$(cat "$root/sys/fs/cgroup$job/memory.max")" != max ]; }; do
  job=${job%/*}
done
cgroup=$root/sys/fs/cgroup$job
quota=max
at=$path
while :; do
  q="" p=""
  [ -r "$root/sys/fs/cgroup$at/cpu.max" ] && read -r q p < "$root/sys/fs/cgroup$at/cpu.max"
  if [[ "$q" =~ ^[0-9]+$ && "$p" =~ ^[1-9][0-9]*$ ]] && { [ "$quota" = max ] || [ $((q * tight_p)) -lt $((tight_q * p)) ]; }; then
    quota="$q $p (${at:-/})" tight_q=$q tight_p=$p
  fi
  [ -z "$at" ] && break
  at=${at%/*}
done
device=$(df --output=source "$work_dir" 2> /dev/null | tail -1)
device=${device##*/}
echo "[jobstat $(stamp)] cpus=$(nproc) cgroup=${job:-/} cpu.max=$quota device=$device" >> "$log"

# " <key>=<value>" for each memory.stat key above, in its order there.
memory_stat() {
  awk '$1 ~ /^(file|anon|file_dirty|file_writeback|pgscan|pgsteal|workingset_refault_file)$/ { printf " %s=%s", $1, $2 }' \
    "$cgroup/memory.stat" 2> /dev/null
}

# " cached=<name>:<bytes>,..." for the files that exist (page_cache.py), or
# nothing.
cached() {
  local out
  [ ${#files[@]} -gt 0 ] || return 0
  out=$(python3 "$tools/page_cache.py" "${files[@]}" 2> /dev/null) || out="?"
  [ -z "$out" ] || echo " cached=$out"
}

while :; do
  usage=$(sed -n 's/^usage_usec //p' "$cgroup/cpu.stat" 2> /dev/null)
  memory=$(cat "$cgroup/memory.current" 2> /dev/null)
  cpu=$(sed -n 's/^cpu  *//p' "$root/proc/stat")
  disk=$(sed "s/^ *//; s/  */ /g" "$root/sys/class/block/$device/stat" 2> /dev/null)
  echo "[jobstat $(stamp)] usage_usec=${usage:-?} memory=${memory:-?}$(memory_stat) cpu=$cpu $device=$disk$(cached)" >> "$log"
  sleep "$every"
done
