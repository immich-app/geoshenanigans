#!/bin/bash
# Usage: with_lock.sh DESCRIPTION CMD... : runs CMD while holding TIMING.lock
# (IO-heavy untimed work that must not overlap a timing run).
set -u
source /ssd/shrink/sortpatch/patch/common.sh
desc=$1; shift
lock_acquire "$desc"
"$@"
rc=$?
lock_release
exit $rc
