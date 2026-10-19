#!/bin/bash
# bcg_cap.sh : caps each 74a4a88 default-argument run inside 32 MiB (sb.sh "bcg") at 180 s, a thrash
# without OOM; logs the kill. Ends when sb.sh is done.
until grep -q SB_DONE /ssd/verify6/logs/sb.log 2>/dev/null; do
  for p in $(pgrep -f "place-search.74a4a88 build .*runs/bcg-"); do
    et=$(ps -o etimes= -p $p | tr -d " ")
    if [ -n "$et" ] && [ "$et" -gt 180 ]; then
      echo "$(date -Is) KILL bcg pid $p after ${et}s: $(ps -o args= -p $p | grep -o "runs/bcg-[a-z]*")" >> /ssd/verify6/logs/bcg_cap.log
      kill $p
    fi
  done
  sleep 5
done
