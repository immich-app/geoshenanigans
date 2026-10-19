#!/bin/bash
until grep -q RUNQ_DONE.cap10_recommended /ssd/tiers3/logs/runq_base.log; do sleep 10; done
/ssd/tiers3/batch_sl.sh
