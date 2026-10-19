#!/bin/bash
until grep -q PIPELINE_ALL_DONE /ssd/tiers3/logs/pipeline_all.log; do sleep 15; done
cd /ssd/tiers3 && ./post1.sh
