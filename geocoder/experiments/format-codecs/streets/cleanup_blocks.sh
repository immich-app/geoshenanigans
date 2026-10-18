#!/bin/bash
# remove the measured block-order experiments (b11, b9)
rm -rf /ssd/shrink/max/streets/reo/street_b11 /ssd/shrink/max/streets/reo/street_b9
rm -rf /ssd/shrink/max/streets/eval/b11 /ssd/shrink/max/streets/eval/b9
du -sh /ssd/shrink/max/streets/reo /ssd/shrink/max/streets/eval
