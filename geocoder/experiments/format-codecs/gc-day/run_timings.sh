#!/bin/bash
# All timing runs (each under TIMING.lock via timed.sh), the determinism and round-trip checks (whole
# day cmp, also under the lock), and the encoded days for the end-to-end step (enc/2026-10-12,
# enc/2026-10-13).
set -u
C=/ssd/shrink/sortpatch/codecs
S=/ssd/shrink/sortpatch/sort/days
export TMPDIR=/ssd/shrink/tmp
step() { echo; echo "######## $* ($(date +%T))"; }
cmpd() { ./locked.sh cmp ./cmp_day.sh "$@"; }
cd $C
rm -rf w c s f enc
mkdir -p enc

step "1 warm encode 10-13"
PREWARM=$S/2026-10-13 ./timed.sh enc_warm_1013 warm $C/w/enc ./enc_day.sh $S/2026-10-13 $C/w/enc

step "2 warm decode 10-13"
PREWARM=$C/w/enc ./timed.sh dec_warm_1013 warm $C/w/dec ./dec_day.sh $C/w/enc $C/w/dec
cmpd $S/2026-10-13 $C/w/dec
rm -rf $C/w/dec

step "3 cold encode 10-13 -> enc/2026-10-13"
./timed.sh enc_cold_1013 cold $C/enc/2026-10-13 ./enc_day.sh $S/2026-10-13 $C/enc/2026-10-13
echo "determinism (warm run vs cold run outputs):"; cmpd $C/w/enc $C/enc/2026-10-13
rm -rf $C/w

step "4 cold decode 10-13"
./timed.sh dec_cold_1013 cold $C/c/dec ./dec_day.sh $C/enc/2026-10-13 $C/c/dec
cmpd $S/2026-10-13 $C/c/dec
rm -rf $C/c

step "5 small-device encode 10-13"
./timed.sh enc_small_1013 small $C/s/enc ./enc_day.sh $S/2026-10-13 $C/s/enc
echo "determinism (small-device run vs cold run outputs):"; cmpd $C/s/enc $C/enc/2026-10-13
rm -rf $C/s

step "6 small-device decode 10-13"
./timed.sh dec_small_1013 small $C/s/dec ./dec_day.sh $C/enc/2026-10-13 $C/s/dec
cmpd $S/2026-10-13 $C/s/dec
rm -rf $C/s

step "7 per-family memory (warm)"
PREWARM="$S/2026-10-13 $C/enc/2026-10-13" ./timed.sh perfam warm $C/f ./perfam.sh

step "8 encode 10-12 -> enc/2026-10-12, decode, compare"
PREWARM=$S/2026-10-12 ./timed.sh enc_warm_1012 warm $C/enc/2026-10-12 ./enc_day.sh $S/2026-10-12 $C/enc/2026-10-12
PREWARM=$C/enc/2026-10-12 ./timed.sh dec_warm_1012 warm $C/c/dec ./dec_day.sh $C/enc/2026-10-12 $C/c/dec
cmpd $S/2026-10-12 $C/c/dec
rm -rf $C/c
echo; echo "ALL DONE $(date +%T)"
