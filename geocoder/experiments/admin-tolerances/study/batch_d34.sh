#!/bin/bash
# batch_d34.sh : d2 variants that keep China's city answers (on planet-261018b the Immich city of CN
# comes from level 5, prefecture-level cities, absent from planet-261006): L5 back to 100-110 m, paid by
# country/state at 90-100 m. Both planets, both sets.
cd /ssd/tiers3
Q=";t9=42;t10=46;t11=47;t15=47"
D3="t2=100;t3=500;t4=100;t5=110;t6=138;t7=56;t8=45;cap=0.1;iso=1"
D4="t2=90;t3=400;t4=90;t5=100;t6=150;t7=54;t8=44.5;cap=0.1;iso=1"
./evalD1.sh "d3|$D3|$D3$Q" "d4|$D4|$D4$Q" > logs/evalD1_d34.log 2>&1 &
JOBS=10 ./cand6.sh d3 "$D3" "$D3$Q"
JOBS=10 ./cand6.sh d4 "$D4" "$D4$Q"
wait
echo BATCH_D34_DONE
