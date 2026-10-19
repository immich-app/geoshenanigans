#!/bin/bash
# batch_c3c.sh : iso candidates with the quality dir's L10 / L11 (postal) / L15 (place areas) at
# 46 / 45 / 45 m iso (L9 42), so its bytes match q2.5+cap's aniso 37.5 m there (c3b's 45/42/37.5
# overshot by 1.6 MB). The final builder table maps L11 and above to one entry, so L15 = L11.
cd /ssd/tiers3
until grep -q BATCH_C3B_DONE logs/batch_c3b.log; do sleep 10; done
Q=";t9=42;t10=46;t11=45;t15=45"
run() { JOBS=12 ./cand6.sh "$1" "$2" "$2$Q"; }
run d1 "t2=82;t3=275;t4=82;t5=165;t6=138;t7=54;t8=44.5;cap=0.1;iso=1" &
run s3i "t2=82;t3=275;t4=82;t5=110;t6=138;t7=66;t8=55;cap=0.1;iso=1"
wait
run s5i "t2=82;t3=275;t4=82;t5=165;t6=165;t7=82;t8=66;cap=0.1;iso=1"
echo BATCH_C3C_DONE
