#!/bin/bash
# post2.sh : the schemes on the newer planet (evalD1.sh), then reader equivalence + work counts for d2 / s5i.
cd /ssd/tiers3
until grep -q POST1_DONE logs/post1.log; do sleep 15; done
Q=";t9=42;t10=46;t11=47;t15=47"
D2="t2=82;t3=275;t4=82;t5=165;t6=138;t7=54;t8=44.5;cap=0.1;iso=1"
S5="t2=82;t3=275;t4=82;t5=165;t6=165;t7=82;t8=66;cap=0.1;iso=1"
./evalD1.sh "q25|scale=2.5|scale=2.5" "cap10|scale=2.5;cap=0.1|scale=2.5;cap=0.1" "f50|uni=75;t8=50;cap=0.1" \
  "f75|uni=75;cap=0.1" "c5|t2=75;t3=250;t4=75;t5=150;t6=125;t7=50;t8=37.5;cap=0.1" "d2|$D2|$D2$Q" "s5i|$S5|$S5$Q"
./readereq.sh d2 s5i
echo POST2_DONE
