#!/bin/bash
# stage1.sh : planet-261018b sets that need no new planet build, and their answers (fix/admin-nesting
# reader bc2e758 = the tiers3 D1 evaluation reader) on verify5's fresh pools.
#   qcapT = tiers3's real D1 build (q2.5 + cap) as is      unc = truth (tiervar3-n none / quality/uncapped)
#   f50, f75, sr1, r1T = tiervar3-n schemes as tiers3 used them (r1T to compare with the committed builder)
set -u
V=/ssd/verify5
cd $V
O=/ssd/tiers3/build/out-D1
Q9=";t9=42;t10=46;t11=47;t15=47"
R1="t2=82;t3=400;t4=90;t5=175;t6=175;t7=54;t8=45;cap=0.1;iso=1;cityovr=50"
SR="t2=90;t3=400;t4=100;t5=200;t6=200;t7=82;t8=66;cap=0.1;iso=1;cityovr=66"
./mkset.sh qcapT $O
./mkset.sh unc $O none
./mkset.sh f50 $O "uni=75;t8=50;cap=0.1" "uni=75;t8=50;cap=0.1;t9=37.5;t10=37.5;t11=37.5" &
./mkset.sh f75 $O "uni=75;cap=0.1" "uni=75;cap=0.1;t9=37.5;t10=37.5;t11=37.5" &
./mkset.sh sr1 $O "$SR" "$SR$Q9" &
./mkset.sh r1T $O "$R1" "$R1$Q9" &
./mkset.sh qcapTV $O "scale=2.5;cap=0.1" "scale=2.5;cap=0.1" &
wait
for s in unc qcapT f50 f75 sr1 r1T; do
  for st in smallest recommended; do
    JOBS=24 ./v5q.sh nest D1_${s}_$st sets/$s/$st pts/D1/fresh/*.txt pts/D1/big/*.txt
  done
done
echo STAGE1_DONE
