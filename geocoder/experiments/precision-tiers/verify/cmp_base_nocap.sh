#!/bin/bash
B=/ssd/precision/build
cd $B/out-nocap && find . -type f | sort > /ssd/precision/verify/logs/list_nocap.txt
cd $B/out-base && find . -type f | sort > /ssd/precision/verify/logs/list_base.txt
diff /ssd/precision/verify/logs/list_nocap.txt /ssd/precision/verify/logs/list_base.txt > /ssd/precision/verify/logs/list_base_nocap.diff
cd $B/out-nocap
xargs -P 16 -I{} sh -c "cmp -s \"{}\" \"$B/out-base/{}\" || echo \"{}\"" < /ssd/precision/verify/logs/list_nocap.txt | sort > /ssd/precision/verify/logs/differ_base_nocap.txt
echo "files $(wc -l < /ssd/precision/verify/logs/list_nocap.txt) differing $(wc -l < /ssd/precision/verify/logs/differ_base_nocap.txt) listdiff $(wc -l < /ssd/precision/verify/logs/list_base_nocap.diff)" > /ssd/precision/verify/logs/cmp_base_nocap.txt
