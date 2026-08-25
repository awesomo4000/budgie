#!/bin/bash
B=$1; N=$2; D=$3
P=$((45000+RANDOM%15000))
./$B $P 13 conn_quota=0 bg_budget=0 bg_period_ms=100000 > r_$B.log 2>&1 &
SPID=$!; sleep 2
./gen $P $N 5 $D 0 > r_$B.out 2>&1
SV=$(grep -m1 voluntary /proc/$SPID/status 2>/dev/null | tr -s ' \t' ' ' | cut -d' ' -f2)
wait $SPID 2>/dev/null
python3 - "$B" "$D" "$(cat r_$B.out)" "$SV" <<'PY'
import sys
b,d,line,sv=sys.argv[1],sys.argv[2],sys.argv[3].strip(),sys.argv[4]
f=line.split(',')
if len(f)<12: print(f"  {b:<14} depth={d} FAILED: {line}"); sys.exit()
print(f"  {b:<14} depth={d:>3} tput={int(f[4]):>7} p50={int(f[5])/1e6:>6.2f}ms p99={int(f[6])/1e6:>7.2f}ms  srv_ctxsw={sv:>7} cli_ctxsw={f[8]:>7} cli_enters={f[10]:>7} malformed={f[12]}")
PY
