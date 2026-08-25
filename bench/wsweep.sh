#!/bin/bash
# usage: wsweep.sh <binary> <conns> <knob=val ...>
B=$1; N=$2; shift 2
WPATH=${WPATH:-/work/0}
P=$((30000+RANDOM%20000))
./$B $P 12 conn_quota=0 bg_budget=0 bg_period_ms=100000 "$@" > ws_$B.log 2>&1 &
SPID=$!; sleep 2
R=$(wrk -t1 -c$N -d5s --latency "http://127.0.0.1:$P$WPATH" 2>&1)
V=$(grep -m1 voluntary /proc/$SPID/status | tr -s ' \t' ' ' | cut -d' ' -f2)
wait $SPID 2>/dev/null
T=$(echo "$R" | grep 'Requests/sec' | awk '{print $2}')
P50=$(echo "$R" | grep -A2 'Latency Distribution' | grep '50%' | awk '{print $2}')
P99=$(echo "$R" | grep '99%' | head -1 | awk '{print $2}')
NON2=$(echo "$R" | grep -o "Non-2xx or 3xx responses: [0-9]*" | awk "{print \$5}"); echo "$T,$P50,$P99,$V,${NON2:-0}"
