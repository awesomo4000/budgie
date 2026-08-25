#!/bin/bash
B=$1; shift
P=$((30000+RANDOM%20000))
./$B $P 12 conn_quota=0 bg_budget=0 bg_period_ms=100000 "$@" > c_$B.log 2>&1 &
SPID=$!; sleep 2
R=$(wrk -t1 -c256 -d6s "http://127.0.0.1:$P/work/0" 2>&1)
wait $SPID 2>/dev/null
T=$(echo "$R" | grep 'Requests/sec' | awk '{print $2}')
TOT=$(echo "$R" | grep 'requests in' | awk '{print $1}')
CPU=$(grep -o 'cpu=[0-9]*ms' c_$B.log | head -1 | tr -d 'cpu=ms')
echo "$T $TOT $CPU"
