#!/bin/bash
B=$1; N=$2; shift 2
P=$((30000+RANDOM%20000))
./$B $P 16 conn_quota=0 bg_budget=0 bg_period_ms=100000 idle_deadline_ms=60000 "$@" > m_$B.log 2>&1 &
SPID=$!
sleep 2
IDLE=$(grep VmRSS /proc/$SPID/status | tr -s ' ' | cut -d' ' -f2)
if [ "$N" -gt 0 ]; then ./hold $P $N 9 > /dev/null 2>&1 & sleep 6; fi
R=$(grep VmRSS /proc/$SPID/status | tr -s ' ' | cut -d' ' -f2)
kill $SPID 2>/dev/null; wait $SPID 2>/dev/null
echo "$IDLE,$R"
