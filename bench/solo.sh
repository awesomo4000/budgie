#!/bin/bash
B=$1; D=$2; Q=$3
P=$((56000+RANDOM%6000))
./$B $P 12 conn_quota=0 bg_budget=0 bg_period_ms=100000 io_bufs=64 drr_quantum=$Q $4 > solo.log 2>&1 &
SPID=$!; sleep 2
./gen $P 64 5 $D 0 > solo.out 2>&1
wait $SPID 2>/dev/null
python3 -c "
import re
v=open('solo.out').read().strip().split(',')
t=int(v[4]) if len(v)>6 else 0
d=open('solo.log').read()
m=re.search(r'throttles=(\d+) credits=(\d+) resumes=(\d+) pauses=(\d+)',d)
e=re.search(r'enters=(\d+)',d); st=re.search(r'steps=(\d+)',d); sv=re.search(r'served=(\d+)',d)
served=int(sv.group(1)) if sv else 0; ent=int(e.group(1)) if e else 0
print('  %-14s depth=%-4s drr=%-6s tput=%-7d  reqs/enter=%4.1f  throttles=%s pauses=%s' %
  ('$B','$D','$Q',t, served/ent if ent else 0, m.group(1) if m else '-', m.group(4) if m else '-'))
"
sleep 3
