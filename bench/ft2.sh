#!/bin/bash
B=$1; AC=$2; AD=$3; BC=$4; BD=$5; Q=$6
P=$((54000+RANDOM%6000))
./$B $P 14 conn_quota=0 bg_budget=0 bg_period_ms=100000 io_bufs=64 drr_quantum=$Q > ft2.log 2>&1 &
SPID=$!; sleep 2; rm -f a.out b.out
./gen $P $AC 5 $AD 0 > a.out 2>&1 & A=$!
./gen $P $BC 5 $BD 0 > b.out 2>&1 & BB=$!
wait $A; wait $BB; wait $SPID 2>/dev/null
python3 -c "
import re
def f(p):
    try:
        v=open(p).read().strip().split(',');  return int(v[4]) if len(v)>6 else 0
    except: return 0
a=f('a.out'); b=f('b.out'); t=a+b
d=open('ft2.log').read()
m=re.search(r'drr: quantum=(\d+) rounds=(\d+) throttles=(\d+) credits=(\d+) resumes=(\d+) pauses=(\d+)',d)
e=re.search(r'endings[^=]*= \{ ([^}]*)\}',d)
print('  A=%-7d B=%-7d total=%-7d Ashare=%5.1f%%  drr[rounds=%s thr=%s cred=%s res=%s pause=%s] endings={%s}' %
  (a,b,t,100.0*a/t if t else 0, m.group(2) if m else '-', m.group(3) if m else '-', m.group(4) if m else '-',
   m.group(5) if m else '-', m.group(6) if m else '-', e.group(1).strip() if e else '?'))
"
sleep 3
