#!/bin/bash
B=$1; APAD=$2; BPAD=$3; Q=$4
P=$((55000+RANDOM%6000))
./$B $P 14 conn_quota=0 bg_budget=0 bg_period_ms=100000 io_bufs=64 drr_quantum=$Q > ft3.log 2>&1 &
SPID=$!; sleep 2; rm -f a.out b.out
./gen $P 32 5 32 0 x $APAD > a.out 2>&1 & A=$!
./gen $P 32 5 32 0 x $BPAD > b.out 2>&1 & BB=$!
wait $A; wait $BB; wait $SPID 2>/dev/null
python3 -c "
import re
def f(p):
    try:
        v=open(p).read().strip().split(','); return int(v[4]) if len(v)>6 else 0
    except: return 0
a=f('a.out'); b=f('b.out')
abytes=a*(33+$APAD); bbytes=b*(33+$BPAD); tb=abytes+bbytes
d=open('ft3.log').read(); m=re.search(r'throttles=(\d+)',d)
print('  padA=%-5s padB=%-5s | reqs A=%-7d B=%-7d | bytes A=%5.1f%% B=%5.1f%% | throttles=%s' %
  ('$APAD','$BPAD',a,b,100.0*abytes/tb if tb else 0,100.0*bbytes/tb if tb else 0, m.group(1) if m else '-'))
"
sleep 3
