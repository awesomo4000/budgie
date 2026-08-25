#!/bin/bash
# $1 binary  $2 io_bufs  $3 drr_quantum
B=$1; BUF=$2; Q=$3
P=$((64000+RANDOM%2000))
./$B $P 14 conn_quota=0 bg_budget=0 bg_period_ms=100000 io_bufs=$BUF drr_quantum=$Q drr_policy=${4:-1} > ft5.log 2>&1 &
SPID=$!; sleep 2; rm -f a.out b.out
./gen $P 32 5 1   0 > a.out 2>&1 & A=$!
./gen $P 32 5 256 0 > b.out 2>&1 & BB=$!
wait $A; wait $BB; wait $SPID 2>/dev/null
python3 -c "
import re
def f(p):
    try:
        v=open(p).read().strip().split(',')
        return (int(v[4]), int(v[13])) if len(v)>13 else (0,0)
    except: return (0,0)
a,ac=f('a.out'); b,bc=f('b.out'); t=a+b
d=open('ft5.log').read()
m=re.search(r'throttles=(\d+)',d); r=re.search(r'rounds=(\d+)',d); e=re.search(r'endings[^=]*= \{ ([^}]*)\}',d)
ok = (ac==32 and bc==32)
print('  %-14s drr=%-6s polite=%-7d greedy=%-7d polite_share=%5.1f%%  thr=%s rounds=%s conns=%d+%d%s' %
  ('$B','$Q',a,b,100.0*a/t if t else 0, m.group(1) if m else '-', r.group(1) if r else '-', ac,bc,
   '' if ok else '  *** INVALID ***'))
"
sleep 3
