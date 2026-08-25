#!/bin/bash
# Fairness trial that REFUSES to report a number when the run was not valid.
B=$1; AC=$2; AD=$3; BC=$4; BD=$5; Q=$6
P=$((57000+RANDOM%6000))
./$B $P 14 conn_quota=0 bg_budget=0 bg_period_ms=100000 io_bufs=64 drr_quantum=$Q tick_ms=1 > ft4.log 2>&1 &
SPID=$!; sleep 2; rm -f a.out b.out
./gen $P $AC 5 $AD 0 > a.out 2>&1 & A=$!
./gen $P $BC 5 $BD 0 > b.out 2>&1 & BB=$!
wait $A; wait $BB; wait $SPID 2>/dev/null
python3 -c "
import re
def f(p):
    try:
        v=open(p).read().strip().split(',')
        return (int(v[4]), int(v[13])) if len(v)>13 else (0,0)
    except: return (0,0)
a,ac=f('a.out'); b,bc=f('b.out')
want_a, want_b = $AC, $BC
valid = (ac==want_a and bc==want_b)
t=a+b
d=open('ft4.log').read()
m=re.search(r'throttles=(\d+)',d); e=re.search(r'accepted=(\d+)',d)
y=re.search(r'multishot_ended=(\d+)',d); z=re.search(r'enobufs=(\d+)',d)
tag='' if valid else '   *** INVALID: conns %d/%d and %d/%d ***' % (ac,want_a,bc,want_b)
print('  A=%-7d B=%-7d Ashare=%5.1f%%  conns=%d+%d  accepted=%s thr=%s mshot_end=%s enobufs=%s%s' %
  (a,b,100.0*a/t if t else 0, ac,bc, e.group(1) if e else '-', m.group(1) if m else '-', (y.group(1) if y else '-'), (z.group(1) if z else '-'), tag))
"
sleep 3
