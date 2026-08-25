#!/bin/bash
Q=$1
P=$((51000+RANDOM%4000))
./server $P 14 io_bufs=1024 drr_quantum=$Q lazy_tick=0 > ct.log 2>&1 &
S=$!; sleep 2; rm -f a.out b.out
./gen $P 32 5 1   0 > a.out 2>&1 & A=$!
./gen $P 32 5 256 0 > b.out 2>&1 & C=$!
wait $A; wait $C; wait $S 2>/dev/null
python3 -c "
import re
def f(p):
    try:
        v=open(p).read().strip().split(','); return (int(v[4]),int(v[13])) if len(v)>13 else (0,0)
    except: return (0,0)
a,ac=f('a.out'); b,bc=f('b.out'); t=a+b
d=open('ct.log').read(); m=re.search(r'rounds=(\d+) throttles=(\d+) resumes=(\d+)',d)
ok = ac==32 and bc==32
print('  q=%-7s polite=%-7d greedy=%-7d share=%5.1f%%  rounds=%s thr=%s res=%s%s' %
  ('$Q',a,b,100.0*a/t if t else 0, m.group(1) if m else '-', m.group(2) if m else '-', m.group(3) if m else '-', '' if ok else '  INVALID'))
"
sleep 3
