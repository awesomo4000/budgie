#!/bin/bash
Q=$1
P=$((64500+RANDOM%2000))
./server $P 14 io_bufs=1024 drr_quantum=$Q lazy_tick=0 > lat.log 2>&1 &
S=$!; sleep 2; rm -f a.out b.out
./gen $P 32 5 1   0 > a.out 2>&1 & A=$!
./gen $P 32 5 256 0 > b.out 2>&1 & C=$!
wait $A; wait $C; wait $S 2>/dev/null
python3 -c "
def f(p):
    v=open(p).read().strip().split(',')
    return (int(v[4]), int(v[5])/1e6, int(v[6])/1e6, int(v[13]))
a=f('a.out'); b=f('b.out')
print('  q=%-6s polite: %6d req/s  p50=%6.3fms p99=%7.3fms   greedy: %6d req/s p50=%7.2fms p99=%7.2fms' %
  ('$Q', a[0],a[1],a[2], b[0],b[1],b[2]))
"
sleep 3
