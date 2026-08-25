#!/bin/bash
T=$1; BD=$2
P=$((40000+RANDOM%20000))
./server $P 14 io_bufs=1024 drr_quantum=0 tick_ms=$T bounded_drain=$BD > tk.log 2>&1 &
S=$!; sleep 2; rm -f a.out b.out
./gen $P 32 5 256 0 > b.out 2>&1 & C=$!
./gen $P 32 5 1 0 > a.out 2>&1
wait $C; wait $S 2>/dev/null
python3 -c "
v=open('a.out').read().strip().split(',')
print('  tick=%-4sms bounded_drain=%s  polite: %6d req/s  p50=%6.3fms  p99=%7.3fms  max=%7.2fms' %
  ('$T','$BD', int(v[4]), int(v[5])/1e6, int(v[6])/1e6, int(v[7])/1e6))
"
sleep 3
