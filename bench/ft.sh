#!/bin/bash
B=$1; LBL=$2; AC=$3; AD=$4; BC=$5; BD=$6; Q=$7
P=$((53000+RANDOM%5000))
./$B $P 14 conn_quota=0 bg_budget=0 bg_period_ms=100000 io_bufs=64 drr_quantum=$Q > ft_$B.log 2>&1 &
SPID=$!
sleep 2
rm -f ft_a.out ft_b.out
if [ "$AC" != "0" ]; then ./gen $P $AC 5 $AD 0 > ft_a.out 2>&1 & A=$!; fi
if [ "$BC" != "0" ]; then ./gen $P $BC 5 $BD 0 > ft_b.out 2>&1 & BB=$!; fi
[ "$AC" != "0" ] && wait $A
[ "$BC" != "0" ] && wait $BB
wait $SPID 2>/dev/null
python3 -c "
def f(p):
    try:
        v=open(p).read().strip().split(',')
        return (int(v[4]), int(v[5])/1e6) if len(v)>6 else (0,0)
    except: return (0,0)
a=f('ft_a.out'); b=f('ft_b.out')
tot=a[0]+b[0]; sh=(100.0*a[0]/tot) if tot else 0
print('  %-26s A(d=%-3s)=%-7d p50=%6.2fms | B(d=%-3s)=%-7d | A share=%5.1f%%' % ('$LBL','$AD',a[0],a[1],'$BD',b[0],sh))
"
sleep 3
