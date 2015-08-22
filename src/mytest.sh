#!/bin/bash

rm 5555.conf
rm 6666.conf

./discnt-server --port 5555 --cluster-config-file 5555.conf 1>&2 &
a=$!
sleep 1
./discnt-server --port 6666 --cluster-config-file 6666.conf 1>&2 &
b=$!

sleep 1

echo "CLUSTER MEET 127.0.0.1 5555" | ./discnt-cli -p 6666
sleep 1

echo "CLUSTER NODES" | ./discnt-cli -p 5555

echo "INCR xxxx 1.2" | ./discnt-cli -p 5555
echo "INCR xxxx 1.2" | ./discnt-cli -p 5555
echo "GET xxxx" | ./discnt-cli -p 5555
sleep 2
echo "GET xxxx" | ./discnt-cli -p 6666

for i in `seq 1 30`; do
    sleep 1
    echo -n "$i "
    echo "GET xxxx" | ./discnt-cli -p 6666
done

kill -9 $a
kill -9 $b

