#!/bin/bash

for i in $(seq -w $1 $2); do
    wget "https://www.grc.com/sn/sn-$i.txt";
done
