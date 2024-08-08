#!/bin/bash

ifconfig ens2 down
ifconfig ens2 down

if [ $(hostname) == "node140" ]; then
  ip addr add 192.168.1.1/24 dev ens2
elif [ $(hostname) == "node141" ]; then
  ip addr add 192.168.1.2/24 dev ens2
fi

ifconfig ens2 up
ifconfig ens2 up
