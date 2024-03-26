#!/bin/bash

ifconfig ib0 down
ifconfig ib1 down

if [ $(hostname) == "aep1" ]; then
  ip addr add 192.168.1.1/24 dev ib0
  ip addr add 192.168.1.2/24 dev ib1
elif [ $(hostname) == "aep8" ]; then
  ip addr add 192.168.1.3/24 dev ib0
  ip addr add 192.168.1.4/24 dev ib1
fi

ifconfig ib0 up
ifconfig ib1 up
