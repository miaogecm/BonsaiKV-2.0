#!/bin/bash

PM_DEVS=(
    # PMs
    "pm0 16G 0"
    "pm1 16G 1"
    "pm2 16G 2"
    "pm3 16G 3"
    "pm4 16G 4"
    "pm5 16G 5"
)

echo "Destroying existing namespaces"
ndctl destroy-namespace -f all

for dev in "${PM_DEVS[@]}"; do
    dev_name=$(echo $dev | cut -d' ' -f1)
    dev_size=$(echo $dev | cut -d' ' -f2)
    dev_region=$(echo $dev | cut -d' ' -f3)
    echo "Initializing $dev_name with size $dev_size on region $dev_region"
    output=$(ndctl create-namespace -m devdax -r region$dev_region -s $dev_size -f -n $dev_name)
    dev_numa=$(echo $output | python3 -c "import sys, json; print(json.load(sys.stdin)['numa_node']);")
    dev_chardev=$(echo $output | python3 -c "import sys, json; print(json.load(sys.stdin)['daxregion']['devices'][0]['chardev'])")
    echo "Created $dev_name with numa $dev_numa"
done
