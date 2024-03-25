#!/bin/bash

PM_DEVS=(
    # log regions
    "./dev_log0 16G 0"
    "./dev_log1 16G 1"
    "./dev_log2 16G 2"
    "./dev_log3 16G 3"
    "./dev_log4 16G 4"
    "./dev_log5 16G 5"

    # bnode buffer
    "./dev_b 16G 0"
)

echo "Destroying existing namespaces"
ndctl destroy-namespace -f all

for dev in "${PM_DEVS[@]}"; do
    dev_path=$(echo $dev | cut -d' ' -f1)
    dev_size=$(echo $dev | cut -d' ' -f2)
    dev_region=$(echo $dev | cut -d' ' -f3)
    echo "Initializing $dev_name with size $dev_size on region $dev_region"
    output=$(ndctl create-namespace -m devdax -r region$dev_region -s $dev_size -f)
    dev_numa=$(echo $output | python3 -c "import sys, json; print(json.load(sys.stdin)['numa_node']);")
    dev_chardev=$(echo $output | python3 -c "import sys, json; print(json.load(sys.stdin)['daxregion']['devices'][0]['chardev'])")
    ln -sf /dev/$dev_chardev $dev_path
    echo "Created $dev_path with numa $dev_numa and path /dev/$dev_chardev"
done
