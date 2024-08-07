#/bin/bash

db="fencekv"
output="exp-sk_output.txt"
workload="../ycsb/workloads/my-sk/load_20G.spec"
moreworkloads="../ycsb/workloads/my-sk/run_update_5G.spec:../ycsb/workloads/my-sk/run_update_50G.spec:../ycsb/workloads/my-sk/run_range_256K.spec:../ycsb/workloads/my-sk/run_range_1M.spec:../ycsb/workloads/my-sk/run_range_5M.spec"

cmd="rm -rf ../data/* ../lsm_dir/* ./$output"
echo $cmd
eval $cmd
cmd="nohup ./$db -db $db -threads 4 -load true -P $workload -morerun $moreworkloads > $output 2>&1 &"
echo $cmd
eval $cmd
echo "=======================$db end======================="
