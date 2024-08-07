#/bin/bash

db="fencekv"
output="exp_output.txt"
workload="../ycsb/workloads/my/load_20G.spec"
moreworkloads="../ycsb/workloads/my/run_update_5G.spec:../ycsb/workloads/my/run_get_10G.spec:../ycsb/workloads/my/run_range_256K.spec:../ycsb/workloads/my/run_update_50G.spec:../ycsb/workloads/my/run_get_10G.spec:../ycsb/workloads/my/run_range_256K.spec:../ycsb/workloads/my/run_range_1M.spec:../ycsb/workloads/my/run_range_5M.spec"

cmd="rm -rf ../data/* ../lsm_dir/* ./$output"
echo $cmd
eval $cmd
cmd="nohup ./$db -db $db -threads 4 -load true -P $workload -morerun $moreworkloads > $output 2>&1 &"
echo $cmd
eval $cmd
echo "=======================$db end======================="
