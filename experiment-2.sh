#/bin/bash

db="fencekv"
output="exp2_output.txt"
workload="../ycsb/workloads/my/load_2G.spec"
moreworkloads="../ycsb/workloads/my/run_update_10G.spec"

cmd="rm -rf ../data/* ../lsm_dir/* ./$output"
echo $cmd
eval $cmd
cmd="nohup ./$db -db $db -threads 4 -load true -P $workload -morerun $moreworkloads > $output 2>&1 &"
echo $cmd
eval $cmd
echo "=======================$db end======================="
