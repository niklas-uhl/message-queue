#!/bin/bash

SUPERMUC_OUTPUT_DIR=$(ws_find message-queue)
SUPERMUC_ACCOUNT=
JOB_DIR=$HOME/jobs
binary=$(realpath ${0%/*}/../build-make/Release/example)
scale_weak=false
source ${0%/*}/sbatch.sh

n_rank=(64 256 1024 2048 4096 8192)
#num_local_vertices=14

declare -A configs
configs["v1"]="$binary --queue_version 1 --iterations 10"
configs["v2"]="$binary --queue_version 2 --iterations 10"

for p in "${n_rank[@]}"; do
    for config_name in ${!configs[@]}; do
        job_name=async_${p}_${config_name}
        out_file=$JOB_DIR/$job_name.sbatch
        sbatch_preamble $p $job_name 02:00:00 > $out_file
        if [[ "$scale_weak" = true ]]; then
            num_vertices=$(( $num_local_vertices + $(log2 p) ))
        else
            num_vertices=$num_local_vertices
        fi
        echo $(mpi_exec_call) ${configs[$config_name]} >> "${out_file}"
    done
done
