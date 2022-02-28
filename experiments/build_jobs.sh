#!/bin/bash

SUPERMUC_OUTPUT_DIR=$SCRATCH/logs/message-queue
SUPERMUC_ACCOUNT=
JOB_DIR=jobs
binary=$(realpath ${0%/*}/../build-clang/bfs)
scale_weak=true
source sbatch.sh

n_rank=(64 256 1024 2048 4096)
num_local_vertices=14

for p in "${n_rank[@]}"; do
    job_name=bfs_$p
    out_file=$JOB_DIR/$job_name.sbatch
    sbatch_preamble $p $job_name 01:00:00 > $out_file
    if [[ "$scale_weak" = true ]]; then
        num_vertices=$(( $num_local_vertices + $(log2 p) ))
    else
        num_vertices=$num_local_vertices
    fi
    echo $(mpi_exec_call) $binary --iterations=3 gnm --n $num_vertices --async false bench >> $out_file
    echo $(mpi_exec_call) $binary --iterations=3 gnm --n $num_vertices --async true bench --buffering true --poll_after_pop true false --poll_after_post true false --flush_level 0 1 2 >> $out_file
    echo $(mpi_exec_call) $binary --iterations=3 gnm --n $num_vertices --async true bench --buffering true --dynamic_buffering true --poll_after_pop true false --poll_after_post true false --flush_level 0 1 2 --dampening_factor 0.5 0.75 0.9 >> $out_file
done
