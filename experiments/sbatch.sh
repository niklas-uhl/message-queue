#!/bin/bash

tasks_per_node=48

if [[ -z ${SUPERMUC_OUTPUT_DIR+x}
          || -z ${SUPERMUC_ACCOUNT+x} ]]; then
    echo "Missing variables" 1>&2
    exit 1;
fi

function log2 {
    local x=0
    for (( y=$1-1 ; $y > 0; y >>= 1 )) ; do
        let x=$x+1
    done
    echo $x
}

function nodes_and_queue() {
    local tasks=$1;
    nodes=$(( ($tasks + $tasks_per_node - 1) / $tasks_per_node ))
    queue=cpuonly
    # if [[ $nodes -le 16 ]]; then
    #    queue=micro
    # elif [[ $nodes -le 768 ]]; then
    #    queue=general
    # else
    #    queue=large
    # fi
}

function sbatch_preamble() {
    local p=$1
    local job_name=$2
    local time=$3
    nodes_and_queue $p
    local output_log=$SUPERMUC_OUTPUT_DIR/${job_name}_log
    local error_log=$SUPERMUC_OUTPUT_DIR/${job_name}_error
    echo \#!/bin/bash
    echo \#SBATCH --nodes=${nodes}
    echo \#SBATCH --ntasks=${p}
    echo \#SBATCH -o ${output_log}
    echo \#SBATCH -e ${error_log}
    echo \#SBATCH -J ${job_name}
    echo \#SBATCH --partition=${queue}
    echo \#SBATCH --time=${time}
    #echo \#SBATCH --get-user-env
    #echo \#SBATCH --account=${SUPERMUC_ACCOUNT}
    echo
    echo module load slurm_setup
    echo
}

function mpi_exec_call() {
    echo mpirun --bind-to core --map-by core -report-bindings
}

