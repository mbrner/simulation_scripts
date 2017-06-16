#!/bin/bash
#PBS -l nodes=1:ppn=1
#PBS -l pmem=3942mb
#PBS -l walltime=48:00:00
#PBS -o {job_file_folder}/logs/step_{step_number}_run_{run_number}_${PBS_JOBID}.out
#PBS -e {job_file_folder}/logs/step_{step_number}_run_{run_number}_${PBS_JOBID}.err
#PBS -q long
FINAL_OUT={final_out}
echo $FINAL_OUT
if [ -z ${PBS_JOBID} ] || [ -z ${CLUSTER} ]
then
    {script_folder}/step_{step_number}.py {yaml_copy} -r {run_number} --scratch
    cp {scratch_out} $FINAL_OUT
else
    {script_folder}/step_{step_number}.py {yaml_copy} -r {run_number} --no-scratch
fi

