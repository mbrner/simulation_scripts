#!/bin/bash
#PBS -l nodes=1:ppn=1
#PBS -l pmem=3942mb
#PBS -l walltime=48:00:00
#PBS -o {job_file_folder}/logs/step_{step_number}_run_{run_number}_${PBS_JOBID}.out
#PBS -e {job_file_folder}/logs/step_{step_number}_run_{run_number}_${PBS_JOBID}.err
#PBS -q long
#PBS -S /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
FINAL_OUT={final_out}
echo $FINAL_OUT
if [ -z ${PBS_JOBID} ] && [ -z ${CLUSTER} ]
then
    echo 'Running Script w/o temporary scratch'
    {script_folder}/step_{step_number}.py {yaml_copy} {run_number} --no-scratch
    if [ "$?" != "0" ] ; then
        echo 'IceTray finished with Exit Code: $?'
    fi
else
    echo 'Running Script w/ temporary scratch'
    {script_folder}/step_{step_number}.py {yaml_copy} {run_number} --scratch
    if [ "$?" = "0" ] ; then
        cp {scratch_out} $FINAL_OUT
    else:
        echo 'IceTray finished with Exit Code: $?'
    fi
fi

