#!/bin/bash
#PBS -l nodes=1:ppn=1
#PBS -l pmem=3942mb
#PBS -l walltime=48:00:00
#PBS -o {processing_folder}/logs/step_{step_number}_run_{run_number}_${PBS_JOBID}.out
#PBS -e {processing_folder}/logs/step_{step_number}_run_{run_number}_${PBS_JOBID}.err
#PBS -q long
#PBS -S /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
FINAL_OUT={final_out}
KEEP_CRASHED_FILES={keep_crashed_files:d}
echo $FINAL_OUT
if [ -z ${PBS_JOBID} ] && [ -z ${CLUSTER} ]
then
    echo 'Running Script w/o temporary scratch'
    {script_folder}/step_{step_number}.py {yaml_copy} {run_number} --no-scratch
    echo 'IceTray finished with Exit Code: ' $?
    if [ "$?" != "0" ] && [ !$KEEP_CRASHED_FILES ] ; then
        echo 'Deleting partially processed file!'
        rm $FINAL_OUT
    fi
else
    echo 'Running Script w/ temporary scratch'
    {script_folder}/step_{step_number}.py {yaml_copy} {run_number} --scratch
    echo 'IceTray finished with Exit Code: ' $?
    if [ "$?" = "0" ] || && [ !$KEEP_CRASHED_FILES ]; then
        cp {scratch_out} $FINAL_OUT
    else
        rm {scratch_out}
    fi
fi

