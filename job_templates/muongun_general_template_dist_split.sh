#!/bin/bash
#PBS -l nodes=1:ppn={cpus}
#PBS -l pmem={memory}
#PBS -l mem={memory}
#PBS -l vmem={memory}
#PBS -l pvmem={memory}
#PBS -l walltime={walltime}
#PBS -o {processing_folder}/logs/{step_name}_run_{run_number}_${PBS_JOBID}.out
#PBS -e {processing_folder}/logs/{step_name}_run_{run_number}_${PBS_JOBID}.err
#PBS -q long
#PBS -S /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
FINAL_OUT={final_out}
KEEP_CRASHED_FILES={keep_crashed_files:d}
echo $FINAL_OUT
if [ -z ${PBS_JOBID} ] && [ -z ${CLUSTER} ]
then
    echo 'Running Script w/o temporary scratch'
    if [ {step_number} -eq 1 ] ; then
        echo 'Running photon propagation with different oversizings'
        {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --no-scratch --oversize
        {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --no-scratch --no-oversize
        {script_folder}steps/step_1_merge_after_clsim.py {yaml_copy} {run_number}
    else
        {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --no-scratch
    fi
    echo 'IceTray finished with Exit Code: ' $?
    ICETRAY_RC=$?
    if [ "$?" != "0" ] && [ $KEEP_CRASHED_FILES != "0" ] ; then
        echo 'Deleting partially processed file!'
        rm $FINAL_OUT
    fi
else
    echo 'Running Script w/ temporary scratch'
    if [ {step_number} -eq 1 ] ; then
        echo 'Running photon propagation with different oversizings'
        {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --no-scratch --oversize
        {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --no-scratch --no-oversize
        {script_folder}steps/step_1_merge_after_clsim.py {yaml_copy} {run_number}
    else
        {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --no-scratch
    fi
    echo 'IceTray finished with Exit Code: ' $?
    ICETRAY_RC=$?
    if [ "$?" = "0" ] || [ $KEEP_CRASHED_FILES = "1" ]; then
        cp {scratch_out} $FINAL_OUT
    else
        rm {scratch_out}
    fi
fi
exit $ICETRAY_RC

