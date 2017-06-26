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
if [ -z ${PBS_JOBID} ] && [ -z ${_CONDOR_SCRATCH_DIR} ]
then
    echo 'Running Script w/o temporary scratch'
    if [ {step} -eq 1 ] ; then
        echo 'Running photon propagation with different oversizings'
        {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --no-scratch --low_oversize
        {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --no-scratch --high_oversize
        {script_folder}/steps/step_1_merge_after_clsim.py {yaml_copy} {run_number} --no-scratch
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
    if [ -z ${_CONDOR_SCRATCH_DIR} ]
    then
        cd /scratch/mboerner
    else
        cd ${_CONDOR_SCRATCH_DIR}
    fi
    if [ {step} -eq 1 ] ; then
        echo 'Running photon propagation with different oversizings'
        {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --scratch --low_oversize
        {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --scratch --high_oversize
        {script_folder}/steps/step_1_merge_after_clsim.py {yaml_copy} {run_number} --scratch
    else
        {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --scratch
    fi
    echo 'IceTray finished with Exit Code: ' $?
    ICETRAY_RC=$?
    if [ "$?" = "0" ] || [ $KEEP_CRASHED_FILES = "1" ]; then
        cp *.i3.bz2 {output_folder}
    fi
    rm *.i3.bz2
fi
exit $ICETRAY_RC

