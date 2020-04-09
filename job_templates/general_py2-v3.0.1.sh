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
#PBS -S /cvmfs/icecube.opensciencegrid.org/py2-v3.0.1/icetray-start

echo 'Starting job on Host: '$HOSTNAME

FINAL_OUT={final_out}
KEEP_CRASHED_FILES={keep_crashed_files}

# -----------------
# Clean environment
# -----------------
PATH=/usr/lib64/qt-3.3/bin:/opt/pgi/linux86-64/13.3/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/opt/puppetlabs/bin:/opt/dell/srvadmin/bin
MANPATH=:/opt/pgi/linux86-64/13.3/man
unset PYTHONPATH
unset GLOBUS_LOCATION
unset PERL5LIB
unset X509_CERT_DIR
unset LD_LIBRARY_PATH
unset SROOTBASE
unset I3_TESTDATA
unset I3_DATA
unset ROOTSYS
unset SROOT
unset PKG_CONFIG_PATH
# -----------------

# ToDo: find a clean way to define which python version to load.
# Possibly define job template for each step and each job template is only
# to be used for that python version
if [ {step} -eq 0 ] || [ {step} -eq 5 ] ; then
    echo 'Loading py2-v3.0.1'
    eval `/cvmfs/icecube.opensciencegrid.org/py2-v3.0.1/setup.sh`
    export PYTHONUSERBASE=/data/user/mhuennefeld/DNN_reco/virtualenvs/tensorflow_cpu_py2-v3.0.1/
    echo 'Using PYTHONUSERBASE: '${PYTHONUSERBASE}
else
    if [ {step} -le 12 ] ; then
        echo 'Loading py2-v2'
        eval `/cvmfs/icecube.opensciencegrid.org/py2-v2/setup.sh`
        export PYTHONUSERBASE=/home/mboerner/software/python_libs
        echo 'Using PYTHONUSERBASE: '${PYTHONUSERBASE}
    else
        echo 'Loading py2-v1'
        eval `/cvmfs/icecube.opensciencegrid.org/py2-v1/setup.sh`
        export PYTHONUSERBASE=/home/mboerner/software/python_libs_py2_v1
        echo 'Using PYTHONUSERBASE: '${PYTHONUSERBASE}
    fi
fi

export PATH=$PYTHONUSERBASE/bin:$PATH
export PYTHONPATH=$PYTHONUSERBASE/lib/python2.7/site-packages:$PYTHONPATH


echo $FINAL_OUT
if [ -z ${PBS_JOBID} ] && [ -z ${_CONDOR_SCRATCH_DIR} ]
then
    echo 'Running Script w/o temporary scratch'
    {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --no-scratch
    ICETRAY_RC=$?
    echo 'IceTray finished with Exit Code: ' $ICETRAY_RC
    if [ $ICETRAY_RC -ne 0 ] && [ $KEEP_CRASHED_FILES -eq 0 ] ; then
        echo 'Deleting partially processed file! ' $FINAL_OUT
        rm $FINAL_OUT
    fi
else
    echo 'Running Script w/ temporary scratch'
    if [ -z ${_CONDOR_SCRATCH_DIR} ]
    then
        cd /scratch/${USER}
    else
        cd ${_CONDOR_SCRATCH_DIR}
    fi
    {script_folder}/steps/{step_name}.py {yaml_copy} {run_number} --scratch
    ICETRAY_RC=$?
    echo 'IceTray finished with Exit Code: ' $ICETRAY_RC
    if [ $ICETRAY_RC -eq 0 ] || [ $KEEP_CRASHED_FILES -eq 1 ]; then
        cp *.i3.bz2 {output_folder}
    fi
    rm *.i3.bz2
fi
exit $ICETRAY_RC

