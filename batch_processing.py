import os
import stat

def write_onejob_file(config):
    process_name = '{dataset_number}_level{step}'.format(**config)

    lines = []
    lines.append('processname = {}'.format(process_name))
    lines.append('executable = $(script_file)')
    lines.append('getenv         = true')
    log_dir = os.path.join(config['dagman_scratch'], 'logs')
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    lines.append('should_transfer_files = YES')
    lines.append('when_to_transfer_output = ON_EXIT')
    lines.append('output = {}/$(processname).$(Cluster).out'.format(log_dir))
    lines.append('error = {}/$(processname).$(Cluster).err'.format(log_dir))
    lines.append('log = {}/$(processname).log'.format(log_dir))
    lines.append('notification   = never')
    lines.append('universe       = vanilla')
    if 'memory' in config.keys():
        lines.append('request_memory = {}'.format(config['memory']))
    lines.append('queue')
    onejob_file = os.path.join(config['dagman_scratch'], 'OneJob.submit')
    with open(onejob_file, 'w') as open_file:
        for line in lines:
            open_file.write(line + '\n')
    return onejob_file

def write_config_file(config):
    lines = []
    if 'dagman_max_jobs' in config.keys():
        lines.append('DAGMAN_MAX_JOBS_SUBMITTED={}'.format(
            config['dagman_max_jobs']))
    else:
        lines.append('DAGMAN_MAX_JOBS_SUBMITTED=1000')
    if 'dagman_scan_interval' in config.keys():
        lines.append('DAGMAN_USER_LOG_SCAN_INTERVAL={}'.format(
            config['dagman_scan_interval']))
    if 'dagman_submits_interval' in config.keys():
        lines.append('DAGMAN_MAX_SUBMIT_PER_INTERVAL={}'.format(
            config['dagman_submits_interval']))
    config_file = os.path.join(config['dagman_scratch'], 'dagman.config')
    with open(config_file, 'w') as open_file:
        for line in lines:
            open_file.write(line + '\n')
    return config_file

def write_option_file(config,
                      script_files,
                      job_file):
    process_name = '{dataset_number}_level{step}'.format(**config)

    lines = []
    for i, script_i in enumerate(script_files):
        job_name = '{}_{}'.format(process_name, i)
        lines.append('JOB {} {}'.format(job_name, job_file))
        lines.append('VARS {} script_file="{}"'.format(job_name, script_i))

    option_file = os.path.join(config['dagman_scratch'], 'dagman.options')
    with open(option_file, 'w') as open_file:
        for line in lines:
            open_file.write(line + '\n')
    return option_file

def create_dagman_files(config,
                        script_files):
    config_file = write_config_file(config)
    onejob_file = write_onejob_file(config)
    options_file = write_option_file(config, script_files, onejob_file)
    cmd = 'condor_submit_dag -config {} -notification Complete {}'.format(
        config_file, options_file)
    run_script = os.path.join(config['dagman_scratch'], 'start_dagman.sh')
    with open(run_script, 'w') as open_file:
        open_file.write(cmd)
    st = os.stat(run_script)
    os.chmod(run_script, st.st_mode | stat.S_IEXEC)


def create_pbs_files(config,
                            script_files):
    pass
