import os
import stat
import subprocess

import click
import yaml


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'


def write_onejob_file(config,
                      scratch_folder):
    process_name = '{dataset_number}_{step_name}'.format(**config)
    resources_cfg = config['resources']

    lines = []
    lines.append('processname = $(run).{}'.format(process_name))
    lines.append('executable = $(script_file)')
    lines.append('getenv         = false')
    log_dir = os.path.join(scratch_folder, 'logs')
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    lines.append('should_transfer_files = YES')
    lines.append('when_to_transfer_output = ON_EXIT')
    lines.append('output = {}/$(processname).out'.format(log_dir))
    lines.append('error = {}/$(processname).err'.format(log_dir))
    lines.append('log = {}/$(processname).log'.format(log_dir))
    lines.append('notification   = never')
    lines.append('universe       = vanilla')
    if resources_cfg['gpus'] is not None:
        if config['step'] in resources_cfg['gpus'].keys():
            lines.append('request_gpus = {}'.format(
                resources_cfg['gpus'][config['step']]))
    if resources_cfg['memory'] is not None:
        if config['step'] in resources_cfg['memory'].keys():
            lines.append('request_memory = {}'.format(
                resources_cfg['memory'][config['step']]))
    if resources_cfg['cpus'] is not None:
        if config['step'] in resources_cfg['cpus'].keys():
            lines.append('request_cpus = {}'.format(
                resources_cfg['cpus'][config['step']]))
    lines.append('queue')
    onejob_file = os.path.join(scratch_folder, 'OneJob.submit')
    with open(onejob_file, 'w') as open_file:
        for line in lines:
            open_file.write(line + '\n')
    return onejob_file


def write_config_file(config,
                      scratch_folder):
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
    if 'dagman_submit_delay' in config.keys():
        lines.append('DAGMAN_SUBMIT_DELAY={}'.format(
            config['dagman_submit_delay']))
    config_file = os.path.join(scratch_folder, 'dagman.config')
    with open(config_file, 'w') as open_file:
        for line in lines:
            open_file.write(line + '\n')
    return config_file


def write_option_file(config,
                      script_files,
                      run_numbers,
                      job_file,
                      scratch_folder):
    process_name = '{dataset_number}_{step_name}'.format(**config)

    lines = []
    for i, script_i in zip(run_numbers, script_files):
        job_name = '{}_{}'.format(process_name, i)
        lines.append('JOB {} {}'.format(job_name, job_file))
        lines.append('VARS {} script_file="{}" run="{}"'.format(
            job_name, script_i, i))

    option_file = os.path.join(scratch_folder, 'dagman.options')
    with open(option_file, 'w') as open_file:
        for line in lines:
            open_file.write(line + '\n')
    return option_file


def create_dagman_files(config,
                        script_files,
                        run_numbers,
                        scratch_folder):
    config_file = write_config_file(config, scratch_folder)
    onejob_file = write_onejob_file(config, scratch_folder)
    options_file = write_option_file(config,
                                     script_files,
                                     run_numbers,
                                     onejob_file,
                                     scratch_folder)
    cmd = 'condor_submit_dag -config {} -notification Complete {}'.format(
        config_file, options_file)
    run_script = os.path.join(scratch_folder, 'start_dagman.sh')
    with open(run_script, 'w') as open_file:
        open_file.write(cmd)
    st = os.stat(run_script)
    os.chmod(run_script, st.st_mode | stat.S_IEXEC)


def adjust_resouces(config, script_files, scratch_folder):
    resources_cfg = config['resources']
    if resources_cfg['gpu_steps'] is not None:
        if config['step_number'] not in config['gpu_steps'].keys():
            resources_cfg['gpu_steps'][config['step_number']] = 0
    if resources_cfg['memory'] is not None:
        if config['step_number'] not in config['memory'].keys():
            resources_cfg['memory'][config['step_number']] = '1gb'
    if resources_cfg['cpu_steps'] is not None:
        if config['step_number'] in config['cpu_steps'].keys():
            resources_cfg['cpu_steps'][config['step_number']] = 1
    if resources_cfg['walltime'] is not None:
        if config['step_number'] in config['walltime'].keys():
            resources_cfg['walltime'][config['step_number']] = 1

    config.update({
        'memory': resources_cfg['memory'][config['step_number']],
        'cpus': resources_cfg['memory'][config['step_number']],
        'gpus': resources_cfg['memory'][config['step_number']],
        'walltime': resources_cfg['walltime'][config['step_number']]})
    return config

def create_pbs_files(config,
                     script_files,
                     run_numbers,
                     scratch_folder):
    pass


@click.command()
@click.argument('config_file', type=click.Path(exists=True))
@click.option('-j', '--n_jobs', default=1,
              help='Number of parallel jobs')
def process_local(config_file, n_jobs):
    config_file = click.format_filename(config_file)
    with open(config_file, 'r') as stream:
        config = SafeDict(yaml.load(stream))
    click.echo('Processing {} with max. {} parralel jobs!'.format(
        config_file, n_jobs))
    output_base = os.path.join(config['processing_folder'], 'jobs')
    log_dir = os.path.join(config['processing_folder'], 'logs')
    job_files = []
    for i in range(config['n_runs']):
        script_name = config['script_name'].format(**config)
        script_name = script_name.format(run_number=i)
        script_path = os.path.join(output_base, script_name)
        script_path = script_path.replace(' ', '')
        if os.path.isfile(script_path):
            job_files.append(script_path)
        else:
            click.echo('{} not found!'.format(script_path))
    click.echo('Starting processing!')

    processes = {}
    finished = 0
    with click.progressbar(length=len(job_files)) as bar:
        for job in job_files:
            job_name = os.path.splitext(job)[0]
            stderr = os.path.join(log_dir, '{}.err'.format(job_name))
            stdout = os.path.join(log_dir, '{}.err'.format(job_name))
            sub_process = subprocess.Popen([job, '>>', stdout, '2>>', stderr],)
            processes[sub_process.pid] = [sub_process, job]
            if len(processes) >= n_jobs:
                pid, exit_code = os.wait()
                if exit_code != 0:
                    job_file = processes[pid][1]
                    click.echo('{} finished with exit code {}'.format(job_file,
                                                                      pid))
                del processes[pid]
                finished += 1
                bar.update(finished)
        while len(processes) > 0 and finshed < len(job_files):
            pid, exit_code = os.wait()
            if exit_code != 0:
                job_file = processes[pid][1]
                click.echo('{} finished with exit code {}'.format(job_file,
                                                                  pid))
            del processes[pid]
            finished += 1
            bar.update(finished)
        click.echo('Finished!')
