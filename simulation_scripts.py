import os
import stat
import string

import click
import yaml
import getpass

from batch_processing import create_pbs_files, create_dagman_files
from steps.utils import get_run_folder

#from batch_processing import adjust_resources


DATASET_FOLDER = '{data_folder}/{dataset_number}'
STEP_FOLDER = DATASET_FOLDER + '/{step_name}'
PREVIOUS_STEP_FOLDER = DATASET_FOLDER + '/{previous_step_name}'
PROCESSING_FOLDER = DATASET_FOLDER + '/processing/{step_name}'
SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'


class DefaultDict(dict):
    def __init__(self, start_dict, default_value):
        dict.__init__(self, start_dict)
        self.default_value = default_value

    def __missing__(self, key):
        return self.default_value


def fetch_chain(chain_name):
    processing_chains_f = os.path.join(SCRIPT_FOLDER, 'processing_chains.yaml')
    with open(processing_chains_f, 'r') as stream:
        processing_chains = SafeDict(yaml.load(stream))
    try:
        chain_definition = processing_chains[chain_name]
    except KeyError:
        click.echo("Not chain called '' found!".format(chain_name))
    else:
        default_config = chain_definition['default_config']
        if not os.path.isabs(default_config):
            default_config = os.path.join(SCRIPT_FOLDER, default_config)
        job_template = chain_definition['job_template']
        if not os.path.isabs(job_template):
            job_template = os.path.join(SCRIPT_FOLDER, job_template)
        job_templates = chain_definition['job_templates']
        job_template_enum = DefaultDict(job_templates, default=job_template)
        for k, template in job_template_enum.items():
            if not os.path.isabs(template):
                job_template_enum[k] = os.path.join(SCRIPT_FOLDER, template)
        step_enum = chain_definition['steps']
    return step_enum, default_config, job_template_enum


def create_filename(cfg, input=False):
    if input:
        step_name = cfg['step_name']
        step = cfg['step']
        cfg['step_name'] = cfg['previous_step_name']
        cfg['step'] = cfg['previous_step']
        filename = cfg['output_pattern'].format(**cfg)
        full_path = os.path.join(cfg['input_folder'], filename)
        cfg['step_name'] = step_name
        cfg['step'] = step
    else:
        filename = cfg['output_pattern'].format(**cfg)
        full_path = os.path.join(cfg['output_folder'], filename)
    full_path = full_path.replace(' ', '0')
    return full_path


def write_job_files(config, step, check_existing=False,
                    run_start=None, run_stop=None):
    with open(config['job_template']) as f:
        template = f.read()
    output_base = os.path.join(config['processing_folder'], 'jobs')
    if 'name_addition' not in config.keys():
        config['name_addition'] = ''
    if not os.path.isdir(output_base):
        os.makedirs(output_base)
    log_dir = os.path.join(config['processing_folder'], 'logs')
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    scripts = []
    run_numbers = []

    if run_start is None:
        run_start = 0
    else:
        if run_start < 0 or run_start >= config['n_runs']:
            raise ValueError('run_start is out of range: {!r}'.format(
                                                                    run_start))
    if run_stop is None:
        run_stop = config['n_runs']
    else:
        if run_start >= run_stop or run_stop > config['n_runs']:
            raise ValueError('run_stop is out of range: {!r}'.format(run_stop))

    for i in range(run_start, run_stop):
        config['run_number'] = i
        config['run_folder'] = get_run_folder(i)
        final_out = config['outfile_pattern'].format(**config)
        final_out = final_out.replace(' ', '0')
        config['final_out'] = final_out
        if check_existing:
            if os.path.isfile(config['final_out']):
                continue
        output_folder = os.path.dirname(final_out)
        if not os.path.isdir(output_folder):
            os.makedirs(output_folder)
        config['output_folder'] = output_folder
        file_config = string.Formatter().vformat(template, (), config)
        script_name = string.Formatter().vformat(
            config['script_name'], (), config)
        script_path = os.path.join(output_base, script_name)
        with open(script_path, 'w') as f:
            f.write(file_config)
        st = os.stat(script_path)
        os.chmod(script_path, st.st_mode | stat.S_IEXEC)
        scripts.append(script_path)
        run_numbers.append(i)
    return scripts, run_numbers


def build_config(data_folder, custom_settings):
    if data_folder is None:
        default = '/data/user/{}/simulation_scripts/'.format(getpass.getuser())
        data_folder = click.prompt(
            'Please enter the dir were the files should be stored:',
            default=default)
    data_folder = os.path.abspath(data_folder)
    if data_folder.endswith('/'):
        data_folder = data_folder[:-1]
    with open(custom_settings['default_config'], 'r') as stream:
        config = SafeDict(yaml.load(stream))
    config.update(custom_settings)

    config.update({'data_folder': data_folder,
                   'run_number': '{run_number:6d}',
                   'run_folder': '{run_folder}'})

    config['input_folder'] = PREVIOUS_STEP_FOLDER.format(**config)
    config['output_folder'] = STEP_FOLDER.format(**config)
    config['dataset_folder'] = DATASET_FOLDER.format(**config)
    config['processing_folder'] = PROCESSING_FOLDER.format(**config)
    config['script_folder'] = SCRIPT_FOLDER
    if not os.path.isdir(config['output_folder']):
        os.makedirs(config['output_folder'])
    if not os.path.isdir(config['processing_folder']):
        os.makedirs(config['processing_folder'])
    return config


@click.command()
@click.argument('config_file', type=click.Path(exists=True))
@click.option('--data_folder', '-d', default=None,
              help='folder were all files should be placed')
@click.option('--processing_scratch', '-p', default=None,
              help='Folder for the DAGMAN Files')
@click.option('--dagman/--no-dagman', default=False,
              help='Write/Not write files to start dagman process.')
@click.option('--pbs/--no-pbs', default=False,
              help='Write/Not write files to start processing on a pbs system')
@click.option('--step', '-s', default=1,
              help='0=upto clsim\n1 = clsim\n2 =upto L2')
@click.option('--resume/--no-resume', default=False,
              help='Resume processing -> check for existing output')
@click.option('--run_start', default=None, type=int,
              help='Only process runs starting with this number.')
@click.option('--run_stop', default=None, type=int,
              help='Only process runs up to this number.')
def main(data_folder,
         config_file,
         processing_scratch,
         step,
         pbs,
         dagman,
         resume,
         run_start,
         run_stop):
    config_file = click.format_filename(config_file)
    with open(config_file, 'r') as stream:
        custom_settings = SafeDict(yaml.load(stream))
    chain_name = custom_settings['chain_name']
    click.echo('Initialized {} chain!'.format(chain_name))
    step_enum, default_config, job_template_enum = fetch_chain(chain_name)

    # Processing chain can implement different branches. These branches
    # can be defined by using step numbers greater than 9. If the previous
    # step is defined in the processing chain, then the files will be used
    # as input. If it does not exist, files form step % 10 -1 will be used,
    # unless step % 10 is zero.
    if step % 10 == 0:
        # the branch does not have any previous input
        previous_step = (step % 10) - 1

    else:
        # the branch can have previous input
        previous_step = step - 1
        if previous_step not in step_enum:
            previous_step = (step % 10) - 1
    previous_step_name = step_enum.get(previous_step, None)

    custom_settings.update({
        'step': step,
        'step_name': step_enum[step],
        'job_template': job_template_enum[step],
        'previous_step_name': previous_step_name,
        'previous_step': previous_step})

    if 'outfile_pattern' in custom_settings.keys():
        click.echo('Building config for next step based on provided config!')
        config = custom_settings
        config['infile_pattern'] = config['outfile_pattern']
        step = config['step'] + 1
        config.update({
            'step': step,
            'step_name': step_enum[step],
            'previous_step_name': previous_step_name})
        if 'processing_scratch' in config.keys():
            processing_scratch = config['processing_scratch']
    else:
        click.echo('Building config from scratch!')
        custom_settings['default_config'] = default_config
        config = build_config(data_folder, custom_settings)
        config['infile_pattern'] = create_filename(config, input=True)

    config['processing_folder'] = PROCESSING_FOLDER.format(**config)
    config['outfile_pattern'] = create_filename(config)
    config['scratchfile_pattern'] = os.path.basename(config['outfile_pattern'])
    config['script_name'] = '{step_name}{name_addition}_{run_number}.sh'
    if not os.path.isdir(config['processing_folder']):
        os.makedirs(config['processing_folder'])

    outfile = os.path.basename(os.path.join(config_file))
    filled_yaml = os.path.join(config['processing_folder'], outfile)
    config['yaml_copy'] = filled_yaml
    with open(config['yaml_copy'], 'w') as yaml_copy:
        yaml.dump(dict(config), yaml_copy, default_flow_style=False)

    if dagman or pbs:
        if processing_scratch is None:
            default = '/scratch/{}/simulation_scripts'.format(
                getpass.getuser())
            processing_scratch = click.prompt(
                'Please enter a processing scrath:',
                default=default)
        config['processing_scratch'] = os.path.abspath(processing_scratch)

    script_files, run_numbers = write_job_files(config, step,
                                                check_existing=resume,
                                                run_start=run_start,
                                                run_stop=run_stop)

    if dagman or pbs:
        scratch_subfolder = '{dataset_number}_{step_name}'.format(**config)
        scratch_folder = os.path.join(config['processing_scratch'],
                                      scratch_subfolder)
        if not os.path.isdir(scratch_folder):
            os.makedirs(scratch_folder)
        if dagman:
            create_dagman_files(config,
                                script_files,
                                run_numbers,
                                scratch_folder)
        if pbs:
            create_pbs_files(config,
                             script_files,
                             run_numbers,
                             scratch_folder)


if __name__ == '__main__':
    main()
