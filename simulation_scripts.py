import os
import stat

import click
import yaml
import getpass

from batch_processing import create_pbs_files, create_dagman_files


DATASET_FOLDER = '{data_folder}/{generator}/{dataset_number}'
STEP_FOLDER = DATASET_FOLDER + '/{step}'
PROCESSING_FOLDER = DATASET_FOLDER + '/processing/{step}'
SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))

step_enum = {-1: None,
             0: '0_after_proposal',
             1: '0_after_clsim',
             2: '2',
             3: '3'}


def create_filename(cfg, input=False):
    if input:
        filename = (
            'Level{previous_step}.IC86.YEAR.{generator}.' +
            '{dataset_number:6d}.{run_number}.i3.bz2').format(**cfg)
    else:
        filename = (
            'Level{step}.IC86.YEAR.{generator}.' +
            '{dataset_number:6d}.{run_number}.i3.bz2').format(**cfg)
    full_path = os.path.join(cfg['output_folder'], filename)
    full_path = full_path.replace(' ', '0')
    return full_path


def write_job_files(config, step):
    with open(os.path.join(SCRIPT_FOLDER, 'job_template.sh')) as f:
        template = f.read()

    config.update({'PBS_JOBID': '{PBS_JOBID}',
                   'CLUSTER': '{CLUSTER}'})
    output_base = os.path.join(config['processing_folder'], 'jobs')

    if not os.path.isdir(output_base):
        os.makedirs(output_base)
    log_dir = os.path.join(config['processing_folder'], 'logs')
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    scripts = []
    for i in range(config['n_runs']):
        final_out = config['outfile_pattern'].format(run_number=i)
        final_out = final_out.replace(' ', '0')
        config['final_out'] = final_out
        scratch_out = config['scratchfile_pattern'].format(run_number=i)
        scratch_out = scratch_out.replace(' ', '0')
        config['scratch_out'] = scratch_out
        config['run_number'] = i
        file_config = template.format(**config)
        script_name = config['script_name'].format(**config)
        script_path = os.path.join(output_base, script_name)
        with open(script_path, 'w') as f:
            f.write(file_config)

        st = os.stat(script_path)
        os.chmod(script_path, st.st_mode | stat.S_IEXEC)

        scripts.append(script_path)
    return scripts


def build_config(data_folder, custom_settings, step):
    if data_folder is None:
        default = '/data/user/{}/simulation_scripts/'.format(getpass.getuser())
        data_folder = click.prompt(
            'Please enter the dir were the files should be stored:',
            default=default)
    data_folder = os.path.abspath(data_folder)
    if data_folder.endswith('/'):
        data_folder = data_folder[:-1]

    default_cfg = os.path.join(SCRIPT_FOLDER, 'configs/default.yaml')
    with open(default_cfg, 'r') as stream:
        config = yaml.load(stream)
    config.update(custom_settings)

    config.update({'step_number': step,
                   'step': step_enum[step],
                   'previous_step': step_enum[step - 1],
                   'data_folder': data_folder,
                   'run_number': '{run_number:6d}'})

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
@click.argument('config_file', click.Path(exists=True))
@click.option('--data_folder', '-d', default=None,
              help='folder were all files should be placed')
@click.option('--processing_scratch', '-p', default=None,
              help='Folder for the DAGMAN Files')
@click.option('--dagman/--no-dagman', default=True,
              help='Write/Not write files to start dagman process.')
@click.option('--pbs/--no-pbs', default=True,
              help='Write/Not write files to start processing on a pbs system')
@click.option('--step', '-s', default=1,
              help='0=upto clsim\n1 = clsim\n2 =upto L2')
def main(data_folder, config_file, processing_scratch, step, pbs, dagman):
    config_file = click.format_filename(config_file)
    with open(config_file, 'r') as stream:
        custom_settings = yaml.load(stream)
    if 'outfile_pattern' in custom_settings.keys():
        click.echo('Building config for next step based on provided config!')
        config = custom_settings
        config['infile_pattern'] = config['outfile_pattern']
        step = config['step_number'] + 1
        config.update({'step_number': step,
                       'step': step_enum[step],
                       'previous_step': step_enum[step - 1]})
        processing_scratch = config['processing_scratch']
        config['processing_folder'] = PROCESSING_FOLDER.format(**config)
    else:
        config = build_config(data_folder, custom_settings, step)
        config['infile_pattern'] = create_filename(config, input=True)
    config['outfile_pattern'] = create_filename(config)
    config['scratchfile_pattern'] = os.path.basename(config['outfile_pattern'])
    config['script_name'] = 'step_{step_number}_run_{run_number}.sh'

    outfile = os.path.basename(os.path.join(config_file))
    filled_yaml = os.path.join(config['processing_folder'], outfile)
    config['yaml_copy'] = filled_yaml
    with open(config['yaml_copy'], 'w') as yaml_copy:
        yaml.dump(config, yaml_copy, default_flow_style=False)

    if dagman or pbs:
        if processing_scratch is None:
            default = '/scratch/{}/simulation_scripts'.format(
                getpass.getuser())
            processing_scratch = click.prompt(
                'Please enter a processing scrath:',
                default=default)
        config['processing_scratch'] = os.path.abspath(processing_scratch)

    script_files = write_job_files(config, step)

    if dagman or pbs:
        scratch_subfolder = '{dataset_number}_level{step}'.format(**config)
        scratch_folder = os.path.join(config['processing_scratch'],
                                      scratch_subfolder)
        if not os.path.isdir(scratch_folder):
            os.makedirs(scratch_folder)
        if dagman:
            create_dagman_files(config,
                                script_files,
                                scratch_folder)
        if pbs:
            create_pbs_files(config,
                             script_files,
                             scratch_folder)


if __name__ == '__main__':
    main()
