import os

import click
import yaml
import getpass

username = getpass.getuser()
default_outdir = '/data/user/{}/simulation-scripts/'.format(username)
base_dir = click.prompt(
    'Please enter the dir were the files should be stored',
    default=default_outdir)

step_enum = {-1: None,
              0: '0_after_proposal',
              1: '0_after_clsim',
              2: '2',
              3: '3'}


@click.command()
@click.argument('config_file', click.Path(exists=True))
@click.option('--step',
              '-s',
              default=1,
              help='0 = everything upto proposal\n' + \
                   '1 = clsim\n' + \
                   '2 = everythin up to L2')

def main(config_file, step):
    config_file = click.format_filename(config_file)
    with open(config_file, 'r') as stream:
        config = yaml.load(stream)
    config.update({'step': step_enum[step],
                   'base_dir': base_dir})
    config['e_min'] = float(config['e_min'])
    config['e_max'] = float(config['e_max'])
    config['muongun_e_break'] = float(config['muongun_e_break'])
    config['n_events_per_run'] = int(config['n_events_per_run'])
    config['output_folder'] = config['output_folder'].format(**config)
    config['previous_step'] = step_enum[step - 1]

    outfile = os.path.basename(os.path.join(config_file))
    raw_filename = os.path.splitext(outfile)[0]
    filled_yaml = '{}_{}.yaml'.format(raw_filename, config['step'])
    if not os.path.isdir(config['output_folder']):
        os.makedirs(config['output_folder'])
    filled_yaml = os.path.join(config['output_folder'], filled_yaml)
    with open(filled_yaml, 'w') as yaml_copy:
        yaml.dump(config, yaml_copy, default_flow_style=False)

if __name__ == '__main__':
    main()
