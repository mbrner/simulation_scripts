#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT icerec/V05-01-06
import click

import step_3_pass2_get_pulses


@click.command()
@click.argument('cfg', click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
@click.argument('do_merging_if_necessary', type=bool, default=True)
def main(cfg, run_number, scratch, do_merging_if_necessary):

    step_3_pass2_get_pulses.main(cfg=cfg,
                                 run_number=run_number,
                                 scratch=scratch,
                                 do_merging_if_necessary=False,
                                 )


if __name__ == '__main__':
    main()
