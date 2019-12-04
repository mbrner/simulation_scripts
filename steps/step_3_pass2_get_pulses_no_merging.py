#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT icerec/V05-01-06
import click

import step_3_pass2_get_pulses


@click.command()
@click.argument('cfg', type=click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
@click.pass_context
def main(ctx, cfg, run_number, scratch):

    # call main function with modified click context
    ctx.forward(step_3_pass2_get_pulses.main, do_merging_if_necessary=False)


if __name__ == '__main__':
    main()
