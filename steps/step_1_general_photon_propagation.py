#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT simulation/V05-01-01
import os
import sys

import multiprocessing
import traceback

import click
import yaml

import numpy as np

from icecube.simprod import segments

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio, phys_services
from utils import create_random_services, get_run_folder
from dom_distance_cut import generate_stream_object


MAX_PARALLEL_EVENTS = 50
SPLINE_TABLES = '/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines'


def process_single_stream(cfg, infile, outfile):
    click.echo('Input: {}'.format(infile))
    hybrid_mode = (cfg['clsim_hybrid_mode'] and
                   cfg['icemodel'].lower() != 'spicelea')
    ignore_muon_light = (cfg['clsim_ignore_muon_light'] and
                         cfg['clsim_hybrid_mode'])
    click.echo('UseGPUs: {}'.format(cfg['clsim_usegpus']))
    click.echo('IceModel: {}'.format(cfg['icemodel']))
    if not cfg['icemodel_location'] is None:
        click.echo('IceModelLocation: {}'.format(cfg['icemodel_location']))
    click.echo('DomOversize {}'.format(cfg['clsim_dom_oversize']))
    click.echo('UnshadowedFraction: {0:.2f}'.format(
        cfg['clsim_unshadowed_fraction']))
    click.echo('HybridMode: {}'.format(hybrid_mode))
    click.echo('IgnoreMuonLight: {}'.format(ignore_muon_light))
    click.echo('KeepMCPE: {}'.format(cfg['clsim_keep_mcpe']))
    click.echo('Output: {}'.format(outfile))

    tray = I3Tray()
    tray.context['I3FileStager'] = dataio.get_stagers()
    random_services, _ = create_random_services(
        dataset_number=cfg['dataset_number'],
        run_number=cfg['run_number'],
        seed=cfg['seed'],
        n_services=process_single_stream.n_streams)

    random_service = random_services[process_single_stream.i_th_stream]
    tray.context['I3RandomService'] = random_service
    tray.Add('I3Reader', FilenameList=[cfg['gcd'], infile])

    if hybrid_mode:
        cascade_tables = segments.LoadCascadeTables(IceModel=cfg['icemodel'],
                                                    TablePath=SPLINE_TABLES)
    else:
        cascade_tables = None

    if cfg['clsim_usegpus']:
        use_gpus = True
        use_cpus = False
    else:
        use_gpus = True
        use_cpus = False

    if 'additional_clsim_params' in cfg:
        additional_clsim_params = cfg['additional_clsim_params']
    else:
        additional_clsim_params = {}

    tray.AddSegment(
        segments.PropagatePhotons,
        "PropagatePhotons",
        RandomService=random_service,
        MaxParallelEvents=MAX_PARALLEL_EVENTS,
        KeepIndividualMaps=cfg['clsim_keep_mcpe'],
        IceModel=cfg['icemodel'],
        IceModelLocation=cfg['icemodel_location'],
        UnshadowedFraction=cfg['clsim_unshadowed_fraction'],
        IgnoreMuons=ignore_muon_light,
        HybridMode=hybrid_mode,
        UseGPUs=use_gpus,
        UseAllCPUCores=use_cpus,
        DOMOversizeFactor=cfg['clsim_dom_oversize'],
        CascadeService=cascade_tables,
        **additional_clsim_params)

    outfile = outfile.replace(' ', '0')
    tray.AddModule("I3Writer", "writer",
                   Filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.Stream('S'),
                            icetray.I3Frame.Stream('M')])
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()


process_single_stream.n_streams = 1
process_single_stream.i_th_stream = 0


def filter_S_frame(frame):
    if not filter_S_frame.already_added:
        filter_S_frame.already_added = True
        return True
    else:
        return False


filter_S_frame.already_added = False


def merge(infiles, outfile):
    tray = I3Tray()
    tray.context['I3FileStager'] = dataio.get_stagers()
    tray.Add('I3Reader', FilenameList=infiles)
    tray.AddModule(filter_S_frame,
                   'S Frame Filter',
                   Streams=[icetray.I3Frame.Stream('S')])
    tray.AddModule("I3Writer", "writer",
                   Filename=outfile,
                   Streams=[icetray.I3Frame.DAQ,
                            icetray.I3Frame.Physics,
                            icetray.I3Frame.Stream('S'),
                            icetray.I3Frame.Stream('M')])
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()
    for file_i in infiles:
        click.echo('Remvoing {}:'.format(file_i))
        os.remove(file_i)


class ExecProcess(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._cconn.send((e, tb))
            # raise e  # You can still rise this exception if you need to

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


@click.command()
@click.argument('cfg', type=click.Path(exists=True))
@click.argument('run_number', type=int)
@click.option('--scratch/--no-scratch', default=True)
def main(cfg, run_number, scratch):
    with open(cfg, 'r') as stream:
        if int(yaml.__version__[0]) < 5:
            # backwards compatibility for yaml versions before version 5
            cfg = yaml.load(stream)
        else:
            cfg = yaml.full_load(stream)
    cfg['run_number'] = run_number
    cfg['run_folder'] = get_run_folder(run_number)

    infile = cfg['infile_pattern'].format(**cfg)
    infile = infile.replace(' ', '0')

    if scratch:
        outfile = cfg['scratchfile_pattern'].format(**cfg)
    else:
        outfile = cfg['outfile_pattern'].format(**cfg)
    outfile = outfile.replace(' ', '0')

    if cfg.get('distance_splits', False):

        distance_splits = np.atleast_1d(cfg['distance_splits'])
        dom_limits = np.atleast_1d(cfg['threshold_doms'])
        if len(dom_limits) == 1:
            dom_limits = np.ones_like(distance_splits) * cfg['threshold_doms']
        oversize_factors = np.atleast_1d(cfg['oversize_factors'])
        order = np.argsort(distance_splits)
        stream_objects = generate_stream_object(distance_splits[order],
                                                dom_limits[order],
                                                oversize_factors[order])
        process_single_stream.n_streams = len(stream_objects)
        for stream_i in stream_objects:
            infile_i = stream_i.transform_filepath(infile)
            outfile_i = stream_i.transform_filepath(outfile)
            cfg['clsim_dom_oversize'] = stream_i.oversize_factor
            proc = ExecProcess(target=process_single_stream,
                               args=(cfg, infile_i, outfile_i))
            proc.start()
            proc.join()
            process_single_stream.i_th_stream += 1
        if proc.exception:
            error, traceback = proc.exception
            print(traceback)
            print(error)
            sys.exit(1)
        infiles = [stream_i.transform_filepath(outfile)
                   for stream_i in stream_objects]
        merge(infiles, outfile)
    else:
        process_single_stream(cfg, infile, outfile)


if __name__ == '__main__':
    main()
