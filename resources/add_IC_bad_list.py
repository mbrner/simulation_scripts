#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT icerec/V05-01-02
import click

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio

@click.command()
@click.argument('gcd_file', click.Path(exists=True))
def main(gcd_file):
    tray = I3Tray()

    class EmptyIceTopBadLists(icetray.I3ConditionalModule):
        def __init__(self, context):
            icetray.I3ConditionalModule.__init__(self, context)

        def Detector(self, frame):
            frame['IceTopBadDOMs'] = dataclasses.I3VectorOMKey()
            frame['IceTopBadTanks'] = dataclasses.I3VectorOMKey()
            self.PushFrame(frame)
    tray.Add('I3Reader',
             FilenameList=[gcd_file])

    tray.AddSegment(EmptyIceTopBadLists, 'Fake Bad IceTop Lists')
    outfile = click.prompt(
            'Please enter the dir were the files should be stored:',
            default=gcd_file)
    tray.AddModule("I3Writer", "EventWriter",
                   filename=outfile)
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()
