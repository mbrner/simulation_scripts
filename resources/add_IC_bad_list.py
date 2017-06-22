import os

import click

from I3Tray import I3Tray
from icecube import icetray, dataclasses, dataio

@click.command()
@click.argument('gcd_file', click.Path(exists=True))
def main(gcd_file):
    gcd_file = str(click.format_filename(gcd_file))
    tray = I3Tray()

    class EmptyIceTopBadLists(icetray.I3ConditionalModule):
        def __init__(self, context):
            icetray.I3ConditionalModule.__init__(self, context)

        def Configure(self):
            pass

        def DetectorStatus(self, frame):
            frame['IceTopBadDOMs'] = dataclasses.I3VectorOMKey()
            frame['IceTopBadTanks'] = dataclasses.I3VectorTankKey()
            self.PushFrame(frame)
    print(gcd_file)
    tray.AddModule('I3Reader',
            'reader',
             FilenameList=[str(gcd_file)])

    tray.AddModule(EmptyIceTopBadLists, 'Fake Bad IceTop Lists')
    default = os.path.basename(gcd_file)
    default = default.replace('i3.gz', 'IT_added.i3.gz')
    outfile = click.prompt(
            'Please enter the dir were the files should be stored:',
            default=default)
    tray.AddModule("I3Writer", "EventWriter",
                   filename=str(outfile))
    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()


if __name__ == '__main__':
    main()
