## Copyright 2015-2016 Fabian Hofmann (FIAS), Jonas Hoersch (FIAS)

## This program is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 3 of the
## License, or (at your option) any later version.

## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.

## You should have received a copy of the GNU General Public License
## along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import, print_function

import os
import subprocess as sub

def add_geoposition_for_duke(df):
    """
    Returns the same pandas.Dataframe with an additional column "Geoposition" which
    concats the lattitude and longitude of the powerplant in a string

    """
    df.loc[df.lat.notnull(), 'Geoposition'] = df[df.lat.notnull()].lat.apply(str)\
           .str.cat(df[df.lat.notnull()].lon.apply(str), sep=',')
    return df


def duke(config, linkfile=None, singlematch=False, showmatches=False, wait=True):
    """
    Run duke in different modes (Deduplication or Record Linkage Mode) to either
    locate duplicates in one database or find the similar entries in two different datasets.
    In RecordLinkagesMode (match two databases) please set singlematch=True and use
    best_matches() afterwards

    Parameters
    ----------

    config : str
        Configruation file (.xml) for the Duke process
    linkfile : str, default None
        txt-file where to record the links
    singlematch: boolean, default False
        Only in Record Linkage Mode. Only report the best match for each entry of the first named
        dataset. This does not guarantee a unique match in the second named dataset.
    wait : boolean, default True
        wait until the process is finished


    """
    os.environ['CLASSPATH'] = ":".join([os.path.join(
                    os.path.dirname(__file__), "duke_binaries", r)
                    for r in os.listdir(os.path.join(
                    os.path.dirname(__file__), "duke_binaries"))])
    args = []
    if linkfile is not None:
        args.append('--linkfile=%s' % linkfile)
    if singlematch:
        args.append('--singlematch')
    if showmatches:
        args.append('--showmatches')
    run = sub.Popen(['java', 'no.priv.garshol.duke.Duke'] + args + [config], stdout=sub.PIPE)
    if showmatches:
        print("\n For displaying matches run: 'for line in _.stdout: print line'")
    if wait:
        run.wait()
    else:
        print("\n The process will continue in the background, type '_.kill()' to abort ")
    return run
