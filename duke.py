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
import logging

logger = logging.getLogger(__name__)

import os
import subprocess as sub
import shutil
import tempfile
import pandas as pd
import numpy as np

def add_geoposition_for_duke(df):
    """
    Returns the same pandas.Dataframe with an additional column "Geoposition" which
    concats the lattitude and longitude of the powerplant in a string

    """
    if not df.loc[:,['lat','lon']].isnull().all().all():
        df.loc[df.lat.notnull(), 'Geoposition'] = df[df.lat.notnull()].lat.apply(str)\
           .str.cat(df[df.lat.notnull()].lon.apply(str), sep=',')
        return df
    else:
        df.loc[:, 'Geoposition'] = np.NaN
        return df      

def duke(datasets, labels=['one', 'two'], singlematch=False,
         showmatches=False, keepfiles=False):
    """
    Run duke in different modes (Deduplication or Record Linkage Mode) to either
    locate duplicates in one database or find the similar entries in two different datasets.
    In RecordLinkagesMode (match two databases) please set singlematch=True and use
    best_matches() afterwards

    Parameters
    ----------

    datasets : pd.DataFrame or [pd.DataFrame]
        A single dataframe is run in deduplication mode, while multiple ones are linked
    labels : [str], default ['one', 'two']
        Labels for the linked dataframe
    singlematch: boolean, default False
        Only in Record Linkage Mode. Only report the best match for each entry of the first named
        dataset. This does not guarantee a unique match in the second named dataset.
    keepfiles : boolean, default False
        If true, do not delete temporary files
    """

    dedup = isinstance(datasets, pd.DataFrame)
    if dedup:
        # Deduplication mode
        config = "Deleteduplicates.xml"
        datasets = [datasets]
    else:
        config = "Comparison.xml"

    duke_bin_dir = os.path.join(os.path.dirname(__file__), "duke_binaries")
    os.environ['CLASSPATH'] = os.pathsep.join([os.path.join(duke_bin_dir, r)
                                               for r in os.listdir(duke_bin_dir)])

    tmpdir = tempfile.mkdtemp()

    try:
        shutil.copyfile(os.path.join(os.path.dirname(__file__), "data", config),
                        os.path.join(tmpdir, "config.xml"))

        for n, df in enumerate(datasets):
            df = add_geoposition_for_duke(df)
            df.to_csv(os.path.join(tmpdir, "file{}.csv".format(n+1)), index_label='id', encoding='utf-8')

        args = ['java', 'no.priv.garshol.duke.Duke', '--linkfile=linkfile.txt']
        if singlematch:
            args.append('--singlematch')
        if showmatches:
            args.append('--showmatches')
        args.append('config.xml')

        run = sub.Popen(args, stderr=sub.PIPE, cwd=tmpdir)
        _, stderr = run.communicate()

        logger.debug("Stderr: {}".format(stderr))
        if 'ERROR' in stderr:
            raise RuntimeError("duke failed: {}".format(stderr))

        if dedup:
            return pd.read_csv(os.path.join(tmpdir, 'linkfile.txt'),
                                  usecols=[1, 2], names=labels)
        else:
            return pd.read_csv(os.path.join(tmpdir, 'linkfile.txt'),
                                  usecols=[1, 2, 3], names=labels + ['scores'])
    finally:
        if keepfiles:
            logger.debug("Files of the duke run are kept in {}", tmpdir)
        else:
            shutil.rmtree(tmpdir)
