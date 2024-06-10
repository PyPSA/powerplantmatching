# Copyright 2015-2016 Fabian Hofmann (FIAS), Jonas Hoersch (FIAS)

# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import logging
import os
import shutil
import subprocess as sub
import tempfile

import numpy as np
import pandas as pd

from .core import _package_data

logger = logging.getLogger(__name__)


def add_geoposition_for_duke(df):
    """
    Returns the same pandas.Dataframe with an additional column "Geoposition"
    which concats the latitude and longitude of the powerplant in a string

    """
    if not df.loc[:, ["lat", "lon"]].isnull().all().all():
        return df.assign(
            Geoposition=df[["lat", "lon"]]
            .astype(str)
            .apply(lambda s: ",".join(s), axis=1)
            .replace("nan,nan", np.nan)
        )
    else:
        return df.assign(Geoposition=np.nan)


def duke(
    datasets,
    labels=["one", "two"],
    singlematch=False,
    showmatches=False,
    keepfiles=False,
    showoutput=False,
):
    """
    Run duke in different modes (Deduplication or Record Linkage Mode) to
    either locate duplicates in one database or find the similar entries in two
    different datasets. In RecordLinkagesMode (match two databases) please
    set singlematch=True and use best_matches() afterwards

    Parameters
    ----------

    datasets : pd.DataFrame or [pd.DataFrame]
        A single dataframe is run in deduplication mode, while multiple ones
        are linked
    labels : [str], default ['one', 'two']
        Labels for the linked dataframe
    singlematch: boolean, default False
        Only in Record Linkage Mode. Only report the best match for each entry
        of the first named dataset. This does not guarantee a unique match in
        the second named dataset.
    keepfiles : boolean, default False
        If true, do not delete temporary files
    """

    try:
        sub.run(["java", "-version"], check=True, capture_output=True)
    except sub.CalledProcessError:
        err = "Java is not installed or not in the system's PATH. Please install Java and ensure it is in your system's PATH, then try again."
        logger.error(err)
        raise FileNotFoundError(err)

    dedup = isinstance(datasets, pd.DataFrame)
    if dedup:
        # Deduplication mode
        duke_config = "Deleteduplicates.xml"
        datasets = [datasets]
    else:
        duke_config = "Comparison.xml"

    duke_bin_dir = _package_data("duke_binaries")

    os.environ["CLASSPATH"] = os.pathsep.join(
        [os.path.join(duke_bin_dir, r) for r in os.listdir(duke_bin_dir)]
    )
    tmpdir = tempfile.mkdtemp()

    try:
        shutil.copyfile(
            os.path.join(_package_data(duke_config)), os.path.join(tmpdir, "config.xml")
        )

        logger.debug("Comparing files: %s", ", ".join(labels))

        for n, df in enumerate(datasets):
            df = add_geoposition_for_duke(df)
            #            due to index unity (see https://github.com/larsga/Duke/issues/236)
            if n == 1:
                shift_by = datasets[0].index.max() + 1
                df.index += shift_by
            df.to_csv(os.path.join(tmpdir, f"file{n + 1}.csv"), index_label="id")
            if n == 1:
                df.index -= shift_by

        args = [
            "java",
            "-Dfile.encoding=UTF-8",
            "no.priv.garshol.duke.Duke",
            "--linkfile=linkfile.txt",
        ]
        if singlematch:
            args.append("--singlematch")
        if showmatches:
            args.append("--showmatches")
            stdout = sub.PIPE
        else:
            stdout = None
        args.append("config.xml")

        run = sub.Popen(
            args,
            stderr=sub.PIPE,
            cwd=tmpdir,
            stdout=stdout,
            universal_newlines=True,
        )
        _, stderr = run.communicate()

        if showmatches:
            print(_)

        logger.debug(f"Stderr: {stderr}")
        if any(word in stderr.lower() for word in ["error", "fehler"]):
            raise RuntimeError(f"duke failed: {stderr}")

        if dedup:
            return pd.read_csv(
                os.path.join(tmpdir, "linkfile.txt"),
                encoding="utf-8",
                usecols=[1, 2],
                names=labels,
            )
        else:
            res = pd.read_csv(
                os.path.join(tmpdir, "linkfile.txt"),
                usecols=[1, 2, 3],
                names=labels + ["scores"],
            )
            res.iloc[:, 1] -= shift_by
            res["scores"] = res.scores.astype(float)
            return res

    finally:
        if keepfiles:
            logger.debug(f"Files of the duke run are kept in {tmpdir}")
        else:
            shutil.rmtree(tmpdir)
            logger.debug(f"Files of the duke run have been deleted in {tmpdir}")
