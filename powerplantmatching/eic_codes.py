"""EIC (Energy Identification Code) utilities.

EIC codes are 16-character identifiers assigned by ENTSO-E.
The third character denotes the object type: 'W' = Resource Object
(power plants and generation units).
"""

import re

import pandas as pd

# ENTSO-E EIC code pattern: 16 chars, 3rd char = 'W' (Resource Object)
EIC_PATTERN = re.compile(r"^..W.{13}$")


def is_valid_eic(code: str | None) -> bool:
    """Return True if *code* is a valid EIC code."""
    if code is None:
        return False
    return bool(EIC_PATTERN.match(str(code)))


def extract_eics(series: pd.Series) -> dict[str, list]:
    """Extract valid EIC codes from a pandas Series.

    Handles scalar strings and sets (as stored after aggregation).

    Returns
    -------
    dict
        ``{eic_code: [index, ...]}`` — each valid EIC mapped to the
        list of row indices where it appears.
    """
    result: dict[str, list] = {}
    for idx, val in series.items():
        try:
            if pd.isna(val):
                continue
        except (ValueError, TypeError):
            pass
        if isinstance(val, set):
            for v in val:
                if isinstance(v, str) and EIC_PATTERN.match(v):
                    result.setdefault(v, []).append(idx)
        elif isinstance(val, str) and EIC_PATTERN.match(val):
            result.setdefault(val, []).append(idx)
    return result
